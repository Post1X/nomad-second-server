import crypto from 'crypto';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import CategorySuggestions from '../schemas/CategorySuggestionsSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { SETTINGS_KEYS } from '../helpers/constants';
import { buildCategoryKeywords } from '../helpers/buildCategoryKeywords';
import {
  categorizeEventsWithAi,
  proposeCategoriesFromEvents,
  rebuildAiPromptIfNeeded,
} from './AiCategoryServices';
import {
  normalizeCategoryKey,
  isInvalidSuggestionName,
} from './CategorySuggestionServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CAT_SUGGEST_BACKFILL');

/** @type {null | object} */
let job = null;
const MAX_LOGS = 500;

const pushLog = (msg) => {
  if (!job) return;
  job.logs.push({ t: Date.now(), msg: String(msg) });
  if (job.logs.length > MAX_LOGS) job.logs = job.logs.slice(-MAX_LOGS);
  logger.info(msg);
};

export const getCategorySuggestionsBackfillJob = () => {
  if (!job) {
    return {
      running: false,
      cancelRequested: false,
      startedAt: null,
      finishedAt: null,
      logs: [],
      result: null,
      error: null,
    };
  }
  return {
    running: job.running,
    cancelRequested: Boolean(job.cancelRequested),
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    logs: job.logs,
    result: job.result,
    error: job.error,
  };
};

export const stopCategorySuggestionsBackfill = () => {
  if (!job?.running) {
    const err = new Error('Backfill is not running');
    err.status = 409;
    throw err;
  }
  job.cancelRequested = true;
  pushLog('Stop requested');
  return getCategorySuggestionsBackfillJob();
};

export async function loadUncategorizedEvents(limit) {
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  const otherId = other?._id ? String(other._id) : null;

  const filter = {
    $or: [
      { 'event_data.events_category_id': null },
      { 'event_data.events_category_id': { $exists: false } },
      { 'event_data.events_category_id': '' },
      { 'event_data.category_resolved_by': 'default_other' },
      ...(otherId ? [{ 'event_data.events_category_id': otherId }] : []),
    ],
  };

  let q = ParsedEventsSchema.find(filter).sort({ updatedAt: -1 });
  if (limit) q = q.limit(limit);
  const docs = await q.lean();

  return docs.map((d) => ({
    parsedId: String(d._id),
    tempId: crypto.randomUUID(),
    name: d.event_data?.name || '',
    description: d.event_data?.description || '',
    address: d.event_data?.address || '',
    specialization: d.event_data?.specialization || '',
    source: d.source || d.event_data?.source,
  })).filter((e) => e.name);
}

export async function writePendingFromDiscovery(proposed, { dryRun = false, replace = true } = {}) {
  const existing = await EventsCategoriesSchema.find({}).select('name').lean();
  const existingKeys = new Set(
    existing.map((c) => normalizeCategoryKey(c.name)).filter(Boolean),
  );

  const rows = [];
  for (const cat of proposed || []) {
    const name = String(cat.name || '').trim();
    const key = normalizeCategoryKey(name);
    if (!key || isInvalidSuggestionName(name) || existingKeys.has(key)) continue;
    const examples = (cat.examples || []).slice(0, 12);
    if (!examples.length) continue;
    const keywords = buildCategoryKeywords(name, examples, cat.keywords || []);
    rows.push({
      raw_name: name,
      normalized_key: key,
      status: 'pending',
      hit_count: Number(cat.hit_count) || examples.length,
      tokens_total: 0,
      example_events: examples,
      keywords,
      sources: Array.isArray(cat.sources) ? cat.sources : [],
      first_seen_at: new Date(),
      last_seen_at: new Date(),
      reject_reason: '',
    });
  }

  if (dryRun) return { written: 0, wouldWrite: rows.length, rows };

  if (replace) await CategorySuggestions.deleteMany({ status: 'pending' });
  if (rows.length) await CategorySuggestions.insertMany(rows);
  return { written: rows.length, wouldWrite: rows.length, rows };
}

/**
 * B+A pipeline:
 * 1) Remap uncategorized → EXISTING categories (cards in prompt)
 * 2) Discovery propose ≤20 new candidates from still-open sample (replace pending)
 */
export async function runCategorySuggestionsBackfill(options = {}) {
  const limit = options.limit != null && options.limit !== ''
    ? Math.max(1, Math.min(5000, Number(options.limit) || 0))
    : null;
  const dryRun = Boolean(options.dryRun);
  const applyCategory = Boolean(options.applyCategory) && !dryRun;
  const chunk = Math.max(10, Math.min(80, Number(options.chunk) || 40));
  const proposeSample = Math.max(40, Math.min(160, Number(options.proposeSample) || 120));
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));
  const skipDiscovery = Boolean(options.skipDiscovery);

  if (job?.running) {
    const err = new Error('Backfill already running');
    err.status = 409;
    throw err;
  }

  job = {
    running: true,
    cancelRequested: false,
    startedAt: new Date(),
    finishedAt: null,
    logs: [],
    result: null,
    error: null,
  };

  setImmediate(async () => {
    let stopped = false;
    try {
      pushLog(
        `Start B+A: existing-only → discovery≤${maxCategories}`
        + ` limit=${limit ?? 'all'} apply=${applyCategory} dryRun=${dryRun}`,
      );

      await SettingsSchema.deleteOne({ key: SETTINGS_KEYS.categoriesHash });
      const { categories } = await rebuildAiPromptIfNeeded();
      pushLog(`Categories+cards: ${(categories || []).map((c) => c.name).join(', ')}`);

      const events = await loadUncategorizedEvents(limit);
      pushLog(`Uncategorized events: ${events.length}`);
      if (!events.length) {
        job.result = { processed: 0, assignedExisting: 0, stillNull: 0, proposed: 0 };
        pushLog('Nothing to process');
        return;
      }

      const nameById = new Map((categories || []).map((c) => [String(c._id), c.name]));
      const usageTotal = {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        batches: 0,
        failedBatches: 0,
      };
      let assignedExisting = 0;
      let stillNull = 0;
      let processed = 0;
      const unresolved = [];

      for (let i = 0; i < events.length; i += chunk) {
        if (job.cancelRequested) {
          stopped = true;
          pushLog(`Stopped after ${processed}/${events.length}`);
          break;
        }

        const batch = events.slice(i, i + chunk);
        const part = `${Math.floor(i / chunk) + 1}/${Math.ceil(events.length / chunk)}`;
        pushLog(`Phase1 chunk ${part} (existing + cards)…`);

        // eslint-disable-next-line no-await-in-loop
        const { map, usage } = await categorizeEventsWithAi(batch);
        usageTotal.prompt_tokens += usage.prompt_tokens || 0;
        usageTotal.completion_tokens += usage.completion_tokens || 0;
        usageTotal.total_tokens += usage.total_tokens || 0;
        usageTotal.batches += usage.batches || 0;
        usageTotal.failedBatches += usage.failedBatches || 0;

        for (const ev of batch) {
          const catId = map.get(ev.tempId) || null;
          if (catId) {
            assignedExisting += 1;
            if (applyCategory) {
              // eslint-disable-next-line no-await-in-loop
              await ParsedEventsSchema.updateOne(
                { _id: ev.parsedId },
                {
                  $set: {
                    'event_data.events_category_id': String(catId),
                    'event_data.category_resolved_by': 'ai',
                  },
                  $unset: {
                    'event_data.category_ai_failed': 1,
                    'event_data.category_suggested_name': 1,
                  },
                },
              );
            }
          } else {
            stillNull += 1;
            unresolved.push(ev);
          }
        }

        processed += batch.length;
        for (const ev of batch.slice(0, 3)) {
          const catId = map.get(ev.tempId);
          pushLog(
            `  · ${ev.name.slice(0, 70)} → `
            + `${catId ? (nameById.get(String(catId)) || catId) : 'null'}`,
          );
        }
      }

      let proposed = 0;
      if (!stopped && !skipDiscovery && unresolved.length && !job.cancelRequested) {
        const sample = unresolved.slice(0, proposeSample);
        pushLog(`Phase2 discovery on ${sample.length}/${unresolved.length} open events…`);
        // eslint-disable-next-line no-await-in-loop
        const result = await proposeCategoriesFromEvents(sample, {
          maxCategories,
          categories,
          chunkSize: 40,
          onChunk: ({ part, total, proposed: n, ms }) => {
            pushLog(`  discovery chunk ${part}/${total} ${Math.round(ms / 1000)}s (+${n})`);
          },
        });
        usageTotal.prompt_tokens += result.usage?.prompt_tokens || 0;
        usageTotal.completion_tokens += result.usage?.completion_tokens || 0;
        usageTotal.total_tokens += result.usage?.total_tokens || 0;

        // eslint-disable-next-line no-await-in-loop
        const write = await writePendingFromDiscovery(result.categories, {
          dryRun,
          replace: true,
        });
        proposed = write.written || write.wouldWrite || 0;
        for (const row of (write.rows || []).slice(0, 20)) {
          pushLog(
            `  candidate "${row.raw_name}" hits=${row.hit_count}`
            + ` · ${(row.example_events || []).slice(0, 2).join(' | ')}`,
          );
        }
        pushLog(dryRun ? `Dry-run would write ${proposed}` : `Pending replaced → ${proposed}`);
      } else if (!unresolved.length) {
        pushLog('All mapped to existing — no discovery needed');
        if (!dryRun) await CategorySuggestions.deleteMany({ status: 'pending' });
      }

      const pending = dryRun
        ? null
        : await CategorySuggestions.countDocuments({ status: 'pending' });

      job.result = {
        processed,
        assignedExisting,
        stillNull,
        proposed,
        usage: usageTotal,
        pendingSuggestions: pending,
        dryRun,
        stopped,
      };
      pushLog(
        `${stopped ? 'Stopped' : 'Done'}. existing=${assignedExisting}`
        + ` open=${stillNull} proposed=${proposed}`
        + `${pending != null ? ` pending=${pending}` : ''}`,
      );
    } catch (e) {
      job.error = e?.message || String(e);
      pushLog(`ERROR: ${job.error}`);
      logger.error(job.error, e);
    } finally {
      job.running = false;
      job.finishedAt = new Date();
    }
  });

  return getCategorySuggestionsBackfillJob();
}

export default {
  getCategorySuggestionsBackfillJob,
  runCategorySuggestionsBackfill,
  stopCategorySuggestionsBackfill,
  loadUncategorizedEvents,
  writePendingFromDiscovery,
};
