import crypto from 'crypto';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import CategorySuggestions from '../schemas/CategorySuggestionsSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { SETTINGS_KEYS } from '../helpers/constants';
import {
  categorizeEventsWithAi,
  rebuildAiPromptIfNeeded,
} from './AiCategoryServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CAT_SUGGEST_BACKFILL');

/** @type {null | {
 *  running: boolean,
 *  cancelRequested: boolean,
 *  startedAt: Date|null,
 *  finishedAt: Date|null,
 *  logs: { t: number, msg: string }[],
 *  result: object|null,
 *  error: string|null,
 * }} */
let job = null;

const MAX_LOGS = 500;

const pushLog = (msg) => {
  if (!job) return;
  job.logs.push({ t: Date.now(), msg: String(msg) });
  if (job.logs.length > MAX_LOGS) {
    job.logs = job.logs.slice(-MAX_LOGS);
  }
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
  pushLog('Stop requested — finishing current chunk, then halt');
  return getCategorySuggestionsBackfillJob();
};

async function loadUncategorized(limit) {
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
    source: d.source,
  })).filter((e) => e.name);
}

/**
 * @param {{ limit?: number|null, applyCategory?: boolean, chunk?: number, dryRun?: boolean }} options
 */
export async function runCategorySuggestionsBackfill(options = {}) {
  const limit = options.limit != null && options.limit !== ''
    ? Math.max(1, Math.min(5000, Number(options.limit) || 0))
    : null;
  const dryRun = Boolean(options.dryRun);
  const applyCategory = Boolean(options.applyCategory) && !dryRun;
  const chunk = Math.max(10, Math.min(80, Number(options.chunk) || 40));

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

  // fire-and-forget
  setImmediate(async () => {
    let stopped = false;
    try {
      pushLog(
        `Start backfill limit=${limit ?? 'all'} applyCategory=${applyCategory}`
        + ` dryRun=${dryRun} chunk=${chunk}`,
      );

      await SettingsSchema.deleteOne({ key: SETTINGS_KEYS.categoriesHash });
      const { categories } = await rebuildAiPromptIfNeeded();
      pushLog(`Categories: ${(categories || []).map((c) => c.name).join(', ')}`);

      const events = await loadUncategorized(limit);
      pushLog(`Uncategorized events: ${events.length}`);

      if (!events.length) {
        job.result = {
          processed: 0,
          assignedExisting: 0,
          suggestedNew: 0,
          stillNull: 0,
          usage: null,
        };
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
      let suggestedNew = 0;
      let stillNull = 0;
      let processed = 0;
      const tokensBySuggestion = new Map();

      for (let i = 0; i < events.length; i += chunk) {
        if (job.cancelRequested) {
          stopped = true;
          pushLog(`Stopped by user after ${processed}/${events.length} events`);
          break;
        }

        const batch = events.slice(i, i + chunk);
        const part = `${Math.floor(i / chunk) + 1}/${Math.ceil(events.length / chunk)}`;
        pushLog(`Chunk ${part} (n=${batch.length})…`);

        // eslint-disable-next-line no-await-in-loop
        const {
          map, suggestions, usage, suggestionUpsert, tokensBySuggestion: shares,
        } = await categorizeEventsWithAi(batch, { persistSuggestions: !dryRun });

        usageTotal.prompt_tokens += usage.prompt_tokens || 0;
        usageTotal.completion_tokens += usage.completion_tokens || 0;
        usageTotal.total_tokens += usage.total_tokens || 0;
        usageTotal.batches += usage.batches || 0;
        usageTotal.failedBatches += usage.failedBatches || 0;

        for (const row of shares || []) {
          const prev = tokensBySuggestion.get(row.name) || { name: row.name, events: 0, tokens: 0 };
          prev.events += row.events;
          prev.tokens += row.tokens;
          tokensBySuggestion.set(row.name, prev);
        }

        pushLog(
          `Chunk ${part} usage total_tokens=${usage.total_tokens || 0}`
          + ` upsert=${JSON.stringify(suggestionUpsert)}`,
        );

        for (const ev of batch) {
          const catId = map.get(ev.tempId) || null;
          const sug = suggestions.get(ev.tempId) || null;
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
          } else if (sug) {
            suggestedNew += 1;
            if (applyCategory) {
              // eslint-disable-next-line no-await-in-loop
              await ParsedEventsSchema.updateOne(
                { _id: ev.parsedId },
                {
                  $set: {
                    'event_data.category_suggested_name': sug,
                    'event_data.category_resolved_by': 'default_other',
                  },
                },
              );
            }
          } else {
            stillNull += 1;
          }
        }

        processed += batch.length;

        // sample lines
        for (const ev of batch.slice(0, 3)) {
          const catId = map.get(ev.tempId);
          const sug = suggestions.get(ev.tempId);
          pushLog(
            `  · ${ev.name.slice(0, 70)} → `
            + `${catId ? (nameById.get(String(catId)) || catId) : (sug ? `suggest:${sug}` : 'null')}`,
          );
        }
      }

      const sharesSorted = [...tokensBySuggestion.values()]
        .sort((a, b) => b.tokens - a.tokens)
        .map((r) => ({ ...r, tokens: Math.round(r.tokens) }));

      const pending = dryRun
        ? null
        : await CategorySuggestions.countDocuments({ status: 'pending' });

      job.result = {
        processed,
        assignedExisting,
        suggestedNew,
        stillNull,
        usage: usageTotal,
        tokensBySuggestion: sharesSorted,
        pendingSuggestions: pending,
        dryRun,
        stopped,
      };

      pushLog(
        `${stopped ? 'Stopped' : 'Done'}. processed=${processed}/${events.length}`
        + ` existing=${assignedExisting} newSuggestions=${suggestedNew} null=${stillNull}`
        + `${pending != null ? ` pending=${pending}` : ''}`,
      );
      pushLog(`Tokens total=${usageTotal.total_tokens}`);
      for (const row of sharesSorted.slice(0, 15)) {
        pushLog(`  candidate "${row.name}": events=${row.events} tokens≈${row.tokens}`);
      }
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

export async function waitForCategorySuggestionsBackfill(pollMs = 400) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const snap = getCategorySuggestionsBackfillJob();
    if (!snap.running) return snap;
    // eslint-disable-next-line no-await-in-loop
    await new Promise((r) => setTimeout(r, pollMs));
  }
}

export default {
  getCategorySuggestionsBackfillJob,
  runCategorySuggestionsBackfill,
  stopCategorySuggestionsBackfill,
  waitForCategorySuggestionsBackfill,
};
