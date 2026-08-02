import CategorySuggestions from '../schemas/CategorySuggestionsSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { buildCategoryKeywords } from '../helpers/buildCategoryKeywords';
import { consolidateCategorySuggestionsWithAi } from './AiCategoryServices';
import {
  normalizeCategoryKey,
  isInvalidSuggestionName,
} from './CategorySuggestionServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CAT_SUGGEST_CONSOLIDATE');

/** @type {null | {
 *  running: boolean,
 *  startedAt: Date|null,
 *  finishedAt: Date|null,
 *  logs: { t: number, msg: string }[],
 *  result: object|null,
 *  error: string|null,
 * }} */
let job = null;

const MAX_LOGS = 300;

const pushLog = (msg) => {
  if (!job) return;
  job.logs.push({ t: Date.now(), msg: String(msg) });
  if (job.logs.length > MAX_LOGS) {
    job.logs = job.logs.slice(-MAX_LOGS);
  }
  logger.info(msg);
};

export const getCategorySuggestionsConsolidateJob = () => {
  if (!job) {
    return {
      running: false,
      startedAt: null,
      finishedAt: null,
      logs: [],
      result: null,
      error: null,
    };
  }
  return {
    running: job.running,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    logs: job.logs,
    result: job.result,
    error: job.error,
  };
};

/**
 * Replace all pending suggestions with ≤ maxCategories consolidated rows.
 * @param {{ maxCategories?: number }} [options]
 */
export async function runCategorySuggestionsConsolidate(options = {}) {
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));

  if (job?.running) {
    const err = new Error('Consolidate already running');
    err.status = 409;
    throw err;
  }

  job = {
    running: true,
    startedAt: new Date(),
    finishedAt: null,
    logs: [],
    result: null,
    error: null,
  };

  setImmediate(async () => {
    try {
      pushLog(`Start consolidate maxCategories=${maxCategories}`);

      const pending = await CategorySuggestions.find({ status: 'pending' })
        .sort({ hit_count: -1 })
        .lean();
      pushLog(`Pending raw candidates: ${pending.length}`);

      if (!pending.length) {
        job.result = {
          before: 0, after: 0, dropped: 0, usage: null, categories: [],
        };
        pushLog('Nothing to consolidate');
        return;
      }

      const existing = await EventsCategoriesSchema.find({}).select('name').lean();
      const existingNames = existing.map((c) => c.name).filter(Boolean);
      const existingKeys = new Set(
        existingNames.map((n) => normalizeCategoryKey(n)).filter(Boolean),
      );

      const byRawKey = new Map(
        pending.map((p) => [normalizeCategoryKey(p.raw_name), p]),
      );

      pushLog('Calling OpenAI consolidate…');
      const { categories: aiCats, usage } = await consolidateCategorySuggestionsWithAi(
        pending,
        { maxCategories, existingNames },
      );
      pushLog(`AI returned ${aiCats.length} categories, tokens=${usage?.total_tokens || 0}`);

      const kept = [];
      let dropped = 0;

      for (const row of aiCats) {
        const name = String(row.name || '').trim();
        const key = normalizeCategoryKey(name);
        if (!key || isInvalidSuggestionName(name) || existingKeys.has(key)) {
          dropped += 1;
          pushLog(`  drop "${name}" (invalid or exists)`);
          continue;
        }

        let hit_count = 0;
        let tokens_total = 0;
        const examples = [];
        const sourcesSet = new Set();
        const sourceNames = row.sources?.length ? row.sources : [name];

        for (const src of sourceNames) {
          const srcKey = normalizeCategoryKey(src);
          const doc = byRawKey.get(srcKey);
          if (doc) {
            hit_count += doc.hit_count || 1;
            tokens_total += doc.tokens_total || 0;
            for (const ex of doc.example_events || []) {
              if (examples.length < 12 && !examples.includes(ex)) examples.push(ex);
            }
            for (const s of doc.sources || []) sourcesSet.add(s);
          } else {
            // unmatched source label — still count as 1 hit for visibility
            hit_count += 1;
          }
        }
        if (!hit_count) hit_count = 1;

        const keywords = buildCategoryKeywords(name, examples, row.keywords || []);
        kept.push({
          raw_name: name,
          normalized_key: key,
          status: 'pending',
          hit_count,
          tokens_total,
          example_events: examples,
          keywords,
          sources: [...sourcesSet],
          first_seen_at: new Date(),
          last_seen_at: new Date(),
          reject_reason: '',
        });
        pushLog(
          `  keep "${name}" hits=${hit_count} kw=${keywords.length}`
          + ` ← ${(row.sources || []).slice(0, 5).join(', ')}`,
        );
      }

      // Deduplicate by normalized_key (AI sometimes repeats)
      const deduped = [];
      const seen = new Set();
      for (const row of kept) {
        if (seen.has(row.normalized_key)) {
          dropped += 1;
          continue;
        }
        seen.add(row.normalized_key);
        deduped.push(row);
      }

      await CategorySuggestions.deleteMany({ status: 'pending' });
      if (deduped.length) {
        await CategorySuggestions.insertMany(deduped);
      }

      job.result = {
        before: pending.length,
        after: deduped.length,
        dropped,
        usage,
        categories: deduped.map((d) => ({
          name: d.raw_name,
          hit_count: d.hit_count,
          keywords: d.keywords,
        })),
      };
      pushLog(`Done. ${pending.length} → ${deduped.length} (dropped=${dropped})`);
    } catch (e) {
      job.error = e?.message || String(e);
      pushLog(`ERROR: ${job.error}`);
      logger.error(job.error, e);
    } finally {
      job.running = false;
      job.finishedAt = new Date();
    }
  });

  return getCategorySuggestionsConsolidateJob();
}

export default {
  getCategorySuggestionsConsolidateJob,
  runCategorySuggestionsConsolidate,
};
