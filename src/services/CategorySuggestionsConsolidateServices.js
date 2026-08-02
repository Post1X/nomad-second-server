import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import CategorySuggestions from '../schemas/CategorySuggestionsSchema';
import { proposeCategoriesFromEvents } from './AiCategoryServices';
import {
  loadUncategorizedEvents,
  writePendingFromDiscovery,
} from './CategorySuggestionsBackfillServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CAT_SUGGEST_DISCOVERY');

/** @type {null | object} */
let job = null;
const MAX_LOGS = 300;

const pushLog = (msg) => {
  if (!job) return;
  job.logs.push({ t: Date.now(), msg: String(msg) });
  if (job.logs.length > MAX_LOGS) job.logs = job.logs.slice(-MAX_LOGS);
  logger.info(msg);
};

export const getCategorySuggestionsConsolidateJob = () => {
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

export const stopCategorySuggestionsConsolidate = () => {
  if (!job?.running) {
    const err = new Error('Discovery is not running');
    err.status = 409;
    throw err;
  }
  job.cancelRequested = true;
  pushLog('Stop requested');
  return getCategorySuggestionsConsolidateJob();
};

/**
 * Standalone discovery (option B): sample of Другое → ≤N candidates (replace pending).
 * Same engine as phase2 of backfill; does not remap existing categories.
 */
export async function runCategorySuggestionsConsolidate(options = {}) {
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));
  const sampleSize = Math.max(40, Math.min(180, Number(options.sampleSize) || 150));
  const limit = options.limit != null
    ? Math.max(1, Math.min(5000, Number(options.limit) || 0))
    : sampleSize * 3;

  if (job?.running) {
    const err = new Error('Discovery already running');
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
    try {
      pushLog(`Discovery start max=${maxCategories} sample=${sampleSize}`);

      const categories = await EventsCategoriesSchema.find({}).sort({ sort: 1 }).lean();
      pushLog(`Existing: ${categories.map((c) => c.name).join(', ')}`);

      const pool = await loadUncategorizedEvents(limit);
      pushLog(`Uncategorized pool: ${pool.length}`);
      if (!pool.length) {
        await CategorySuggestions.deleteMany({ status: 'pending' });
        job.result = { before: 0, after: 0, proposed: 0 };
        pushLog('Nothing to discover');
        return;
      }

      if (job.cancelRequested) {
        job.result = { stopped: true };
        pushLog('Stopped before AI');
        return;
      }

      const sample = pool.slice(0, sampleSize);
      pushLog(`Calling proposeCategoriesFromEvents(n=${sample.length})…`);
      const { categories: proposed, usage } = await proposeCategoriesFromEvents(sample, {
        maxCategories,
        categories,
      });

      if (job.cancelRequested) {
        job.result = { stopped: true, usage };
        pushLog('Stopped after AI — DB unchanged');
        return;
      }

      const before = await CategorySuggestions.countDocuments({ status: 'pending' });
      const write = await writePendingFromDiscovery(proposed, { replace: true });
      pushLog(`Pending ${before} → ${write.written}`);
      for (const row of (write.rows || []).slice(0, 20)) {
        pushLog(
          `  "${row.raw_name}" hits=${row.hit_count}`
          + ` · ${(row.example_events || []).slice(0, 2).join(' | ')}`,
        );
      }

      job.result = {
        before,
        after: write.written,
        proposed: write.written,
        usage,
        categories: (write.rows || []).map((r) => ({
          name: r.raw_name,
          hit_count: r.hit_count,
          examples: r.example_events,
        })),
      };
      pushLog(`Done. tokens=${usage?.total_tokens || 0}`);
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
  stopCategorySuggestionsConsolidate,
};
