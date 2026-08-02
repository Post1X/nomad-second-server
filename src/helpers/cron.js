import cron from 'node-cron';
import {
  EVENT_SOURCE,
  OPERATION_TYPES,
  SETTINGS_KEYS,
  TICKETMASTER_PARSE_INTERVAL_DAYS,
} from './constants';
import startParseRun from './startParseRun';
import CleanupServices from '../services/CleanupServices';
import DictSyncServices from '../services/DictSyncServices';
import SettingsSchema from '../schemas/SettingsSchema';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('CRON');

/** @type {Array<{ id: string, expr: string, label: string, kind: 'parse'|'dict'|'cleanup', source?: string, type?: string, meta?: object }>} */
export const CRON_JOBS = [
  {
    id: 'kontramarka',
    expr: '0 2 * * 1',
    label: 'Kontramarka',
    kind: 'parse',
    source: EVENT_SOURCE.kontramarka,
    type: OPERATION_TYPES.parsingEventsFromKontramarka,
    meta: { specialization: 'Event' },
  },
  {
    id: 'eventim',
    expr: '0 2 * * 3',
    label: 'Eventim',
    kind: 'parse',
    source: EVENT_SOURCE.eventim,
    type: OPERATION_TYPES.parsingEventsFromEventim,
    meta: {},
  },
  {
    id: 'fienta',
    expr: '0 2 * * 5',
    label: 'Fienta',
    kind: 'parse',
    source: EVENT_SOURCE.fienta,
    type: OPERATION_TYPES.parsingEventsFromFienta,
    meta: { specialization: 'Event' },
  },
  {
    id: 'ticketmaster',
    expr: '0 2 * * 0',
    label: 'Ticketmaster (every 3 weeks)',
    kind: 'parse',
    source: EVENT_SOURCE.ticketmaster,
    type: OPERATION_TYPES.parsingEventsFromTicketmaster,
    meta: { specialization: 'Event' },
  },
  {
    id: 'israelinfo',
    expr: '0 4 * * 0',
    label: 'Israelinfo',
    kind: 'parse',
    source: EVENT_SOURCE.israelinfo,
    type: OPERATION_TYPES.parsingEventsFromIsraelinfo,
    meta: { specialization: 'Event' },
  },
  {
    id: 'dictSync',
    expr: '0 1 * * 0',
    label: 'Dictionary sync from main',
    kind: 'dict',
  },
  {
    id: 'cleanup',
    expr: '15 3 * * *',
    label: 'Cleanup expired ParsedEvents',
    kind: 'cleanup',
  },
];

const jobById = Object.fromEntries(CRON_JOBS.map((j) => [j.id, j]));

const defaultJobFlags = () => Object.fromEntries(CRON_JOBS.map((j) => [j.id, true]));

export async function getCronJobFlags() {
  const row = await SettingsSchema.findOne({ key: SETTINGS_KEYS.parsingCronJobs }).lean();
  const flags = { ...defaultJobFlags(), ...(row?.value && typeof row.value === 'object' ? row.value : {}) };
  for (const j of CRON_JOBS) {
    if (typeof flags[j.id] !== 'boolean') flags[j.id] = true;
  }
  return flags;
}

export async function setCronJobEnabled(jobId, enabled) {
  if (!jobById[jobId]) {
    const err = new Error(`Unknown cron job: ${jobId}`);
    err.status = 404;
    throw err;
  }
  const flags = await getCronJobFlags();
  flags[jobId] = Boolean(enabled);
  await SettingsSchema.findOneAndUpdate(
    { key: SETTINGS_KEYS.parsingCronJobs },
    { $set: { value: flags } },
    { upsert: true },
  );
  return flags;
}

/** @deprecated global master — true if any parse/dict job enabled */
export async function isParsingCronEnabled() {
  const flags = await getCronJobFlags();
  return CRON_JOBS.some((j) => j.kind !== 'cleanup' && flags[j.id] !== false);
}

export async function setParsingCronEnabled(enabled) {
  const flags = Object.fromEntries(CRON_JOBS.map((j) => [j.id, Boolean(enabled)]));
  // cleanup stays on when enabling all; when disabling all parsers, leave cleanup alone
  if (!enabled) {
    const prev = await getCronJobFlags();
    flags.cleanup = prev.cleanup !== false;
    for (const j of CRON_JOBS) {
      if (j.kind !== 'cleanup') flags[j.id] = false;
    }
  }
  await SettingsSchema.findOneAndUpdate(
    { key: SETTINGS_KEYS.parsingCronJobs },
    { $set: { value: flags } },
    { upsert: true },
  );
  return isParsingCronEnabled();
}

export async function getCronStatus() {
  const flags = await getCronJobFlags();
  return {
    enabled: await isParsingCronEnabled(),
    timezone: 'UTC',
    jobs: CRON_JOBS.map((j) => ({
      id: j.id,
      expr: j.expr,
      label: j.label,
      kind: j.kind,
      source: j.source || null,
      enabled: flags[j.id] !== false,
    })),
  };
}

const shouldRunTicketmaster = async () => {
  const row = await SettingsSchema.findOne({ key: SETTINGS_KEYS.lastTicketmasterParseAt }).lean();
  if (!row?.value) return true;
  const last = new Date(row.value);
  if (Number.isNaN(last.getTime())) return true;
  const diffDays = (Date.now() - last.getTime()) / (1000 * 60 * 60 * 24);
  return diffDays >= TICKETMASTER_PARSE_INTERVAL_DAYS;
};

const markTicketmasterParsed = async () => {
  await SettingsSchema.findOneAndUpdate(
    { key: SETTINGS_KEYS.lastTicketmasterParseAt },
    { $set: { value: new Date().toISOString() } },
    { upsert: true },
  );
};

/**
 * Run a cron job now (manual or scheduled).
 * @param {string} jobId
 * @param {{ force?: boolean, ignoreEnabled?: boolean }} [options]
 *   force — skip Ticketmaster 21d interval
 *   ignoreEnabled — run even if job toggle is OFF (manual UI)
 */
export async function runCronJob(jobId, options = {}) {
  const job = jobById[jobId];
  if (!job) {
    const err = new Error(`Unknown cron job: ${jobId}`);
    err.status = 404;
    throw err;
  }

  const flags = await getCronJobFlags();
  if (!options.ignoreEnabled && flags[jobId] === false) {
    logger.info(`Skip ${job.label}: job disabled`);
    return { skipped: true, reason: 'disabled', jobId };
  }

  if (job.kind === 'parse') {
    if (jobId === 'ticketmaster' && !options.force) {
      if (!(await shouldRunTicketmaster())) {
        logger.info('Skip Ticketmaster parse: interval < 21 days');
        return { skipped: true, reason: 'interval', jobId };
      }
    }
    logger.info(`Starting ${job.label} parsing...`);
    const runId = await startParseRun(job.type, job.meta || {});
    if (jobId === 'ticketmaster') await markTicketmasterParsed();
    logger.info(`${job.label} parse run created: ${runId}`);
    return { skipped: false, jobId, runId: String(runId) };
  }

  if (job.kind === 'dict') {
    logger.info('Starting dictionary sync from main...');
    const stats = await DictSyncServices.pullFromMainServer();
    logger.info(`Dictionary sync done: ${JSON.stringify(stats)}`);
    return { skipped: false, jobId, stats };
  }

  if (job.kind === 'cleanup') {
    logger.info('Starting expired events cleanup...');
    const result = await CleanupServices.cleanupExpiredEventsByDays();
    logger.info(`Cleanup done: ${JSON.stringify(result)}`);
    return { skipped: false, jobId, result };
  }

  const err = new Error(`Unsupported cron kind: ${job.kind}`);
  err.status = 500;
  throw err;
}

const setupCron = () => {
  for (const job of CRON_JOBS) {
    cron.schedule(job.expr, () => {
      runCronJob(job.id).catch((error) => {
        logger.error(`${job.label} schedule error: ${error.message || error}`);
      });
    }, { timezone: 'UTC' });
  }

  logger.info('Cron jobs for parsing are set up');
  for (const j of CRON_JOBS) {
    logger.info(`- ${j.expr} UTC: ${j.label} (${j.id})`);
  }
  logger.info(`Sources: ${Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad).join(', ')}`);
};

export default setupCron;
