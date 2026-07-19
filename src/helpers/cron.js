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

const runScheduledParsing = async (type, meta, label) => {
  logger.info(`Starting ${label} parsing...`);
  try {
    const runId = await startParseRun(type, meta);
    logger.info(`${label} parse run created: ${runId}`);
  } catch (error) {
    logger.error(`Error starting ${label} parsing: ${error.message || error}`);
  }
};

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

const setupCron = () => {
  cron.schedule('0 2 * * 1', () => {
    runScheduledParsing(
      OPERATION_TYPES.parsingEventsFromKontramarka,
      { specialization: 'Event' },
      'Kontramarka',
    );
  }, { timezone: 'UTC' });

  cron.schedule('0 2 * * 3', () => {
    runScheduledParsing(
      OPERATION_TYPES.parsingEventsFromEventim,
      {},
      'Eventim',
    );
  }, { timezone: 'UTC' });

  cron.schedule('0 2 * * 5', () => {
    runScheduledParsing(
      OPERATION_TYPES.parsingEventsFromFienta,
      { specialization: 'Event' },
      'Fienta',
    );
  }, { timezone: 'UTC' });

  cron.schedule('0 2 * * 0', async () => {
    try {
      if (!(await shouldRunTicketmaster())) {
        logger.info('Skip Ticketmaster parse: interval < 21 days');
        return;
      }
      await runScheduledParsing(
        OPERATION_TYPES.parsingEventsFromTicketmaster,
        { specialization: 'Event' },
        'Ticketmaster',
      );
      await markTicketmasterParsed();
    } catch (error) {
      logger.error(`Ticketmaster schedule error: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  cron.schedule('0 4 * * 0', () => {
    runScheduledParsing(
      OPERATION_TYPES.parsingEventsFromIsraelinfo,
      { specialization: 'Event' },
      'Israelinfo',
    );
  }, { timezone: 'UTC' });

  cron.schedule('0 1 * * 0', async () => {
    logger.info('Starting weekly dictionary sync from main...');
    try {
      const stats = await DictSyncServices.pullFromMainServer();
      logger.info(`Dictionary sync done: ${JSON.stringify(stats)}`);
    } catch (error) {
      logger.error(`Dictionary sync failed: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  cron.schedule('0 5 1 */6 *', async () => {
    logger.info('Starting expired events cleanup (>6 months)...');
    try {
      const result = await CleanupServices.cleanupExpiredEvents();
      logger.info(`Cleanup done: ${JSON.stringify(result)}`);
    } catch (error) {
      logger.error(`Cleanup failed: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  logger.info('Cron jobs for parsing are set up');
  logger.info('- Sun 01:00 UTC: Pull dictionaries from main');
  logger.info('- Monday 02:00 UTC: Kontramarka');
  logger.info('- Wednesday 02:00 UTC: Eventim');
  logger.info('- Friday 02:00 UTC: Fienta');
  logger.info('- Sunday 02:00 UTC: Ticketmaster (every 3 weeks)');
  logger.info('- Sunday 04:00 UTC: Israelinfo');
  logger.info('- 1st of every 6th month 05:00 UTC: Cleanup expired events (>6 months)');
  logger.info(`Sources: ${Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad).join(', ')}`);
};

export default setupCron;
