import cron from 'node-cron';
import { OPERATION_TYPES } from './constants';
import startParsingOperation from './startParsingOperation';
import CleanupServices from '../services/CleanupServices';
import DictSyncServices from '../services/DictSyncServices';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('CRON');

const runScheduledParsing = async (type, meta, label) => {
  logger.info(`Starting ${label} parsing...`);
  try {
    const operationId = await startParsingOperation(type, meta);
    logger.info(`${label} parsing operation created: ${operationId}`);
  } catch (error) {
    logger.error(`Error starting ${label} parsing: ${error.message || error}`);
  }
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

  cron.schedule('0 2 * * 0', () => {
    runScheduledParsing(
      OPERATION_TYPES.parsingEventsFromTicketmaster,
      { specialization: 'Event' },
      'Ticketmaster',
    );
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

  cron.schedule('0 5 * * 1', async () => {
    logger.info('Starting weekly cleanup of taken events...');
    try {
      const result = await CleanupServices.cleanupTakenEvents();
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
  logger.info('- Sunday 02:00 UTC: Ticketmaster');
  logger.info('- Sunday 04:00 UTC: Israelinfo');
  logger.info('- Monday 05:00 UTC: Cleanup taken events (>3 weeks)');
};

export default setupCron;
