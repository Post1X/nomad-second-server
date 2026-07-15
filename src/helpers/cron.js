import cron from 'node-cron';
import { OPERATION_TYPES } from './constants';
import startParsingOperation from './startParsingOperation';
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

  logger.info('Cron jobs for parsing are set up');
  logger.info('- Monday 02:00 UTC: Kontramarka');
  logger.info('- Wednesday 02:00 UTC: Eventim');
  logger.info('- Friday 02:00 UTC: Fienta');
  logger.info('- Sunday 02:00 UTC: Ticketmaster (all countries)');
};

export default setupCron;
