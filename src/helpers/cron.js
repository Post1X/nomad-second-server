import cron from 'node-cron';
import OperationsSchema from '../schemas/OperationsSchema';
import { OPERATION_STATUSES, OPERATION_TYPES } from './constants';
import parseFienta from '../operations/parseFienta';
import parseEventim from '../operations/parseEventim';
import parseKontramarka from '../operations/parseKontramarka';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('CRON');

const setupCron = () => {
  cron.schedule('0 2 * * 1', async () => {
    logger.info('Starting Kontramarka parsing...');
    try {
      const operation = new OperationsSchema({
        type: OPERATION_TYPES.parsingEventsFromKontramarka,
        status: OPERATION_STATUSES.pending,
        statistics: '',
        errorText: '',
        infoText: 'Operation created by cron, starting parsing...',
        is_processed: false,
        is_taken: false,
      });
      await operation.save();

      await OperationsSchema.findByIdAndUpdate(operation._id, {
        status: OPERATION_STATUSES.processing,
        infoText: 'Parsing started...',
      });

      await parseKontramarka({
        meta: {
          specialization: 'Event',
        },
        operationId: operation._id,
      });
    } catch (error) {
      logger.error(`Error in Kontramarka parsing: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  cron.schedule('0 2 * * 3', async () => {
    logger.info('Starting Fienta parsing...');
    try {
      const operation = new OperationsSchema({
        type: OPERATION_TYPES.parsingEventsFromFienta,
        status: OPERATION_STATUSES.pending,
        statistics: '',
        errorText: '',
        infoText: 'Operation created by cron, starting parsing...',
        is_processed: false,
        is_taken: false,
      });
      await operation.save();

      await OperationsSchema.findByIdAndUpdate(operation._id, {
        status: OPERATION_STATUSES.processing,
        infoText: 'Parsing started...',
      });

      await parseFienta({
        meta: {
          specialization: 'Event',
        },
        operationId: operation._id,
      });
    } catch (error) {
      logger.error(`Error in Fienta parsing: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  cron.schedule('0 2 * * 5', async () => {
    logger.info('Starting Eventim parsing...');
    try {
      const operation = new OperationsSchema({
        type: OPERATION_TYPES.parsingEventsFromEventim,
        status: OPERATION_STATUSES.pending,
        statistics: '',
        errorText: '',
        infoText: 'Operation created by cron, starting parsing...',
        is_processed: false,
        is_taken: false,
      });
      await operation.save();

      await OperationsSchema.findByIdAndUpdate(operation._id, {
        status: OPERATION_STATUSES.processing,
        infoText: 'Parsing started...',
      });

      await parseEventim({
        meta: {},
        operationId: operation._id,
      });
    } catch (error) {
      logger.error(`Error in Eventim parsing: ${error.message || error}`);
    }
  }, { timezone: 'UTC' });

  logger.info('Cron jobs for parsing are set up');
  logger.info('- Monday 02:00 UTC: Kontramarka');
  logger.info('- Wednesday 02:00 UTC: Fienta');
  logger.info('- Friday 02:00 UTC: Eventim');
};

export default setupCron;

