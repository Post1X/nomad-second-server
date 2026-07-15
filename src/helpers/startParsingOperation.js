import OperationsSchema from '../schemas/OperationsSchema';
import { OPERATION_STATUSES, OPERATION_TYPES } from './constants';
import parseFienta from '../operations/parseFienta';
import parseEventim from '../operations/parseEventim';
import parseKontramarka from '../operations/parseKontramarka';
import parseTicketmaster from '../operations/parseTicketmaster';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('START_PARSING');

const PARSERS = {
  [OPERATION_TYPES.parsingEventsFromFienta]: parseFienta,
  [OPERATION_TYPES.parsingEventsFromEventim]: parseEventim,
  [OPERATION_TYPES.parsingEventsFromKontramarka]: parseKontramarka,
  [OPERATION_TYPES.parsingEventsFromTicketmaster]: parseTicketmaster,
};

export async function startParsingOperation(type, meta = {}) {
  if (!Object.values(OPERATION_TYPES).includes(type)) {
    throw new Error(`Invalid operation type: ${type}`);
  }

  const parseFunction = PARSERS[type];
  if (!parseFunction) {
    throw new Error(`No parser registered for type: ${type}`);
  }

  const operation = new OperationsSchema({
    type,
    status: OPERATION_STATUSES.pending,
    statistics: '',
    errorText: '',
    infoText: 'Operation created, starting parsing...',
    is_processed: false,
    is_taken: false,
  });
  await operation.save();

  setImmediate(async () => {
    try {
      await OperationsSchema.findByIdAndUpdate(operation._id, {
        status: OPERATION_STATUSES.processing,
        infoText: 'Parsing started...',
      });

      await parseFunction({ meta, operationId: operation._id });
    } catch (error) {
      logger.error(`Error in parsing operation ${operation._id}: ${error.message || error}`);
      await OperationsSchema.findByIdAndUpdate(operation._id, {
        status: OPERATION_STATUSES.error,
        errorText: error.message || 'Unknown error occurred',
        finish_time: new Date(),
      });
    }
  });

  return operation._id;
}

export default startParsingOperation;
