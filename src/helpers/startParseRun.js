import ParseRunsSchema from '../schemas/ParseRunsSchema';
import {
  EVENT_SOURCE,
  OPERATION_STATUSES,
  OPERATION_TYPES,
  SOURCE_BY_OPERATION_TYPE,
} from './constants';
import parseFienta from '../operations/parseFienta';
import parseEventim from '../operations/parseEventim';
import parseKontramarka from '../operations/parseKontramarka';
import parseTicketmaster from '../operations/parseTicketmaster';
import parseIsraelinfo from '../operations/parseIsraelinfo';
import { ParseRunCancelledError, markParseRunCancelled } from './logParseRun';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('START_PARSE_RUN');

const PARSERS = {
  [OPERATION_TYPES.parsingEventsFromFienta]: parseFienta,
  [OPERATION_TYPES.parsingEventsFromEventim]: parseEventim,
  [OPERATION_TYPES.parsingEventsFromKontramarka]: parseKontramarka,
  [OPERATION_TYPES.parsingEventsFromTicketmaster]: parseTicketmaster,
  [OPERATION_TYPES.parsingEventsFromIsraelinfo]: parseIsraelinfo,
};

export async function startParseRun(typeOrSource, meta = {}) {
  let type = typeOrSource;
  let source = SOURCE_BY_OPERATION_TYPE[typeOrSource];

  if (!source && Object.values(EVENT_SOURCE).includes(typeOrSource)) {
    source = typeOrSource;
    type = Object.entries(SOURCE_BY_OPERATION_TYPE).find(([, s]) => s === source)?.[0];
  }

  if (!type || !PARSERS[type]) {
    throw new Error(`Invalid parser type/source: ${typeOrSource}`);
  }

  const active = await ParseRunsSchema.findOne({
    source,
    status: { $in: [OPERATION_STATUSES.pending, OPERATION_STATUSES.processing] },
  }).lean();
  if (active) {
    const err = new Error(`Parse already running for ${source} (${active._id})`);
    err.status = 409;
    err.activeRunId = active._id;
    throw err;
  }

  const run = new ParseRunsSchema({
    source,
    status: OPERATION_STATUSES.pending,
    statistics: '',
    errorText: '',
    infoText: 'Parse run created, starting...',
    meta: meta || {},
    startedAt: new Date(),
    cancelRequested: false,
  });
  await run.save();

  setImmediate(async () => {
    try {
      await ParseRunsSchema.findByIdAndUpdate(run._id, {
        status: OPERATION_STATUSES.processing,
        infoText: 'Parsing started...',
      });

      await PARSERS[type]({ meta, runId: run._id });
    } catch (error) {
      if (error instanceof ParseRunCancelledError || error?.cancelled) {
        logger.info(`Parse run ${run._id} cancelled`);
        await markParseRunCancelled(run._id, 'Stopped by user');
        return;
      }
      logger.error(`Error in parse run ${run._id}: ${error.message || error}`);
      await ParseRunsSchema.findByIdAndUpdate(run._id, {
        status: OPERATION_STATUSES.error,
        errorText: error.message || 'Unknown error occurred',
        finishedAt: new Date(),
      });
    }
  });

  return run._id;
}

export default startParseRun;
