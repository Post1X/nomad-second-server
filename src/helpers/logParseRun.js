import ParseRunsSchema from '../schemas/ParseRunsSchema';
import { OPERATION_STATUSES } from './constants';

export class ParseRunCancelledError extends Error {
  constructor(runId) {
    super(`Parse run ${runId} cancelled`);
    this.name = 'ParseRunCancelledError';
    this.cancelled = true;
    this.runId = runId;
  }
}

export const isCancelledError = (error) => Boolean(
  error instanceof ParseRunCancelledError || error?.cancelled,
);

export const isParseRunCancelled = async (runId) => {
  if (!runId) return false;
  const run = await ParseRunsSchema.findById(runId).select('cancelRequested status').lean();
  return Boolean(run?.cancelRequested);
};

export const assertParseRunActive = async (runId) => {
  if (await isParseRunCancelled(runId)) {
    throw new ParseRunCancelledError(runId);
  }
};

export const requestParseRunStop = async (runId) => {
  const run = await ParseRunsSchema.findById(runId);
  if (!run) {
    const err = new Error('Parse run not found');
    err.status = 404;
    throw err;
  }
  if ([OPERATION_STATUSES.success, OPERATION_STATUSES.error, OPERATION_STATUSES.cancelled]
    .includes(run.status)) {
    const err = new Error(`Parse run already finished (${run.status})`);
    err.status = 409;
    throw err;
  }
  const stamp = `[${new Date().toISOString()}] Stop requested`;
  await ParseRunsSchema.findByIdAndUpdate(runId, {
    cancelRequested: true,
    infoText: `${run.infoText || ''}\n${stamp}`,
  });
  return ParseRunsSchema.findById(runId).lean();
};

export const markParseRunCancelled = async (runId, message = 'Stopped by user') => {
  await ParseRunsSchema.findByIdAndUpdate(runId, {
    status: OPERATION_STATUSES.cancelled,
    errorText: message,
    finishedAt: new Date(),
    cancelRequested: true,
  });
};

/**
 * Append log line; throws ParseRunCancelledError if stop was requested.
 */
export const logParseRun = async (runId, message) => {
  if (!runId || !message) return;
  const run = await ParseRunsSchema.findById(runId).select('infoText cancelRequested').lean();
  if (run?.cancelRequested) {
    throw new ParseRunCancelledError(runId);
  }
  await ParseRunsSchema.findByIdAndUpdate(runId, {
    infoText: `${run?.infoText || ''}\n${message}`,
  });
};

export default logParseRun;
