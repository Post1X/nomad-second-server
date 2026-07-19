import ParseRunsSchema from '../schemas/ParseRunsSchema';

export const logParseRun = async (runId, message) => {
  if (!runId || !message) return;
  try {
    const run = await ParseRunsSchema.findById(runId);
    await ParseRunsSchema.findByIdAndUpdate(runId, {
      infoText: `${run?.infoText || ''}\n${message}`,
    });
  } catch (_) {
    // ignore logging failures
  }
};

export default logParseRun;
