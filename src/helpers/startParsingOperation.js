import startParseRun from './startParseRun';

export async function startParsingOperation(type, meta = {}) {
  return startParseRun(type, meta);
}

export default startParsingOperation;
