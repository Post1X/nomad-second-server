import { EVENT_SOURCE } from '../constants';
import { mergeDuplicateEvents } from './mergeDuplicateEvents';

export {
  mergeDuplicateEvents,
  formatHoldingDate,
  formatHoldingDateNumeric,
  parseHoldingDate,
  mergeHoldingDates,
  eventFingerprint,
} from './mergeDuplicateEvents';
export {
  FULL_MATCH_FIELDS,
  classifyMatchStage,
  applyMergeToExisting,
} from './upsertStages';
export {
  pickWinnerSource,
  mergeCrossSourceEvent,
} from './crossSource';

export const mergeDuplicateEventsForSource = (events, source) => {
  return mergeDuplicateEvents(events || [], {
    source: source || EVENT_SOURCE.nomad,
  });
};

export default mergeDuplicateEventsForSource;
