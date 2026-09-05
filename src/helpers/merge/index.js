import { EVENT_SOURCE } from '../constants';
import { mergeDuplicateEvents } from './mergeDuplicateEvents';

export {
  mergeDuplicateEvents,
  formatHoldingDate,
  formatHoldingDateNumeric,
  parseHoldingDate,
  mergeHoldingDates,
  nameKey,
  cityKey,
  normalize,
} from './mergeDuplicateEvents';
export {
  FULL_MATCH_FIELDS,
  PRIORITY_FIELDS,
  classifyMatchStage,
  applyMergeToExisting,
  applyPriorityMerge,
  unionDatesAndPrices,
  collectDates,
  mergePhotos,
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
