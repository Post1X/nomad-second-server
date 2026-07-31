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

export const mergeDuplicateEventsForSource = (events, source) => {
  const opts = {
    source: source || EVENT_SOURCE.nomad,
    includeCityInKey: false,
  };
  return mergeDuplicateEvents(events || [], opts);
};

export default mergeDuplicateEventsForSource;
