import { SOURCE_PRIORITY } from '../constants';
import { applyPriorityMerge } from './upsertStages';

const sourceRank = (source) => SOURCE_PRIORITY[source] ?? 0;

/**
 * Higher → merge with incoming as primary.
 * Equal → merge (incoming = newer for priority fields; dates/photos union).
 * Lower → discard_incoming.
 */
export const pickWinnerSource = (existingSource, incomingSource) => {
  const ra = sourceRank(existingSource);
  const rb = sourceRank(incomingSource);
  if (rb < ra) return 'discard_incoming';
  return 'incoming'; // higher or equal → merge
};

/**
 * Merge when incoming priority is higher or equal.
 * address/coords/contacts/... from primary; photos + dates/prices always unioned.
 */
export const mergeCrossSourceEvent = (existingData, existingSource, incoming, incomingSource) => {
  const decision = pickWinnerSource(existingSource, incomingSource);
  if (decision === 'discard_incoming') {
    return {
      event: existingData,
      winnerSource: existingSource,
      changed: false,
      discarded: true,
    };
  }

  const { event } = applyPriorityMerge(
    { ...existingData, source: existingSource },
    { ...incoming, source: incomingSource },
    { primaryIsIncoming: true },
  );

  const winnerSource = incomingSource;
  return {
    event: { ...event, source: winnerSource },
    winnerSource,
    changed: true,
    discarded: false,
  };
};

export default {
  pickWinnerSource,
  mergeCrossSourceEvent,
  sourceRank,
};
