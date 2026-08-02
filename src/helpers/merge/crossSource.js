import { SOURCE_PRIORITY } from '../constants';
import { applyMergeToExisting, classifyMatchStage } from './upsertStages';

const sourceRank = (source) => SOURCE_PRIORITY[source] ?? 0;

/**
 * Prefer higher SOURCE_PRIORITY; on tie keep existing (stable parser_unique_id / source).
 */
export const pickWinnerSource = (existingSource, incomingSource) => {
  const ra = sourceRank(existingSource);
  const rb = sourceRank(incomingSource);
  if (rb > ra) return 'incoming';
  return 'existing';
};

/**
 * Merge incoming event into existing ParsedEvents row (possibly other source).
 * Dates/prices always unioned; non-date fields from higher-priority source.
 */
export const mergeCrossSourceEvent = (existingData, existingSource, incoming, incomingSource) => {
  const stage = existingData
    ? classifyMatchStage(
      { ...existingData, city_id: existingData.city_id },
      { ...incoming, city_id: incoming.city_id },
    )
    : 'insert';

  // Force field/date merge path when identity matches (same name+city)
  const mergeStage = stage === 'insert' ? 'merge_dates_prices' : stage;
  const { event: dateMerged } = applyMergeToExisting(
    existingData || {},
    incoming,
    mergeStage === 'skip' ? 'update_fields' : mergeStage,
  );

  const winner = pickWinnerSource(existingSource, incomingSource);
  const primary = winner === 'incoming'
    ? { ...dateMerged, ...incoming, source: incomingSource }
    : {
      ...incoming,
      ...dateMerged,
      ...existingData,
      // keep unioned dates/prices from dateMerged
      date_start: dateMerged.date_start,
      date_end: dateMerged.date_end,
      holding_date: dateMerged.holding_date,
      min_price: dateMerged.min_price,
      max_price: dateMerged.max_price,
      source: existingSource,
    };

  // description: keep longer
  const descA = String(existingData?.description || '');
  const descB = String(incoming.description || '');
  if (descB.length > descA.length) primary.description = incoming.description;

  primary.parser_unique_id = existingData?.parser_unique_id || incoming.parser_unique_id || null;
  primary.ticketmaster_id = primary.ticketmaster_id
    || existingData?.ticketmaster_id
    || incoming.ticketmaster_id
    || null;

  const winnerSource = winner === 'incoming' ? incomingSource : existingSource;
  return {
    event: { ...primary, source: winnerSource },
    winnerSource,
    changed: true,
  };
};

export default {
  pickWinnerSource,
  mergeCrossSourceEvent,
};
