import { mergeDuplicateEventsForSource } from '../helpers/merge';
import { filterIngestEvents } from '../helpers/eventFilters';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PROCESS_EVENTS');

/**
 * In-batch merge + ingest filters (past / no city).
 * Category resolution happens only on create in saveProcessedEvents.
 */
export async function processParsedEvents(rawEvents, source) {
  const merged = mergeDuplicateEventsForSource(rawEvents || [], source);
  logger.info(`Merge: ${rawEvents?.length || 0} → ${merged.length} (source=${source})`);

  const { events, skippedPast, skippedNoCity } = filterIngestEvents(merged);

  const stats = {
    input: rawEvents?.length || 0,
    afterMerge: merged.length,
    afterFilters: events.length,
    skippedPast,
    skippedNoCity,
    noCity: skippedNoCity,
  };

  logger.info(`Process stats (${source}): ${JSON.stringify(stats)}`);
  return { events, stats };
}

export default processParsedEvents;
