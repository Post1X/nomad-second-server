import crypto from 'crypto';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { processParsedEvents } from '../services/ProcessParsedEventsServices';
import {
  eventFingerprint,
  classifyMatchStage,
  applyMergeToExisting,
} from './merge';
import { OPERATION_STATUSES } from './constants';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('SAVE_EVENTS');

const newParserUniqueId = () => crypto.randomUUID();

export async function saveProcessedEvents({
  runId,
  operationId,
  events,
  source,
  infoTexts = [],
  errorTexts = [],
  extraStatistics = {},
}) {
  const parseRunId = runId || operationId;
  const { events: processed, stats: processStats } = await processParsedEvents(events || [], source);

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  try {
    for (const event of processed) {
      const fingerprint = eventFingerprint(source, event.name, event.address);
      // eslint-disable-next-line no-await-in-loop
      const existingDoc = await ParsedEventsSchema.findOne({ source, fingerprint }).lean();
      const existingData = existingDoc?.event_data || null;
      const stage = classifyMatchStage(existingData, event);
      const { event: mergedEvent, changed } = applyMergeToExisting(existingData, event, stage);

      if (stage === 'skip' || !changed) {
        skipped += 1;
        continue;
      }

      if (!existingDoc) {
        const parserUniqueId = mergedEvent.parser_unique_id || newParserUniqueId();
        const eventData = { ...mergedEvent, parser_unique_id: parserUniqueId };
        // eslint-disable-next-line no-await-in-loop
        await ParsedEventsSchema.create({
          source,
          fingerprint,
          parser_unique_id: parserUniqueId,
          event_data: eventData,
          parse_run: parseRunId,
          exported_at: null,
        });
        inserted += 1;
      } else {
        const parserUniqueId = existingDoc.parser_unique_id
          || existingData?.parser_unique_id
          || newParserUniqueId();
        const eventData = { ...mergedEvent, parser_unique_id: parserUniqueId };
        // eslint-disable-next-line no-await-in-loop
        await ParsedEventsSchema.updateOne(
          { _id: existingDoc._id },
          {
            $set: {
              parser_unique_id: parserUniqueId,
              event_data: eventData,
              parse_run: parseRunId,
              exported_at: null,
            },
          },
        );
        updated += 1;
      }
    }

    const upsertStats = { inserted, updated, skipped };
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';
    const run = await ParseRunsSchema.findById(parseRunId);
    const finalInfoText = `${run?.infoText || ''}\nSaved: insert=${inserted}, update=${updated}, skip=${skipped}${additionalInfo}`;

    await ParseRunsSchema.findByIdAndUpdate(parseRunId, {
      status: OPERATION_STATUSES.success,
      finishedAt: new Date(),
      statistics: JSON.stringify({
        total: processed.length,
        upsert: upsertStats,
        errors: errorTexts.length,
        process: processStats,
        ...extraStatistics,
      }),
      errorText: errorTexts.join('\n'),
      infoText: finalInfoText,
    });

    logger.info(`Upserted events for run ${parseRunId}: ${JSON.stringify(upsertStats)}`);
    return { processed, processStats, upsertStats };
  } catch (error) {
    logger.error(`Error saving events: ${error.message || error}`);
    await ParseRunsSchema.findByIdAndUpdate(parseRunId, {
      status: OPERATION_STATUSES.error,
      errorText: error.message || 'Unknown error while saving events',
      finishedAt: new Date(),
    });
    throw error;
  }
}

export default saveProcessedEvents;
