import crypto from 'crypto';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { processParsedEvents } from '../services/ProcessParsedEventsServices';
import {
  eventFingerprint,
  classifyMatchStage,
  applyMergeToExisting,
  mergeCrossSourceEvent,
} from './merge';
import { OPERATION_STATUSES } from './constants';
import { assertParseRunActive, ParseRunCancelledError } from './logParseRun';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('SAVE_EVENTS');

const newParserUniqueId = () => crypto.randomUUID();

export async function saveProcessedEvents({
  runId,
  events,
  source,
  infoTexts = [],
  errorTexts = [],
  extraStatistics = {},
}) {
  const parseRunId = runId;
  await assertParseRunActive(parseRunId);
  const { events: processed, stats: processStats } = await processParsedEvents(events || [], source);

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  let crossMerged = 0;

  try {
    for (const event of processed) {
      // eslint-disable-next-line no-await-in-loop
      await assertParseRunActive(parseRunId);
      const fingerprint = eventFingerprint(event.name, event.city_id);
      // eslint-disable-next-line no-await-in-loop
      const existingDoc = await ParsedEventsSchema.findOne({ fingerprint }).lean();

      if (!existingDoc) {
        const parserUniqueId = event.parser_unique_id || newParserUniqueId();
        const eventData = { ...event, source, parser_unique_id: parserUniqueId };
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
        continue;
      }

      const existingData = existingDoc.event_data || null;
      const existingSource = existingDoc.source || existingData?.source;

      let mergedEvent;
      let winnerSource = existingSource;
      let changed = true;

      if (existingSource && existingSource !== source) {
        const cross = mergeCrossSourceEvent(existingData, existingSource, event, source);
        mergedEvent = cross.event;
        winnerSource = cross.winnerSource;
        crossMerged += 1;
      } else {
        const stage = classifyMatchStage(existingData, event);
        const applied = applyMergeToExisting(existingData, event, stage);
        mergedEvent = applied.event;
        changed = applied.changed && stage !== 'skip';
        winnerSource = source;
        if (stage === 'skip' || !changed) {
          skipped += 1;
          continue;
        }
      }

      const parserUniqueId = existingDoc.parser_unique_id
        || existingData?.parser_unique_id
        || mergedEvent.parser_unique_id
        || newParserUniqueId();
      const eventData = {
        ...mergedEvent,
        source: winnerSource,
        parser_unique_id: parserUniqueId,
      };

      // eslint-disable-next-line no-await-in-loop
      await ParsedEventsSchema.updateOne(
        { _id: existingDoc._id },
        {
          $set: {
            source: winnerSource,
            parser_unique_id: parserUniqueId,
            event_data: eventData,
            parse_run: parseRunId,
            exported_at: null,
          },
        },
      );
      updated += 1;
    }

    const upsertStats = { inserted, updated, skipped, crossMerged };
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';
    const run = await ParseRunsSchema.findById(parseRunId);
    const finalInfoText = `${run?.infoText || ''}\nSaved: insert=${inserted}, update=${updated}, skip=${skipped}, cross=${crossMerged}${additionalInfo}`;

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
    if (error instanceof ParseRunCancelledError || error?.cancelled) {
      throw error;
    }
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
