import crypto from 'crypto';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { processParsedEvents } from '../services/ProcessParsedEventsServices';
import { categorizeNewEvent } from '../services/CategorizeEventServices';
import {
  nameKey,
  cityKey,
  applyPriorityMerge,
  mergeCrossSourceEvent,
} from './merge';
import { SOURCE_PRIORITY, OPERATION_STATUSES } from './constants';
import { assertParseRunActive, ParseRunCancelledError } from './logParseRun';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('SAVE_EVENTS');

const newParserUniqueId = () => crypto.randomUUID();
const sourceRank = (s) => SOURCE_PRIORITY[s] ?? 0;

const stripDeprecated = (event) => {
  const next = { ...event };
  delete next.ticketmaster_id;
  delete next.is_hidden;
  delete next.fingerprint;
  delete next._mergeDates;
  delete next._tempId;
  return next;
};

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
  let discardedLowPriority = 0;
  let crossMerged = 0;
  let categorizedByKeywords = 0;
  let categorizedByAi = 0;
  let noCategoryAfterAi = 0;

  try {
    for (const event of processed) {
      // eslint-disable-next-line no-await-in-loop
      await assertParseRunActive(parseRunId);

      const nk = nameKey(event.name);
      const cid = cityKey(event.city_id);
      // eslint-disable-next-line no-await-in-loop
      const existingDoc = await ParsedEventsSchema.findOne({ name_key: nk, city_id: cid }).lean();

      if (!existingDoc) {
        // eslint-disable-next-line no-await-in-loop
        const { event: categorized, stats: catStats } = await categorizeNewEvent(
          stripDeprecated({ ...event, source }),
          source,
        );
        categorizedByKeywords += catStats.categorizedByKeywords;
        categorizedByAi += catStats.categorizedByAi;
        noCategoryAfterAi += catStats.noCategoryAfterAi;

        const parserUniqueId = categorized.parser_unique_id || newParserUniqueId();
        const eventData = { ...categorized, source, parser_unique_id: parserUniqueId };
        // eslint-disable-next-line no-await-in-loop
        await ParsedEventsSchema.create({
          source,
          name_key: nk,
          city_id: cid,
          parser_unique_id: parserUniqueId,
          event_data: eventData,
          parse_run: parseRunId,
        });
        inserted += 1;
        continue;
      }

      const existingData = existingDoc.event_data || {};
      const existingSource = existingDoc.source || existingData.source;

      if (sourceRank(source) < sourceRank(existingSource)) {
        discardedLowPriority += 1;
        skipped += 1;
        continue;
      }

      let mergedEvent;
      let winnerSource = source;

      if (existingSource && existingSource !== source) {
        const cross = mergeCrossSourceEvent(existingData, existingSource, event, source);
        if (cross.discarded) {
          discardedLowPriority += 1;
          skipped += 1;
          continue;
        }
        mergedEvent = cross.event;
        winnerSource = cross.winnerSource;
        crossMerged += 1;
      } else {
        const { event: merged, changed } = applyPriorityMerge(
          existingData,
          stripDeprecated({ ...event, source }),
          { primaryIsIncoming: true },
        );
        if (!changed) {
          skipped += 1;
          continue;
        }
        mergedEvent = merged;
        winnerSource = source;
      }

      const parserUniqueId = existingDoc.parser_unique_id
        || existingData.parser_unique_id
        || mergedEvent.parser_unique_id
        || newParserUniqueId();
      const eventData = stripDeprecated({
        ...mergedEvent,
        source: winnerSource,
        parser_unique_id: parserUniqueId,
      });

      // eslint-disable-next-line no-await-in-loop
      await ParsedEventsSchema.updateOne(
        { _id: existingDoc._id },
        {
          $set: {
            source: winnerSource,
            name_key: nk,
            city_id: cid,
            parser_unique_id: parserUniqueId,
            event_data: eventData,
            parse_run: parseRunId,
          },
          $unset: { fingerprint: 1, exported_at: 1 },
        },
      );
      updated += 1;
    }

    const upsertStats = {
      inserted,
      updated,
      skipped,
      discardedLowPriority,
      crossMerged,
      categorizedByKeywords,
      categorizedByAi,
      noCategoryAfterAi,
    };
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';
    const run = await ParseRunsSchema.findById(parseRunId);
    const finalInfoText = `${run?.infoText || ''}\nSaved: insert=${inserted}, update=${updated}, skip=${skipped}, discardLow=${discardedLowPriority}, cross=${crossMerged}${additionalInfo}`;

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
