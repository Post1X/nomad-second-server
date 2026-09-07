import crypto from 'crypto';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { processParsedEvents } from '../services/ProcessParsedEventsServices';
import { categorizeNewEvent } from '../services/CategorizeEventServices';
import { enrichEventDescriptions } from '../services/AiDescriptionServices';
import {
  nameKey,
  cityKey,
  applyPriorityMerge,
  mergeCrossSourceEvent,
} from './merge';
import { SOURCE_PRIORITY, OPERATION_STATUSES } from './constants';
import logParseRun, { assertParseRunActive, ParseRunCancelledError } from './logParseRun';
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
  delete next.specialization;
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
  await logParseRun(
    parseRunId,
    `[${new Date().toISOString()}] Saving: merge/filter ${events?.length || 0} parsed events...`,
  );
  const { events: processed, stats: processStats } = await processParsedEvents(events || [], source);
  await logParseRun(
    parseRunId,
    `[${new Date().toISOString()}] Saving: upsert ${processed.length} events `
    + `(afterMerge=${processStats.afterMerge}, skippedNoCity=${processStats.skippedNoCity}, `
    + `skippedPast=${processStats.skippedPast})`,
  );

  await assertParseRunActive(parseRunId);
  await logParseRun(
    parseRunId,
    `[${new Date().toISOString()}] Saving: AI description enrich for ${processed.length} events...`,
  );
  const { stats: descStats } = await enrichEventDescriptions(processed);
  await logParseRun(
    parseRunId,
    `[${new Date().toISOString()}] Description enrich done: `
    + `empty=${descStats.emptyOrCopy}, suspicious=${descStats.markedSuspicious}, `
    + `rewritten=${descStats.rewritten}, leftAsIs=${descStats.leftAsIs}`,
  );

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  let discardedLowPriority = 0;
  let crossMerged = 0;
  let categorizedByKeywords = 0;
  let categorizedByAi = 0;
  let noCategoryAfterAi = 0;
  const openaiUsage = {
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    batches: 0,
    failedBatches: 0,
  };
  const addUsage = (usage) => {
    if (!usage || typeof usage !== 'object') return;
    openaiUsage.prompt_tokens += Number(usage.prompt_tokens) || 0;
    openaiUsage.completion_tokens += Number(usage.completion_tokens) || 0;
    openaiUsage.total_tokens += Number(usage.total_tokens) || 0;
    openaiUsage.batches += Number(usage.batches) || 0;
    openaiUsage.failedBatches += Number(usage.failedBatches) || 0;
  };
  addUsage(descStats.openaiUsage);
  const progressEvery = Math.max(25, Math.min(100, Math.ceil(processed.length / 20) || 25));

  try {
    for (let idx = 0; idx < processed.length; idx += 1) {
      const event = processed[idx];
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
        addUsage(catStats.openaiUsage);

        const parserUniqueId = categorized.parser_unique_id || newParserUniqueId();
        const eventData = stripDeprecated({
          ...categorized,
          source,
          parser_unique_id: parserUniqueId,
        });
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
        if ((idx + 1) % progressEvery === 0 || idx + 1 === processed.length) {
          // eslint-disable-next-line no-await-in-loop
          await logParseRun(
            parseRunId,
            `[${new Date().toISOString()}] Upsert progress: ${idx + 1}/${processed.length} `
            + `(insert=${inserted}, update=${updated}, skip=${skipped})`,
          );
        }
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
      if (eventData.category_resolved_by === 'none'
        || eventData.category_resolved_by === 'None'
        || eventData.category_resolved_by === 'default_other') {
        eventData.category_resolved_by = 'other';
      }

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

      if ((idx + 1) % progressEvery === 0 || idx + 1 === processed.length) {
        // eslint-disable-next-line no-await-in-loop
        await logParseRun(
          parseRunId,
          `[${new Date().toISOString()}] Upsert progress: ${idx + 1}/${processed.length} `
          + `(insert=${inserted}, update=${updated}, skip=${skipped})`,
        );
      }
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
      descriptionEnrich: {
        emptyOrCopy: descStats.emptyOrCopy,
        markedSuspicious: descStats.markedSuspicious,
        rewritten: descStats.rewritten,
        leftAsIs: descStats.leftAsIs,
      },
      openaiUsage,
    };
    const processForUi = {
      ...processStats,
      categorizedByKeywords,
      categorizedByAi,
      noCategoryAfterAi,
      openaiUsage,
    };
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';
    const run = await ParseRunsSchema.findById(parseRunId);
    const tok = openaiUsage.total_tokens
      ? `, openai=${openaiUsage.total_tokens} tok (ai=${categorizedByAi}, kw=${categorizedByKeywords})`
      : `, cat kw=${categorizedByKeywords} ai=${categorizedByAi}`;
    const finalInfoText = `${run?.infoText || ''}\nSaved: insert=${inserted}, update=${updated}, skip=${skipped}, discardLow=${discardedLowPriority}, cross=${crossMerged}${tok}${additionalInfo}`;

    await ParseRunsSchema.findByIdAndUpdate(parseRunId, {
      status: OPERATION_STATUSES.success,
      finishedAt: new Date(),
      statistics: JSON.stringify({
        total: processed.length,
        upsert: upsertStats,
        errors: errorTexts.length,
        process: processForUi,
        openaiUsage,
        ...extraStatistics,
      }),
      errorText: errorTexts.join('\n'),
      infoText: finalInfoText,
    });

    logger.info(`Upserted events for run ${parseRunId}: ${JSON.stringify(upsertStats)}`);
    return { processed, processStats: processForUi, upsertStats };
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
