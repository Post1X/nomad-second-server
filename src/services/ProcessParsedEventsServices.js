import crypto from 'crypto';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { mergeDuplicateEventsForSource } from '../helpers/merge';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import { categorizeEventsWithAi } from './AiCategoryServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PROCESS_EVENTS');

let otherCategoryIdCache = null;

async function getOtherCategoryId() {
  if (otherCategoryIdCache) return otherCategoryIdCache;
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  otherCategoryIdCache = other?._id ? String(other._id) : null;
  return otherCategoryIdCache;
}

export async function processParsedEvents(rawEvents, source) {
  const merged = mergeDuplicateEventsForSource(rawEvents || [], source);
  logger.info(`Merge: ${rawEvents?.length || 0} → ${merged.length} (source=${source})`);

  let categorizedByKeywords = 0;
  let noCategoryAfterKeywords = 0;
  let categorizedByAi = 0;
  let noCategoryAfterAi = 0;
  let noCity = 0;

  const needsAi = [];

  for (let i = 0; i < merged.length; i += 1) {
    const ev = merged[i];
    if (!ev.city_id) {
      noCity += 1;
      ev.no_city = true;
    }

    const { categoryId, score } = await detectCategoryByKeywords(ev, source);
    ev.category_keyword_score = score;

    if (categoryId) {
      ev.events_category_id = categoryId;
      ev.category_resolved_by = 'keywords';
      categorizedByKeywords += 1;
    } else {
      ev.events_category_id = null;
      ev.category_resolved_by = null;
      noCategoryAfterKeywords += 1;
      const tempId = crypto.randomUUID();
      ev._tempId = tempId;
      needsAi.push({
        tempId,
        name: ev.name,
        description: ev.description,
        address: ev.address,
        source,
        index: i,
      });
    }
  }

  let openaiUsage = null;
  let categorySuggestions = null;
  let tokensBySuggestion = [];
  if (needsAi.length) {
    const {
      map: aiMap,
      suggestions: aiSuggestions,
      usage,
      suggestionUpsert,
      tokensBySuggestion: tokenShares,
    } = await categorizeEventsWithAi(needsAi);
    openaiUsage = usage;
    categorySuggestions = suggestionUpsert;
    tokensBySuggestion = tokenShares || [];
    const otherId = await getOtherCategoryId();
    for (const item of needsAi) {
      const catId = aiMap.get(item.tempId);
      const suggested = aiSuggestions?.get(item.tempId) || null;
      const ev = merged[item.index];
      if (catId) {
        ev.events_category_id = catId;
        ev.category_resolved_by = 'ai';
        categorizedByAi += 1;
      } else if (otherId) {
        ev.events_category_id = otherId;
        ev.category_resolved_by = 'default_other';
        ev.category_ai_failed = true;
        if (suggested) ev.category_suggested_name = suggested;
        noCategoryAfterAi += 1;
      } else {
        ev.events_category_id = null;
        ev.category_resolved_by = 'default_other';
        ev.category_ai_failed = true;
        if (suggested) ev.category_suggested_name = suggested;
        noCategoryAfterAi += 1;
      }
      delete ev._tempId;
    }
  }

  const stats = {
    input: rawEvents?.length || 0,
    afterMerge: merged.length,
    categorizedByKeywords,
    noCategoryAfterKeywords,
    categorizedByAi,
    noCategoryAfterAi,
    noCity,
    openaiUsage,
    categorySuggestions,
    tokensBySuggestion,
  };

  logger.info(`Process stats (${source}): ${JSON.stringify(stats)}`);
  return { events: merged, stats };
}

export default processParsedEvents;
