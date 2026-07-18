import crypto from 'crypto';
import mergeDuplicateEvents from '../helpers/mergeDuplicateEvents';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import { categorizeEventsWithAi } from './AiCategoryServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PROCESS_EVENTS');

export async function processParsedEvents(rawEvents, source) {
  const merged = mergeDuplicateEvents(rawEvents || []);
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
        index: i,
      });
    }
  }

  if (needsAi.length) {
    const aiMap = await categorizeEventsWithAi(needsAi);
    for (const item of needsAi) {
      const catId = aiMap.get(item.tempId);
      const ev = merged[item.index];
      if (catId) {
        ev.events_category_id = catId;
        ev.category_resolved_by = 'ai';
        categorizedByAi += 1;
      } else {
        ev.events_category_id = null;
        ev.category_resolved_by = null;
        ev.category_ai_failed = true;
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
  };

  logger.info(`Process stats (${source}): ${JSON.stringify(stats)}`);
  return { events: merged, stats };
}

export default processParsedEvents;
