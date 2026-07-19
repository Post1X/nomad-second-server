import crypto from 'crypto';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import { categorizeEventsWithAi } from './AiCategoryServices';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CATEGORIZE_BATCH');

async function getOtherCategoryId() {
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  return other?._id ? String(other._id) : null;
}

export async function categorizeBatch(events, source = 'backfill') {
  const list = Array.isArray(events) ? events : [];
  const results = [];
  const needsAi = [];

  for (let i = 0; i < list.length; i += 1) {
    const item = list[i];
    const eventId = item.event_id || item.eventId || item.id;
    const ev = {
      name: item.name || '',
      description: item.description || '',
      specialization: item.specialization || '',
      address: item.address || '',
    };

    const { categoryId, score } = await detectCategoryByKeywords(ev, source);
    if (categoryId) {
      results.push({
        event_id: eventId,
        category_id: categoryId,
        resolved_by: 'keywords',
        score,
      });
    } else {
      const tempId = crypto.randomUUID();
      needsAi.push({
        tempId,
        eventId,
        name: ev.name,
        description: ev.description,
        address: ev.address,
        index: results.length,
      });
      results.push(null);
    }
  }

  if (needsAi.length) {
    const { map: aiMap } = await categorizeEventsWithAi(needsAi);
    const otherId = await getOtherCategoryId();
    for (const item of needsAi) {
      const catId = aiMap.get(item.tempId);
      if (catId) {
        results[item.index] = {
          event_id: item.eventId,
          category_id: catId,
          resolved_by: 'ai',
        };
      } else {
        results[item.index] = {
          event_id: item.eventId,
          category_id: otherId,
          resolved_by: 'default_other',
        };
      }
    }
  }

  const filtered = results.filter(Boolean);
  logger.info(`Categorized batch: ${filtered.length} (source=${source})`);
  return filtered;
}

export default { categorizeBatch };
