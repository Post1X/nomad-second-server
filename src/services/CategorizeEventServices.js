import crypto from 'crypto';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import { categorizeEventsWithAi } from './AiCategoryServices';

let otherCategoryIdCache = null;

async function getOtherCategoryId() {
  if (otherCategoryIdCache) return otherCategoryIdCache;
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  otherCategoryIdCache = other?._id ? String(other._id) : null;
  return otherCategoryIdCache;
}

/**
 * Categorize a single new event (keywords → AI → default_other).
 * Also fills specialization from category name when empty.
 */
export async function categorizeNewEvent(event, source) {
  const stats = {
    categorizedByKeywords: 0,
    categorizedByAi: 0,
    noCategoryAfterAi: 0,
    openaiUsage: null,
  };

  const { categoryId, score } = await detectCategoryByKeywords(event, source);
  event.category_keyword_score = score;

  if (categoryId) {
    event.events_category_id = categoryId;
    event.category_resolved_by = 'keywords';
    stats.categorizedByKeywords = 1;
  } else {
    const tempId = crypto.randomUUID();
    const {
      map: aiMap,
      suggestions: aiSuggestions,
      usage,
    } = await categorizeEventsWithAi([{
      tempId,
      name: event.name,
      description: event.description,
      address: event.address,
      specialization: event.specialization,
      source,
    }]);
    stats.openaiUsage = usage;
    const catId = aiMap.get(tempId);
    const suggested = aiSuggestions?.get(tempId) || null;
    if (catId) {
      event.events_category_id = catId;
      event.category_resolved_by = 'ai';
      stats.categorizedByAi = 1;
    } else {
      const otherId = await getOtherCategoryId();
      event.events_category_id = otherId;
      // "other" (not none / default_other) — main may still accept default_other via pull mapping
      event.category_resolved_by = 'other';
      event.category_ai_failed = true;
      if (suggested) event.category_suggested_name = suggested;
      stats.noCategoryAfterAi = 1;
    }
  }

  if (event.category_resolved_by === 'none' || event.category_resolved_by === 'None') {
    event.category_resolved_by = 'other';
  }
  if (event.category_resolved_by === 'default_other') {
    event.category_resolved_by = 'other';
  }

  // specialization: if placeholder / missing → category name (all sources)
  const spec = String(event.specialization || '').trim();
  if (!spec || spec === 'Event' || /^none$/i.test(spec)) {
    if (event.events_category_id) {
      const cat = await EventsCategoriesSchema.findById(event.events_category_id).lean();
      if (cat?.name) {
        event.specialization = /^none$/i.test(cat.name) ? 'Другое' : cat.name;
      } else {
        event.specialization = 'Другое';
      }
    } else {
      event.specialization = 'Другое';
    }
  }

  return { event, stats };
}

export default categorizeNewEvent;
