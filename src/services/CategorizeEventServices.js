import crypto from 'crypto';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import {
  categorizeEventsWithAi,
  categorizeEventWithWeb,
} from './AiCategoryServices';

let otherCategoryIdCache = null;

async function getOtherCategoryId() {
  if (otherCategoryIdCache) return otherCategoryIdCache;
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  otherCategoryIdCache = other?._id ? String(other._id) : null;
  return otherCategoryIdCache;
}

const eventWebsite = (event) => String(
  event?.contacts?.website || event?.website || event?.url || '',
).trim();

/**
 * After description enrich:
 * - if website → page/web grounded AI first (fixes «концерт»→Музыка for comedians, «показ»→Мода for films)
 * - else keywords → AI → «Другое»
 */
export async function categorizeNewEvent(event, source) {
  const stats = {
    categorizedByKeywords: 0,
    categorizedByAi: 0,
    categorizedByWeb: 0,
    noCategoryAfterAi: 0,
    openaiUsage: null,
  };

  delete event.specialization;
  delete event.description_ai_failed;

  const website = eventWebsite(event);
  if (website) {
    try {
      const web = await categorizeEventWithWeb(event);
      stats.openaiUsage = web.usage;
      if (web.categoryId) {
        event.events_category_id = web.categoryId;
        event.category_resolved_by = web.usedWeb ? 'ai_web' : 'ai_page';
        event.category_confidence = web.confidence;
        event.is_active = true;
        event.needs_manual_review = false;
        stats.categorizedByWeb = 1;
        return { event, stats };
      }
    } catch (e) {
      // fall through to keywords / batch AI
    }
  }

  const { categoryId, score } = await detectCategoryByKeywords(event, source);
  event.category_keyword_score = score;

  if (categoryId) {
    event.events_category_id = categoryId;
    event.category_resolved_by = 'keywords';
    event.is_active = true;
    event.needs_manual_review = false;
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
      source,
    }]);
    stats.openaiUsage = usage;
    const catId = aiMap.get(tempId);
    const suggested = aiSuggestions?.get(tempId) || null;
    if (catId) {
      event.events_category_id = catId;
      event.category_resolved_by = 'ai';
      event.is_active = true;
      event.needs_manual_review = false;
      stats.categorizedByAi = 1;
    } else {
      const otherId = await getOtherCategoryId();
      event.events_category_id = otherId;
      event.category_resolved_by = 'other';
      event.category_ai_failed = true;
      event.is_active = false;
      event.needs_manual_review = true;
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

  if (event.category_resolved_by === 'other') {
    event.is_active = false;
    event.needs_manual_review = true;
  }

  return { event, stats };
}

export default categorizeNewEvent;
