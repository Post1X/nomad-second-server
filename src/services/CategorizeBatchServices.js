import crypto from 'crypto';
import { detectCategoryByKeywords } from './CategoryKeywordServices';
import { categorizeEventsWithAi } from './AiCategoryServices';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import BackfillRunsSchema from '../schemas/BackfillRunsSchema';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CATEGORIZE_BATCH');

async function getOtherCategoryId() {
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  return other?._id ? String(other._id) : null;
}

const bump = (obj, key) => {
  const k = key || 'unknown';
  obj[k] = (obj[k] || 0) + 1;
};

/**
 * @param {Array} events
 * @param {string} defaultSource keyword pack fallback
 * @param {{ purpose?: string, persist?: boolean, meta?: object }} options
 */
export async function categorizeBatch(events, defaultSource = 'backfill', options = {}) {
  const list = Array.isArray(events) ? events : [];
  const purpose = options.purpose || (defaultSource === 'backfill' ? 'backfill' : null);
  const persist = options.persist !== false && purpose === 'backfill';
  const results = [];
  const needsAi = [];

  for (let i = 0; i < list.length; i += 1) {
    const item = list[i];
    const eventId = item.event_id || item.eventId || item.id;
    const keywordSource = item.source || defaultSource || 'backfill';
    const ev = {
      name: item.name || '',
      description: item.description || '',
      specialization: item.specialization || '',
      address: item.address || '',
    };

    const cardMeta = {
      name: ev.name,
      description: ev.description,
      address: ev.address,
      website: item.website || '',
      holding_date: item.holding_date || '',
      date_start: item.date_start || null,
      date_end: item.date_end || null,
      min_price: item.min_price != null ? item.min_price : null,
      max_price: item.max_price != null ? item.max_price : null,
      currency: item.currency || '',
      specialization: item.specialization || '',
    };

    const { categoryId, score } = await detectCategoryByKeywords(ev, keywordSource);
    if (categoryId) {
      results.push({
        event_id: eventId,
        category_id: categoryId,
        resolved_by: 'keywords',
        score,
        source: keywordSource,
        city_id: item.city_id ? String(item.city_id) : null,
        country_id: item.country_id ? String(item.country_id) : null,
        enriched_description: !!item.enriched_description,
        ...cardMeta,
      });
    } else {
      const tempId = crypto.randomUUID();
      needsAi.push({
        tempId,
        eventId,
        ...cardMeta,
        source: keywordSource,
        city_id: item.city_id ? String(item.city_id) : null,
        country_id: item.country_id ? String(item.country_id) : null,
        enriched_description: !!item.enriched_description,
        index: results.length,
      });
      results.push(null);
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
    openaiUsage = usage || null;
    categorySuggestions = suggestionUpsert;
    tokensBySuggestion = tokenShares || [];
    const otherId = await getOtherCategoryId();
    for (const item of needsAi) {
      const catId = aiMap.get(item.tempId);
      const suggested = aiSuggestions?.get(item.tempId) || null;
      const card = {
        name: item.name || '',
        description: item.description || '',
        address: item.address || '',
        website: item.website || '',
        holding_date: item.holding_date || '',
        date_start: item.date_start || null,
        date_end: item.date_end || null,
        min_price: item.min_price != null ? item.min_price : null,
        max_price: item.max_price != null ? item.max_price : null,
        currency: item.currency || '',
        specialization: item.specialization || '',
      };
      if (catId) {
        results[item.index] = {
          event_id: item.eventId,
          category_id: catId,
          resolved_by: 'ai',
          source: item.source,
          city_id: item.city_id,
          country_id: item.country_id,
          enriched_description: item.enriched_description,
          ...card,
        };
      } else {
        results[item.index] = {
          event_id: item.eventId,
          category_id: otherId,
          resolved_by: 'other',
          suggested_name: suggested || null,
          source: item.source,
          city_id: item.city_id,
          country_id: item.country_id,
          enriched_description: item.enriched_description,
          ...card,
        };
      }
    }
  }

  const filtered = results.filter(Boolean);

  const byResolved = {};
  const bySource = {};
  let enrichedDescriptions = 0;
  for (const row of filtered) {
    bump(byResolved, row.resolved_by || 'unknown');
    const src = row.source || 'unknown';
    if (!bySource[src]) {
      bySource[src] = {
        total: 0,
        byResolvedBy: {},
        noCategory: 0,
        noCategoryAfterAi: 0,
        noCity: 0,
        byCategory: {},
        byCountry: {},
        byCity: {},
      };
    }
    const s = bySource[src];
    s.total += 1;
    bump(s.byResolvedBy, row.resolved_by || 'unknown');
    if (row.resolved_by === 'default_other' || row.resolved_by === 'other' || !row.category_id) {
      s.noCategory += 1;
      s.noCategoryAfterAi += 1;
    }
    if (!row.city_id) s.noCity += 1;
    bump(s.byCategory, row.category_id || 'other');
    bump(s.byCountry, row.country_id);
    bump(s.byCity, row.city_id);
    if (row.enriched_description) enrichedDescriptions += 1;
  }

  const statistics = {
    total: filtered.length,
    byResolved,
    bySource,
    enrichedDescriptions,
    openaiUsage,
    categorySuggestions,
    tokensBySuggestion,
  };

  let runId = null;
  if (persist && filtered.length) {
    const run = await BackfillRunsSchema.create({
      purpose: 'backfill',
      status: 'success',
      results: filtered.map((r) => ({
        event_id: r.event_id,
        source: r.source,
        category_id: r.category_id,
        resolved_by: r.resolved_by,
        score: r.score,
        city_id: r.city_id,
        country_id: r.country_id,
        enriched_description: !!r.enriched_description,
        name: r.name || '',
        description: r.description || '',
        address: r.address || '',
        website: r.website || '',
        holding_date: r.holding_date || '',
        date_start: r.date_start || null,
        date_end: r.date_end || null,
        min_price: r.min_price != null ? r.min_price : null,
        max_price: r.max_price != null ? r.max_price : null,
        currency: r.currency || '',
        specialization: r.specialization || '',
      })),
      statistics,
      openaiUsage,
      meta: options.meta || {},
    });
    runId = String(run._id);
    logger.info(`Backfill run saved ${runId}: ${filtered.length} events`);
  }

  logger.info(`Categorized batch: ${filtered.length} (defaultSource=${defaultSource}, purpose=${purpose || 'n/a'})`);
  return { results: filtered, openaiUsage, statistics, runId };
}

export default { categorizeBatch };
