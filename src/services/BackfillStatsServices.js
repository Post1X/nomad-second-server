import BackfillRunsSchema from '../schemas/BackfillRunsSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import { EVENT_SOURCE } from '../helpers/constants';
import StatsServices from './StatsServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('BACKFILL_STATS');

const bump = (obj, key) => {
  const k = key || 'unknown';
  obj[k] = (obj[k] || 0) + 1;
};

const shortName = (name) => {
  if (!name) return 'unknown';
  const part = String(name).split('|')[0].trim();
  return part || String(name);
};

const toSortedList = (mapObj, labelFn) => Object.entries(mapObj || {})
  .map(([id, count]) => ({
    id,
    name: labelFn(id),
    count,
  }))
  .sort((a, b) => b.count - a.count);

class BackfillStatsServices {
  static async getBackfillStats({ source, sources, from, to } = {}) {
    const range = (from && to)
      ? { from: new Date(from), to: new Date(to) }
      : StatsServices.getPreviousWeekRange();

    let sourceList;
    if (Array.isArray(sources) && sources.length) {
      sourceList = sources;
    } else if (source) {
      sourceList = String(source).split(',').map((s) => s.trim()).filter(Boolean);
    } else {
      sourceList = Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);
    }
    const sourceSet = new Set(sourceList);

    const runs = await BackfillRunsSchema.find({
      purpose: 'backfill',
      createdAt: { $gte: range.from, $lt: range.to },
    }).lean();

    const [countries, cities, categories] = await Promise.all([
      CountriesSchema.find({}).select('_id name').lean(),
      CitiesSchema.find({}).select('_id name').lean(),
      EventsCategoriesSchema.find({}).select('_id name').lean(),
    ]);
    const labels = {
      countries: new Map(countries.map((c) => [String(c._id), shortName(c.name)])),
      cities: new Map(cities.map((c) => [String(c._id), shortName(c.name)])),
      categories: new Map(categories.map((c) => [String(c._id), c.name])),
    };

    const bySource = {};
    const totals = {
      total: 0,
      noCategory: 0,
      noCategoryAfterAi: 0,
      noCity: 0,
      enrichedDescriptions: 0,
      byCountry: {},
      byCity: {},
      byCategory: {},
      byResolvedBy: {},
    };
    let openaiUsage = {
      prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
    };

    for (const src of sourceList) {
      bySource[src] = {
        total: 0,
        noCategory: 0,
        noCategoryAfterAi: 0,
        noCity: 0,
        byCountry: {},
        byCity: {},
        byCategory: {},
        byResolvedBy: {},
        countries: [],
        cities: [],
        categories: [],
        resolvedBy: [],
      };
    }

    for (const run of runs) {
      const usage = run.openaiUsage || run.statistics?.openaiUsage;
      if (usage) {
        openaiUsage.prompt_tokens += usage.prompt_tokens || 0;
        openaiUsage.completion_tokens += usage.completion_tokens || 0;
        openaiUsage.total_tokens += usage.total_tokens || 0;
      }

      for (const row of run.results || []) {
        const src = row.source || 'unknown';
        if (!sourceSet.has(src)) continue;

        if (!bySource[src]) {
          bySource[src] = {
            total: 0,
            noCategory: 0,
            noCategoryAfterAi: 0,
            noCity: 0,
            byCountry: {},
            byCity: {},
            byCategory: {},
            byResolvedBy: {},
            countries: [],
            cities: [],
            categories: [],
            resolvedBy: [],
          };
        }

        const s = bySource[src];
        s.total += 1;
        totals.total += 1;
        bump(s.byResolvedBy, row.resolved_by || 'none');
        bump(totals.byResolvedBy, row.resolved_by || 'none');

        if (row.resolved_by === 'default_other' || !row.category_id) {
          s.noCategory += 1;
          s.noCategoryAfterAi += 1;
          totals.noCategory += 1;
          totals.noCategoryAfterAi += 1;
          bump(s.byCategory, row.category_id || 'other');
          bump(totals.byCategory, row.category_id || 'other');
        } else if (row.category_id) {
          bump(s.byCategory, String(row.category_id));
          bump(totals.byCategory, String(row.category_id));
        }

        bump(s.byCountry, row.country_id);
        bump(s.byCity, row.city_id);
        bump(totals.byCountry, row.country_id);
        bump(totals.byCity, row.city_id);

        if (!row.city_id) {
          s.noCity += 1;
          totals.noCity += 1;
        }
        if (row.enriched_description) totals.enrichedDescriptions += 1;
      }
    }

    for (const src of Object.keys(bySource)) {
      const s = bySource[src];
      s.countries = toSortedList(s.byCountry, (id) => labels.countries.get(id) || id);
      s.cities = toSortedList(s.byCity, (id) => labels.cities.get(id) || id);
      s.categories = toSortedList(s.byCategory, (id) => labels.categories.get(id) || id);
      s.resolvedBy = toSortedList(s.byResolvedBy, (id) => id);
    }

    const summary = {
      ...totals,
      countries: toSortedList(totals.byCountry, (id) => labels.countries.get(id) || id),
      cities: toSortedList(totals.byCity, (id) => labels.cities.get(id) || id),
      categories: toSortedList(totals.byCategory, (id) => labels.categories.get(id) || id),
      resolvedBy: toSortedList(totals.byResolvedBy, (id) => id),
      openaiUsage,
      runs: runs.length,
    };

    const mapCard = (row, runId, createdAt) => ({
      event_id: row.event_id,
      run_id: runId ? String(runId) : null,
      createdAt,
      name: row.name || '(без названия)',
      description: row.description || '',
      address: row.address || '',
      website: row.website || '',
      ticketmaster_id: row.ticketmaster_id || '',
      holding_date: row.holding_date || '',
      date_start: row.date_start || null,
      date_end: row.date_end || null,
      min_price: row.min_price != null ? row.min_price : null,
      max_price: row.max_price != null ? row.max_price : null,
      currency: row.currency || '',
      specialization: row.specialization || '',
      source: row.source || 'unknown',
      resolved_by: row.resolved_by || 'unknown',
      enriched_description: !!row.enriched_description,
      category_id: row.category_id || null,
      category_name: row.category_id
        ? (labels.categories.get(String(row.category_id)) || String(row.category_id))
        : '—',
      city_id: row.city_id || null,
      city_name: row.city_id
        ? (labels.cities.get(String(row.city_id)) || String(row.city_id))
        : '—',
      country_id: row.country_id || null,
      country_name: row.country_id
        ? (labels.countries.get(String(row.country_id)) || String(row.country_id))
        : '—',
    });

    const eventCards = [];
    const backfillRuns = runs.map((r) => {
      const cards = (r.results || [])
        .filter((row) => sourceSet.has(row.source || 'unknown'))
        .map((row) => mapCard(row, r._id, r.createdAt));
      eventCards.push(...cards);
      return {
        _id: r._id,
        createdAt: r.createdAt,
        total: r.results?.length || r.statistics?.total || 0,
        byResolved: r.statistics?.byResolved || {},
        enrichedDescriptions: r.statistics?.enrichedDescriptions || 0,
        openaiUsage: r.openaiUsage || null,
        results: cards,
      };
    });

    // newest first for UI
    eventCards.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));

    logger.info(`Backfill stats: runs=${runs.length} events=${totals.total}`);
    return {
      from: range.from,
      to: range.to,
      sources: sourceList,
      bySource,
      summary,
      eventCards: eventCards.slice(0, 300),
      backfillRuns,
    };
  }
}

export default BackfillStatsServices;
