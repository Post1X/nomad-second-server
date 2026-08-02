import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { EVENT_SOURCE } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('STATS');

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

class StatsServices {
  static getPreviousWeekRange(now = new Date()) {
    const d = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
    ));
    const day = d.getUTCDay();
    const daysSinceMonday = (day + 6) % 7;
    const thisMonday = new Date(d);
    thisMonday.setUTCDate(d.getUTCDate() - daysSinceMonday);
    thisMonday.setUTCHours(0, 0, 0, 0);

    const prevMonday = new Date(thisMonday);
    prevMonday.setUTCDate(thisMonday.getUTCDate() - 7);

    return { from: prevMonday, to: thisMonday };
  }

  static async loadLabelMaps() {
    const [countries, cities, categories] = await Promise.all([
      CountriesSchema.find({}).select('_id name').lean(),
      CitiesSchema.find({}).select('_id name country_id').lean(),
      EventsCategoriesSchema.find({}).select('_id name').lean(),
    ]);

    return {
      countries: new Map(countries.map((c) => [String(c._id), shortName(c.name)])),
      cities: new Map(cities.map((c) => [String(c._id), shortName(c.name)])),
      categories: new Map(categories.map((c) => [String(c._id), c.name])),
    };
  }

  static async getWeeklyStats({ source, sources, from, to } = {}) {
    const range = (from && to)
      ? { from: new Date(from), to: new Date(to) }
      : this.getPreviousWeekRange();

    let sourceList;
    if (Array.isArray(sources) && sources.length) {
      sourceList = sources;
    } else if (source) {
      sourceList = String(source).split(',').map((s) => s.trim()).filter(Boolean);
    } else {
      sourceList = Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);
    }

    const labels = await this.loadLabelMaps();
    const bySource = {};
    const totals = {
      total: 0,
      noCategory: 0,
      noCategoryAfterAi: 0,
      noCity: 0,
      byCountry: {},
      byCity: {},
      byCategory: {},
      byResolvedBy: {},
    };

    for (const src of sourceList) {
      const parsed = await ParsedEventsSchema.find({
        source: src,
        updatedAt: { $gte: range.from, $lt: range.to },
      }).lean();

      const total = parsed.length;
      const byCountry = {};
      const byCity = {};
      const byCategory = {};
      const byResolvedBy = {};
      let noCategory = 0;
      let noCategoryAfterAi = 0;
      let noCity = 0;

      for (const pe of parsed) {
        const e = pe.event_data || {};
        bump(byCountry, e.country_id ? String(e.country_id) : null);
        bump(byCity, e.city_id ? String(e.city_id) : null);
        let resolvedBy = e.category_resolved_by || 'other';
        if (resolvedBy === 'none' || resolvedBy === 'None' || resolvedBy === 'default_other') {
          resolvedBy = 'other';
        }
        bump(byResolvedBy, resolvedBy);

        if (e.category_resolved_by === 'default_other' || e.category_resolved_by === 'other') {
          noCategory += 1;
          noCategoryAfterAi += 1;
          bump(byCategory, e.events_category_id ? String(e.events_category_id) : 'other');
        } else if (e.events_category_id) {
          bump(byCategory, String(e.events_category_id));
        } else {
          noCategory += 1;
          noCategoryAfterAi += 1;
        }

        if (!e.city_id || e.no_city) noCity += 1;
      }

      bySource[src] = {
        total,
        byCountry,
        byCity,
        byCategory,
        byResolvedBy,
        noCategory,
        noCategoryAfterAi,
        noCity,
        countries: toSortedList(byCountry, (id) => labels.countries.get(id) || id),
        cities: toSortedList(byCity, (id) => labels.cities.get(id) || id),
        categories: toSortedList(byCategory, (id) => labels.categories.get(id) || id),
        resolvedBy: toSortedList(byResolvedBy, (id) => id),
      };

      totals.total += total;
      totals.noCategory += noCategory;
      totals.noCategoryAfterAi += noCategoryAfterAi;
      totals.noCity += noCity;
      Object.entries(byCountry).forEach(([k, v]) => { totals.byCountry[k] = (totals.byCountry[k] || 0) + v; });
      Object.entries(byCity).forEach(([k, v]) => { totals.byCity[k] = (totals.byCity[k] || 0) + v; });
      Object.entries(byCategory).forEach(([k, v]) => { totals.byCategory[k] = (totals.byCategory[k] || 0) + v; });
      Object.entries(byResolvedBy).forEach(([k, v]) => { totals.byResolvedBy[k] = (totals.byResolvedBy[k] || 0) + v; });
    }

    const summary = {
      ...totals,
      countries: toSortedList(totals.byCountry, (id) => labels.countries.get(id) || id),
      cities: toSortedList(totals.byCity, (id) => labels.cities.get(id) || id),
      categories: toSortedList(totals.byCategory, (id) => labels.categories.get(id) || id),
      resolvedBy: toSortedList(totals.byResolvedBy, (id) => id),
    };

    logger.info(`Weekly stats built for ${Object.keys(bySource).length} sources`);
    return {
      from: range.from,
      to: range.to,
      sources: sourceList,
      bySource,
      summary,
    };
  }
}

export default StatsServices;
