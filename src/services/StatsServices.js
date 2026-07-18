import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE, OPERATION_TYPES } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('STATS');

const SOURCE_TO_OPERATION = {
  [EVENT_SOURCE.kontramarka]: OPERATION_TYPES.parsingEventsFromKontramarka,
  [EVENT_SOURCE.fienta]: OPERATION_TYPES.parsingEventsFromFienta,
  [EVENT_SOURCE.eventim]: OPERATION_TYPES.parsingEventsFromEventim,
  [EVENT_SOURCE.ticketmaster]: OPERATION_TYPES.parsingEventsFromTicketmaster,
  [EVENT_SOURCE.israelinfo]: OPERATION_TYPES.parsingEventsFromIsraelinfo,
};

const OPERATION_TO_SOURCE = Object.fromEntries(
  Object.entries(SOURCE_TO_OPERATION).map(([source, type]) => [type, source]),
);

const bump = (obj, key) => {
  const k = key || 'unknown';
  obj[k] = (obj[k] || 0) + 1;
};

class StatsServices {
  static getPreviousWeekRange(now = new Date()) {
        const d = new Date(Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
    ));
    const day = d.getUTCDay();     const daysSinceMonday = (day + 6) % 7;
    const thisMonday = new Date(d);
    thisMonday.setUTCDate(d.getUTCDate() - daysSinceMonday);
    thisMonday.setUTCHours(0, 0, 0, 0);

    const prevMonday = new Date(thisMonday);
    prevMonday.setUTCDate(thisMonday.getUTCDate() - 7);

    return { from: prevMonday, to: thisMonday };
  }

  static async getWeeklyStats({ source, from, to } = {}) {
    const range = (from && to)
      ? { from: new Date(from), to: new Date(to) }
      : this.getPreviousWeekRange();

    const sources = source
      ? [source]
      : Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);

    const bySource = {};

    for (const src of sources) {
      const opType = SOURCE_TO_OPERATION[src];
      if (!opType) continue;

      const operations = await OperationsSchema.find({
        type: opType,
        createdAt: { $gte: range.from, $lt: range.to },
      }).select('_id').lean();

      const opIds = operations.map((o) => o._id);
      const parsed = opIds.length
        ? await ParsedEventsSchema.find({ operation: { $in: opIds } }).lean()
        : [];

      const total = parsed.length;
      const byCountry = {};
      const byCity = {};
      const byCategory = {};
      let noCategory = 0;
      let noCategoryAfterAi = 0;
      let noCity = 0;

      for (const pe of parsed) {
        const e = pe.event_data || {};
        bump(byCountry, e.country_id ? String(e.country_id) : null);
        bump(byCity, e.city_id ? String(e.city_id) : null);
        if (e.events_category_id) {
          bump(byCategory, String(e.events_category_id));
        } else {
          noCategory += 1;
          if (e.category_ai_failed || e.category_resolved_by == null) {
            noCategoryAfterAi += 1;
          }
        }
        if (!e.city_id || e.no_city) noCity += 1;
      }

      bySource[src] = {
        total,
        byCountry,
        byCity,
        byCategory,
        noCategory,
        noCategoryAfterAi,
        noCity,
      };
    }

    logger.info(`Weekly stats built for ${Object.keys(bySource).length} sources`);
    return {
      from: range.from,
      to: range.to,
      bySource,
    };
  }
}

export { SOURCE_TO_OPERATION, OPERATION_TO_SOURCE };
export default StatsServices;
