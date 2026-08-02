import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { eventFingerprint } from '../helpers/merge/mergeDuplicateEvents';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('LOOKUP_EVENTS');

/**
 * Batch lookup ParsedEvents by name + city_id (global fingerprint).
 */
export async function lookupParsedEvents(items = []) {
  const list = Array.isArray(items) ? items : [];
  const results = [];

  for (const item of list) {
    const eventId = item.event_id || item.eventId || item.id || null;
    const name = item.name || '';
    const cityId = item.city_id || item.cityId || '';

    if (!name || !cityId) {
      results.push({
        event_id: eventId,
        found: false,
        reason: 'missing_name_city',
      });
      continue;
    }

    const fingerprint = eventFingerprint(name, cityId);
    // eslint-disable-next-line no-await-in-loop
    const doc = await ParsedEventsSchema.findOne({ fingerprint }).lean();
    if (!doc?.event_data) {
      results.push({
        event_id: eventId,
        found: false,
        fingerprint,
      });
      continue;
    }

    const e = doc.event_data;
    results.push({
      event_id: eventId,
      found: true,
      fingerprint,
      source: doc.source || e.source || null,
      description: e.description || '',
      holding_date: e.holding_date || '',
      date_start: e.date_start || null,
      date_end: e.date_end || null,
      min_price: e.min_price ?? null,
      max_price: e.max_price ?? null,
      specialization: e.specialization || '',
      city_id: e.city_id ? String(e.city_id) : null,
      country_id: e.country_id ? String(e.country_id) : null,
      events_category_id: e.events_category_id ? String(e.events_category_id) : null,
      category_resolved_by: e.category_resolved_by || null,
      website: e.contacts?.website || '',
      ticketmaster_id: e.ticketmaster_id || null,
      parser_unique_id: doc.parser_unique_id || e.parser_unique_id || null,
    });
  }

  logger.info(`Lookup ${list.length} → found ${results.filter((r) => r.found).length}`);
  return results;
}

export default { lookupParsedEvents };
