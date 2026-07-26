import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { eventFingerprint } from '../helpers/merge/mergeDuplicateEvents';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('LOOKUP_EVENTS');

/**
 * Batch lookup ParsedEvents by source + name + address (fingerprint).
 */
export async function lookupParsedEvents(items = []) {
  const list = Array.isArray(items) ? items : [];
  const results = [];

  for (const item of list) {
    const eventId = item.event_id || item.eventId || item.id || null;
    const source = item.source || '';
    const name = item.name || '';
    const address = item.address || '';

    if (!source || !name || !address) {
      results.push({
        event_id: eventId,
        found: false,
        reason: 'missing_source_name_address',
      });
      continue;
    }

    const fingerprint = eventFingerprint(source, name, address);
    // eslint-disable-next-line no-await-in-loop
    const doc = await ParsedEventsSchema.findOne({ source, fingerprint }).lean();
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
    });
  }

  logger.info(`Lookup ${list.length} → found ${results.filter((r) => r.found).length}`);
  return results;
}

export default { lookupParsedEvents };
