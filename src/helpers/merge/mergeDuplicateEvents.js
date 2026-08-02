import crypto from 'crypto';
import {
  formatHoldingDate,
  formatHoldingDateNumeric,
  parseHoldingDate,
  mergeHoldingDates,
} from '../holdingDate';

export {
  formatHoldingDate,
  formatHoldingDateNumeric,
  parseHoldingDate,
  mergeHoldingDates,
};

export const normalize = (s) => String(s || '')
  .trim()
  .toLowerCase()
  .replace(/\s+/g, ' ');

/** Stable city key for merge (ObjectId or empty if unknown). */
export const cityKey = (cityId) => {
  if (cityId == null || cityId === '') return '';
  return String(cityId);
};

/**
 * Global identity fingerprint: name + city (no source — cross-source dedup on second).
 */
export const eventFingerprint = (name, cityId) => {
  const raw = `${normalize(name)}\n${cityKey(cityId)}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
};

export const mergeDuplicateEvents = (events, { source = '' } = {}) => {
  if (!events || events.length === 0) return [];

  // Within one parse batch still group by name+city (source is uniform in batch)
  const keyFn = (e) => `${normalize(e.name)}\n${cityKey(e.city_id)}`;

  const byKey = new Map();
  for (const e of events) {
    const k = keyFn(e);
    if (!byKey.has(k)) byKey.set(k, { events: [], dates: [], prices: [] });
    const g = byKey.get(k);
    g.events.push(e);
    const dates = e._mergeDates || (e.date_start ? [e.date_start] : []);
    g.dates.push(...dates);
    if (e.min_price != null) g.prices.push(e.min_price);
    if (e.max_price != null) g.prices.push(e.max_price);
  }

  const result = [];
  for (const g of byKey.values()) {
    const first = g.events[0];
    const toTime = (d) => (d && d.getTime ? d.getTime() : (d ? new Date(d).getTime() : null));
    const validDates = g.dates
      .map((d) => (d instanceof Date ? d : new Date(d)))
      .filter((d) => !Number.isNaN(d.getTime()));
    const dateStart = validDates.length ? new Date(Math.min(...validDates.map(toTime))) : first.date_start || null;
    const dateEnd = validDates.length ? new Date(Math.max(...validDates.map(toTime))) : first.date_end || null;
    const holdingDateStr = formatHoldingDate(validDates.length ? validDates : []);

    const longerDesc = g.events.reduce((best, cur) => (
      String(cur.description || '').length > String(best.description || '').length ? cur : best
    ), first);

    const ev = {
      ...first,
      source: source || first.source,
      description: longerDesc.description || first.description,
      date_start: dateStart,
      date_end: dateEnd,
      holding_date: holdingDateStr || first.holding_date || '',
      min_price: g.prices.length ? Math.min(...g.prices) : first.min_price,
      max_price: g.prices.length ? Math.max(...g.prices) : first.max_price,
    };
    delete ev._mergeDates;
    result.push(ev);
  }

  return result;
};

export default mergeDuplicateEvents;
