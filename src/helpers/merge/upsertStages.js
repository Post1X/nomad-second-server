import { formatHoldingDate, normalize } from './mergeDuplicateEvents';

export const FULL_MATCH_FIELDS = [
  'name',
  'address',
  'date_start',
  'date_end',
  'holding_date',
  'min_price',
  'max_price',
  'description',
  'image',
  'events_category_id',
  'city_id',
  'country_id',
  'specialization',
  'website',
];

const toMs = (d) => {
  if (!d) return null;
  const t = d instanceof Date ? d.getTime() : new Date(d).getTime();
  return Number.isNaN(t) ? null : t;
};

const sameScalar = (a, b) => {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (a instanceof Date || b instanceof Date || (typeof a === 'string' && /^\d{4}-\d{2}/.test(String(a)))) {
    return toMs(a) === toMs(b);
  }
  if (typeof a === 'object' || typeof b === 'object') {
    return JSON.stringify(a) === JSON.stringify(b);
  }
  return String(a) === String(b);
};

const collectDates = (event) => {
  const dates = [];
  if (event._mergeDates) dates.push(...event._mergeDates);
  if (event.date_start) dates.push(event.date_start);
  if (event.date_end) dates.push(event.date_end);
  return dates
    .map((d) => (d instanceof Date ? d : new Date(d)))
    .filter((d) => !Number.isNaN(d.getTime()));
};

const datesEqual = (a, b) => toMs(a?.date_start) === toMs(b?.date_start)
  && toMs(a?.date_end) === toMs(b?.date_end)
  && normalize(a?.holding_date) === normalize(b?.holding_date);

const pricesEqual = (a, b) => Number(a?.min_price) === Number(b?.min_price)
  && Number(a?.max_price) === Number(b?.max_price);

export const classifyMatchStage = (existing, incoming) => {
  if (!existing) return 'insert';

  const nameOk = normalize(existing.name) === normalize(incoming.name);
  const cityOk = String(existing.city_id || '') === String(incoming.city_id || '');
  if (!nameOk || !cityOk) return 'insert';

  const allSame = FULL_MATCH_FIELDS.every((field) => {
    if (field === 'name' || field === 'address') return true;
    return sameScalar(existing[field], incoming[field]);
  });
  if (allSame) return 'skip';

  if (!datesEqual(existing, incoming) || !pricesEqual(existing, incoming)) {
    return 'merge_dates_prices';
  }

  return 'update_fields';
};

export const applyMergeToExisting = (existing, incoming, stage) => {
  if (stage === 'skip') {
    return { event: existing, changed: false };
  }

  if (stage === 'insert') {
    return { event: { ...incoming }, changed: true };
  }

  const next = { ...existing };

  if (stage === 'merge_dates_prices' || stage === 'update_fields') {
    const allDates = [...collectDates(existing), ...collectDates(incoming)];
    if (allDates.length) {
      next.date_start = new Date(Math.min(...allDates.map((d) => d.getTime())));
      next.date_end = new Date(Math.max(...allDates.map((d) => d.getTime())));
      next.holding_date = formatHoldingDate(allDates);
    }

    const prices = [existing.min_price, existing.max_price, incoming.min_price, incoming.max_price]
      .filter((p) => p != null && !Number.isNaN(Number(p)))
      .map(Number);
    if (prices.length) {
      next.min_price = Math.min(...prices);
      next.max_price = Math.max(...prices);
    }
  }

  if (stage === 'update_fields' || stage === 'merge_dates_prices') {
    for (const field of FULL_MATCH_FIELDS) {
      if (field === 'name' || field === 'address') continue;
      if (field === 'date_start' || field === 'date_end' || field === 'holding_date') continue;
      if (field === 'min_price' || field === 'max_price') continue;
      const inc = incoming[field];
      if (inc == null || inc === '') continue;
      if (!sameScalar(existing[field], inc)) {
        if (field === 'description'
          && String(existing.description || '').length > String(inc || '').length) {
          continue;
        }
        next[field] = inc;
      }
    }
    if (incoming.contacts != null) next.contacts = incoming.contacts;
    if (incoming.category_resolved_by != null) next.category_resolved_by = incoming.category_resolved_by;
    if (incoming.category_keyword_score != null) next.category_keyword_score = incoming.category_keyword_score;
    if (incoming.category_ai_failed != null) next.category_ai_failed = incoming.category_ai_failed;
    if (incoming.no_city != null) next.no_city = incoming.no_city;
  }

  return { event: next, changed: true };
};

export default {
  FULL_MATCH_FIELDS,
  classifyMatchStage,
  applyMergeToExisting,
};
