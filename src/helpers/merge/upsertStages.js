import {
  normalize,
  parseHoldingDate,
  mergeHoldingDates,
} from './mergeDuplicateEvents';

/**
 * Fields from higher-priority source (equal priority → newer / incoming).
 * photos: replace from winner (not union).
 */
export const PRIORITY_FIELDS = [
  'source',
  'specialization',
  'description',
  'address',
  'lat',
  'lon',
  'is_special_point_on_map',
  'coordinates',
  'contacts',
  'photos',
  'events_category_id',
  'category_resolved_by',
  'country_id',
  'admin_id',
];

export const FULL_MATCH_FIELDS = [
  'name',
  'address',
  'date_start',
  'date_end',
  'holding_date',
  'min_price',
  'max_price',
  'description',
  'events_category_id',
  'city_id',
  'country_id',
  'specialization',
  'photos',
  'contacts',
  'lat',
  'lon',
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

/** Collect dates from _mergeDates, start/end, and parse holding_date → array. */
export const collectDates = (event) => {
  const dates = [];
  if (event?._mergeDates) dates.push(...event._mergeDates);
  if (event?.date_start) dates.push(event.date_start);
  if (event?.date_end) dates.push(event.date_end);
  if (event?.holding_date) dates.push(...parseHoldingDate(event.holding_date));
  return dates
    .map((d) => (d instanceof Date ? d : new Date(d)))
    .filter((d) => !Number.isNaN(d.getTime()));
};

/** Normalize photos to [{ full_url }]. */
export const normalizePhotos = (photos) => {
  if (!Array.isArray(photos)) return [];
  const out = [];
  const seen = new Set();
  for (const p of photos) {
    const url = typeof p === 'string' ? p : (p?.full_url || p?.url || '');
    if (!url || seen.has(url)) continue;
    seen.add(url);
    out.push({ full_url: url });
  }
  return out;
};

/**
 * Photos: replace from primary (winner). If primary has none, keep secondary.
 * @deprecated name kept for imports — behavior is replace, not union.
 */
export const mergePhotos = (primary, secondary) => {
  const fromPrimary = normalizePhotos(primary);
  if (fromPrimary.length) return fromPrimary;
  return normalizePhotos(secondary);
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

  const allSame = FULL_MATCH_FIELDS.every((field) => sameScalar(existing[field], incoming[field]));
  if (allSame) return 'skip';

  if (!datesEqual(existing, incoming) || !pricesEqual(existing, incoming)) {
    return 'merge_dates_prices';
  }

  return 'update_fields';
};

/**
 * Union holding_date / date_start / date_end / min_price / max_price.
 * holding_date: parse both strings → array → add → format again (dash for consecutive).
 */
export const unionDatesAndPrices = (existing = {}, incoming = {}) => {
  const merged = mergeHoldingDates(
    existing.holding_date,
    incoming.holding_date,
    collectDates(existing),
    collectDates(incoming),
  );

  const prices = [existing.min_price, existing.max_price, incoming.min_price, incoming.max_price]
    .filter((p) => p != null && !Number.isNaN(Number(p)))
    .map(Number);

  return {
    date_start: merged.date_start
      || existing.date_start
      || incoming.date_start
      || null,
    date_end: merged.date_end
      || existing.date_end
      || incoming.date_end
      || null,
    holding_date: merged.holding_date
      || existing.holding_date
      || incoming.holding_date
      || '',
    min_price: prices.length ? Math.min(...prices) : (existing.min_price ?? incoming.min_price ?? null),
    max_price: prices.length ? Math.max(...prices) : (existing.max_price ?? incoming.max_price ?? null),
  };
};

/**
 * Higher/equal priority merge:
 * - priority fields (incl. address, lat/lon, photos) from primary — photos replaced
 * - dates/prices always unioned via holding_date parse↔format
 */
export const applyPriorityMerge = (existing, incoming, { primaryIsIncoming }) => {
  const primary = primaryIsIncoming ? incoming : existing;
  const secondary = primaryIsIncoming ? existing : incoming;

  const next = {
    ...secondary,
    ...primary,
    city_id: existing.city_id || incoming.city_id,
    name: existing.name || incoming.name,
    ...unionDatesAndPrices(existing, incoming),
    photos: mergePhotos(primary.photos, secondary.photos),
  };

  for (const field of PRIORITY_FIELDS) {
    if (field === 'photos') continue; // already set via mergePhotos (replace)
    const v = primary[field];
    if (v == null || v === '') continue;
    // Don't overwrite a real specialization with placeholder "Event"
    if (field === 'specialization' && (v === 'Event' || /^none$/i.test(String(v)))) {
      if (secondary.specialization && secondary.specialization !== 'Event') {
        next.specialization = secondary.specialization;
        continue;
      }
    }
    next[field] = v;
  }

  // coords: prefer primary; if primary missing, keep secondary
  if (primary.lat == null && secondary.lat != null) next.lat = secondary.lat;
  if (primary.lon == null && secondary.lon != null) next.lon = secondary.lon;
  if (primary.is_special_point_on_map == null && secondary.is_special_point_on_map != null) {
    next.is_special_point_on_map = secondary.is_special_point_on_map;
  }

  if (!next.specialization || next.specialization === 'Event' || /^none$/i.test(String(next.specialization))) {
    next.specialization = secondary.specialization && secondary.specialization !== 'Event'
      ? secondary.specialization
      : next.specialization;
  }
  if (/^none$/i.test(String(next.specialization || ''))) {
    next.specialization = 'Другое';
  }
  if (next.category_resolved_by === 'none' || next.category_resolved_by === 'None') {
    next.category_resolved_by = 'other';
  }

  next.parser_unique_id = existing.parser_unique_id || incoming.parser_unique_id || null;

  delete next.ticketmaster_id;
  delete next.is_hidden;
  delete next.fingerprint;
  delete next.exported_at;
  delete next._mergeDates;
  delete next.coordinates; // flat lat/lon only on second

  return { event: next, changed: true };
};

export const applyMergeToExisting = (existing, incoming, stage) => {
  if (stage === 'skip') {
    return { event: existing, changed: false };
  }

  if (stage === 'insert') {
    const event = { ...incoming };
    delete event.ticketmaster_id;
    delete event.is_hidden;
    delete event.fingerprint;
    delete event.exported_at;
    delete event.coordinates;
    return { event, changed: true };
  }

  // Equal priority / same source → merge (incoming = newer for priority fields)
  return applyPriorityMerge(existing || {}, incoming, { primaryIsIncoming: true });
};

export default {
  FULL_MATCH_FIELDS,
  PRIORITY_FIELDS,
  classifyMatchStage,
  applyMergeToExisting,
  applyPriorityMerge,
  unionDatesAndPrices,
  collectDates,
  mergePhotos,
};
