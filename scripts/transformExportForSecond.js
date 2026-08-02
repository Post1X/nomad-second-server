/* eslint-disable no-console */
/**
 * Transform tmp/main-to-second-export.json according to product rules (2026-08-02).
 * Read-only on DBs. Writes a new JSON ready for ParsedEvents insert.
 *
 *   node scripts/transformExportForSecond.js
 *   IN=... OUT=... node scripts/transformExportForSecond.js
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const IN = process.env.IN
  || path.resolve(__dirname, '../tmp/main-to-second-export.json');
const OUT = process.env.OUT
  || path.resolve(__dirname, '../tmp/main-to-second-export.transformed.json');

const SOURCE_PRIORITY = {
  eventim: 100,
  ticketmaster: 100,
  israelinfo: 90,
  kontramarka: 50,
  fienta: 50,
  nomad: 10,
};

const normalize = (s) => String(s || '')
  .trim()
  .toLowerCase()
  .replace(/\s+/g, ' ');

const nameKey = (name) => normalize(name);
const cityKey = (id) => (id == null || id === '' ? '' : String(id));
const sourceRank = (s) => SOURCE_PRIORITY[s] ?? 0;

const MONTHS_RU = {
  января: 0, февраля: 1, марта: 2, апреля: 3, мая: 4, июня: 5,
  июля: 6, августа: 7, сентября: 8, октября: 9, ноября: 10, декабря: 11,
};

const uniqueSortedDays = (dates) => {
  const map = new Map();
  for (const d of dates) {
    const x = d instanceof Date ? d : new Date(d);
    if (Number.isNaN(x.getTime())) continue;
    const key = `${x.getUTCFullYear()}-${x.getUTCMonth()}-${x.getUTCDate()}`;
    if (!map.has(key)) map.set(key, new Date(Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate())));
  }
  return [...map.values()].sort((a, b) => a - b);
};

const parseHoldingDate = (str = '') => {
  const out = [];
  const s = String(str || '').trim();
  if (!s) return out;

  // DD.MM.YYYY / ranges
  const numeric = s.match(/\d{1,2}\.\d{1,2}\.\d{4}/g) || [];
  for (const bit of numeric) {
    const [dd, mm, yyyy] = bit.split('.').map(Number);
    out.push(new Date(Date.UTC(yyyy, mm - 1, dd)));
  }
  if (out.length) return uniqueSortedDays(out);

  // rough RU month parse
  const lower = s.toLowerCase();
  for (const [monthName, month] of Object.entries(MONTHS_RU)) {
    if (!lower.includes(monthName)) continue;
    const yearMatch = lower.match(/20\d{2}/);
    const year = yearMatch ? Number(yearMatch[0]) : new Date().getUTCFullYear();
    const beforeMonth = lower.split(monthName)[0] || '';
    const range = beforeMonth.match(/(\d{1,2})\s*[–-]\s*(\d{1,2})/);
    if (range) {
      for (let d = Number(range[1]); d <= Number(range[2]); d += 1) {
        out.push(new Date(Date.UTC(year, month, d)));
      }
    } else {
      for (const bit of beforeMonth.match(/\d{1,2}/g) || []) {
        out.push(new Date(Date.UTC(year, month, Number(bit))));
      }
    }
  }
  return uniqueSortedDays(out);
};

const formatHoldingDate = (dates) => {
  const days = uniqueSortedDays(dates);
  if (!days.length) return '';
  // simple: list DD.MM.YYYY, collapse consecutive with –
  const parts = [];
  let runStart = days[0];
  let runEnd = days[0];
  const fmt = (d) => {
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    return `${dd}.${mm}.${d.getUTCFullYear()}`;
  };
  const flush = () => {
    if (runStart.getTime() === runEnd.getTime()) parts.push(fmt(runStart));
    else parts.push(`${fmt(runStart)}–${fmt(runEnd)}`); // consecutive → dash
  };
  for (let i = 1; i < days.length; i += 1) {
    const prev = runEnd;
    const cur = days[i];
    const nextDay = new Date(prev);
    nextDay.setUTCDate(nextDay.getUTCDate() + 1);
    if (cur.getTime() === nextDay.getTime()) {
      runEnd = cur;
    } else {
      flush();
      runStart = cur;
      runEnd = cur;
    }
  }
  flush();
  return parts.join(', ');
};

const collectDates = (ev) => {
  const dates = [];
  if (ev.date_start) dates.push(ev.date_start);
  if (ev.date_end) dates.push(ev.date_end);
  if (ev.holding_date) dates.push(...parseHoldingDate(ev.holding_date));
  return uniqueSortedDays(dates);
};

const unionDatesPrices = (a, b) => {
  const dates = [...collectDates(a), ...collectDates(b)];
  const prices = [a.min_price, a.max_price, b.min_price, b.max_price]
    .filter((p) => p != null && !Number.isNaN(Number(p)))
    .map(Number);
  return {
    date_start: dates.length ? dates[0] : (a.date_start || b.date_start || null),
    date_end: dates.length ? dates[dates.length - 1] : (a.date_end || b.date_end || null),
    holding_date: dates.length ? formatHoldingDate(dates) : (a.holding_date || b.holding_date || ''),
    min_price: prices.length ? Math.min(...prices) : (a.min_price ?? b.min_price ?? null),
    max_price: prices.length ? Math.max(...prices) : (a.max_price ?? b.max_price ?? null),
  };
};

const PRIORITY_FIELDS = [
  'source', 'specialization', 'description', 'address',
  'lat', 'lon', 'is_special_point_on_map',
  'contacts', 'events_category_id', 'category_resolved_by',
  'country_id', 'admin_id',
];

const mergePhotos = (a, b) => {
  const list = [...(Array.isArray(a) ? a : []), ...(Array.isArray(b) ? b : [])];
  const seen = new Set();
  const out = [];
  for (const p of list) {
    const url = typeof p === 'string' ? p : (p?.full_url || p?.url || '');
    if (!url || seen.has(url)) continue;
    seen.add(url);
    out.push({ full_url: url });
  }
  return out;
};

const mergeKeepHigher = (existing, incoming) => {
  const ra = sourceRank(existing.source);
  const rb = sourceRank(incoming.source);
  if (rb < ra) return { kept: existing, discarded: true };
  // equal or higher → merge: priority fields from incoming, photos+dates union
  const next = {
    ...existing,
    ...incoming,
    ...unionDatesPrices(existing, incoming),
    photos: mergePhotos(existing.photos, incoming.photos),
    parser_unique_id: existing.parser_unique_id || incoming.parser_unique_id || crypto.randomUUID(),
    source: incoming.source,
  };
  for (const f of PRIORITY_FIELDS) {
    if (incoming[f] != null && incoming[f] !== '') next[f] = incoming[f];
  }
  if (incoming.lat == null && existing.lat != null) next.lat = existing.lat;
  if (incoming.lon == null && existing.lon != null) next.lon = existing.lon;
  delete next.ticketmaster_id;
  delete next.fingerprint;
  delete next.exported_at;
  delete next.coordinates;
  return { kept: next, discarded: false };
};

const startOfTodayUtc = () => {
  const d = new Date();
  d.setUTCHours(0, 0, 0, 0);
  return d;
};

const isPast = (ev) => {
  const end = ev.date_end || ev.date_start;
  if (!end) return false;
  const t = new Date(end).getTime();
  return !Number.isNaN(t) && t < startOfTodayUtc().getTime();
};

const strip = (ev) => {
  const next = { ...ev };
  delete next.ticketmaster_id;
  delete next.is_hidden;
  delete next.fingerprint;
  delete next.name_for_search;
  delete next.user;
  delete next.is_active;
  delete next.date_of_deactivating;
  delete next.carousel_photos;
  delete next.country;
  delete next.city;
  delete next.events_category;
  return next;
};

function main() {
  const raw = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const items = raw.items || [];
  console.log(`in items=${items.length}`);

  let skippedPast = 0;
  let skippedNoCity = 0;
  const byKey = new Map();

  for (const item of items) {
    const ed = strip({ ...(item.event_data || {}), source: item.source || item.event_data?.source });
    if (!ed.city_id || !ed.name) {
      skippedNoCity += 1;
      continue;
    }
    if (isPast(ed)) {
      skippedPast += 1;
      continue;
    }

    // normalize coords from nested if present
    if (ed.coordinates && ed.lat == null) {
      const lat = Number(ed.coordinates.lat);
      const lon = Number(ed.coordinates.lon);
      if (Number.isFinite(lat)) ed.lat = lat;
      if (Number.isFinite(lon)) ed.lon = lon;
      if (ed.coordinates.is_special_point_on_map != null) {
        ed.is_special_point_on_map = Boolean(ed.coordinates.is_special_point_on_map);
      }
    }
    delete ed.coordinates;

    const dates = collectDates(ed);
    if (dates.length) {
      ed.date_start = dates[0];
      ed.date_end = dates[dates.length - 1];
      ed.holding_date = formatHoldingDate(dates);
    }

    const puid = item.parser_unique_id || ed.parser_unique_id || crypto.randomUUID();
    ed.parser_unique_id = puid;

    const key = `${nameKey(ed.name)}\n${cityKey(ed.city_id)}`;
    const row = {
      source: ed.source,
      name_key: nameKey(ed.name),
      city_id: cityKey(ed.city_id),
      parser_unique_id: puid,
      event_data: ed,
      _meta: item._meta || null,
    };

    if (!byKey.has(key)) {
      byKey.set(key, row);
    } else {
      const prev = byKey.get(key);
      const { kept, discarded } = mergeKeepHigher(prev.event_data, ed);
      if (!discarded) {
        byKey.set(key, {
          source: kept.source,
          name_key: prev.name_key,
          city_id: prev.city_id,
          parser_unique_id: prev.parser_unique_id || kept.parser_unique_id,
          event_data: { ...kept, parser_unique_id: prev.parser_unique_id || kept.parser_unique_id },
          _meta: {
            main_winner_id: prev._meta?.main_winner_id,
            main_ids: [...new Set([
              ...(prev._meta?.main_ids || []),
              ...(item._meta?.main_ids || []),
            ])],
          },
        });
      }
    }
  }

  const outItems = [...byKey.values()].map((row) => ({
    source: row.source,
    name_key: row.name_key,
    city_id: row.city_id,
    parser_unique_id: row.parser_unique_id,
    event_data: strip({
      ...row.event_data,
      source: row.source,
      parser_unique_id: row.parser_unique_id,
      city_id: row.city_id,
    }),
    _meta: row._meta,
  }));

  const bySource = {};
  for (const it of outItems) {
    bySource[it.source] = (bySource[it.source] || 0) + 1;
  }

  const payload = {
    meta: {
      created_at: new Date().toISOString(),
      source_file: IN,
      rules: '2026-08-02 product ruleset',
      input_items: items.length,
      skipped_past: skippedPast,
      skipped_no_city: skippedNoCity,
      output_items: outItems.length,
      by_source: bySource,
      note: 'No fingerprint/exported_at. Insert as ParsedEvents { source, name_key, city_id, parser_unique_id, event_data }. Strip _meta before insert.',
    },
    items: outItems,
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(payload));
  const st = fs.statSync(OUT);
  console.log(JSON.stringify({
    out: OUT,
    mb: Math.round(st.size / 1024 / 1024 * 10) / 10,
    skippedPast,
    skippedNoCity,
    output: outItems.length,
    bySource,
    sample: outItems.slice(0, 2).map((it) => ({
      name: it.event_data.name,
      source: it.source,
      name_key: it.name_key,
      holding_date: it.event_data.holding_date,
      puid: it.parser_unique_id,
      has_tm: Object.prototype.hasOwnProperty.call(it.event_data, 'ticketmaster_id'),
      has_fp: Object.prototype.hasOwnProperty.call(it, 'fingerprint'),
    })),
  }, null, 2));
}

main();
