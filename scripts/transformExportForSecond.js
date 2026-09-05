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
const moment = require('moment');

moment.locale('ru');

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
  const valid = (dates || [])
    .map((d) => (d instanceof Date ? d : new Date(d)))
    .filter((d) => !Number.isNaN(d.getTime()))
    .sort((a, b) => a.getTime() - b.getTime());
  const unique = [];
  const seen = new Set();
  for (const d of valid) {
    const key = moment(d).format('YYYY-MM-DD');
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }
  return unique;
};

/** Same as parseIsraelinfo cleanFeedDescription — strip feed meta lines. */
const cleanFeedDescription = (text = '') => String(text || '')
  .replace(/\s*Дат[аы]\s*:\s*.*$/i, ' ')
  .replace(/\s*Город[аы]?\s*:\s*.*$/i, ' ')
  .replace(/\s*Купить билеты[:\s].*$/i, ' ')
  .replace(/\s+/g, ' ')
  .trim();

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

const formatDayRanges = (dayNumbers) => {
  if (!dayNumbers?.length) return '';
  const numbers = dayNumbers.map((n) => parseInt(n, 10)).filter((n) => !Number.isNaN(n));
  if (!numbers.length) return dayNumbers.join(', ');
  const result = [];
  let start = numbers[0];
  let end = numbers[0];
  const flush = () => {
    if (end === start) result.push(String(start));
    else result.push(`${start}–${end}`);
  };
  for (let i = 1; i < numbers.length; i += 1) {
    if (numbers[i] === end + 1) end = numbers[i];
    else {
      flush();
      start = end = numbers[i];
    }
  }
  flush();
  return result.join(', ');
};

/** Same display rules as src/helpers/holdingDate.js formatHoldingDate. */
const formatHoldingDate = (dates) => {
  const uniqueDays = uniqueSortedDays(dates);
  if (!uniqueDays.length) return '';
  if (uniqueDays.length === 1) return moment(uniqueDays[0]).format('D MMMM YYYY');

  const years = [...new Set(uniqueDays.map((d) => d.getFullYear()))];
  const multiYear = years.length > 1;
  const byMonth = new Map();
  for (const d of uniqueDays) {
    const k = `${d.getFullYear()}-${d.getMonth()}`;
    if (!byMonth.has(k)) byMonth.set(k, []);
    byMonth.get(k).push(d);
  }

  const parts = [];
  for (const [, arr] of byMonth) {
    arr.sort((a, b) => a.getTime() - b.getTime());
    const m = moment(arr[0]);
    const withYear = multiYear ? ' YYYY' : '';
    const consecutive = arr.length >= 2
      && arr.every((d, i) => i === 0 || d.getDate() === arr[i - 1].getDate() + 1);
    if (consecutive) {
      parts.push(
        `${moment(arr[0]).format('D')}–${moment(arr[arr.length - 1]).format('D')} `
        + `${m.format(`MMMM${withYear}`)}`,
      );
    } else {
      parts.push(`${formatDayRanges(arr.map((d) => moment(d).format('D')))} ${m.format(`MMMM${withYear}`)}`);
    }
  }
  const result = parts.join(', ');
  if (!multiYear && years[0] != null) return `${result} ${years[0]}`;
  return result;
};

/** Same rules as src/helpers/israelinfoDates.js (kept inline for plain node). */
const parseIsraelinfoDatesFromText = (text = '') => {
  const block = String(text || '').match(/Дат[аы]\s*:\s*([^\n]+?)(?:\s+Город[аы]?|$)/i);
  const src = block ? block[1] : String(text || '');
  const dates = [];
  const toDate = (d, m, yRaw) => {
    let year = Number(yRaw);
    if (year < 100) year += 2000;
    const dt = moment(
      `${year}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`,
      'YYYY-MM-DD',
      true,
    );
    return dt.isValid() ? dt.toDate() : null;
  };
  const rangeRe = /(\d{1,2})[./](\d{1,2})[./](\d{2,4})\s*[–-]\s*(\d{1,2})[./](\d{1,2})[./](\d{2,4})/g;
  let rm;
  while ((rm = rangeRe.exec(src))) {
    const start = toDate(rm[1], rm[2], rm[3]);
    const end = toDate(rm[4], rm[5], rm[6]);
    if (!start || !end) continue;
    const from = start <= end ? start : end;
    const to = start <= end ? end : start;
    for (let t = from.getTime(); t <= to.getTime(); t += 86400000) dates.push(new Date(t));
  }
  const singleRe = /(\d{1,2})[./](\d{1,2})[./](\d{2,4})/g;
  let sm;
  while ((sm = singleRe.exec(src))) {
    const d = toDate(sm[1], sm[2], sm[3]);
    if (d) dates.push(d);
  }
  return uniqueSortedDays(dates);
};

const collectDates = (ev) => {
  const dates = [];
  if (ev.source === 'israelinfo' && ev.description) {
    dates.push(...parseIsraelinfoDatesFromText(ev.description));
  }
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
  'contacts', 'photos', 'events_category_id', 'category_resolved_by',
  'country_id', 'admin_id',
];

/** Photos: replace from winner; if winner empty keep secondary. */
const replacePhotos = (primary, secondary) => {
  const norm = (photos) => {
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
  const fromPrimary = norm(primary);
  return fromPrimary.length ? fromPrimary : norm(secondary);
};

const mergeKeepHigher = (existing, incoming) => {
  const ra = sourceRank(existing.source);
  const rb = sourceRank(incoming.source);
  if (rb < ra) return { kept: existing, discarded: true };
  // equal or higher → merge: priority fields from incoming (photos REPLACE), dates union
  const next = {
    ...existing,
    ...incoming,
    ...unionDatesPrices(existing, incoming),
    photos: replacePhotos(incoming.photos, existing.photos),
    parser_unique_id: existing.parser_unique_id || incoming.parser_unique_id || crypto.randomUUID(),
    source: incoming.source,
  };
  for (const f of PRIORITY_FIELDS) {
    if (f === 'photos') continue;
    if (incoming[f] != null && incoming[f] !== '') {
      if (f === 'specialization' && (incoming[f] === 'Event' || /^none$/i.test(String(incoming[f])))) {
        if (existing.specialization && existing.specialization !== 'Event') continue;
      }
      next[f] = incoming[f];
    }
  }
  if (incoming.lat == null && existing.lat != null) next.lat = existing.lat;
  if (incoming.lon == null && existing.lon != null) next.lon = existing.lon;
  if (next.category_resolved_by === 'none' || next.category_resolved_by === 'None'
    || next.category_resolved_by === 'default_other') {
    next.category_resolved_by = 'other';
  }
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

function loadCategoryMap() {
  const mapPath = process.env.CATEGORIES_JSON
    || path.resolve(__dirname, '../tmp/categories-id-name.json');
  if (fs.existsSync(mapPath)) {
    return JSON.parse(fs.readFileSync(mapPath, 'utf8'));
  }
  return {};
}

function main() {
  const raw = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const items = raw.items || [];
  const catMap = loadCategoryMap();
  console.log(`in items=${items.length}, categories=${Object.keys(catMap).length}`);

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

    if (ed.category_resolved_by === 'none' || ed.category_resolved_by === 'None'
      || ed.category_resolved_by === 'default_other') {
      ed.category_resolved_by = 'other';
    }

    // Parse dates from raw description first, then strip feed meta (Даты/Города/…).
    const dates = collectDates(ed);
    if (dates.length) {
      ed.date_start = dates[0];
      ed.date_end = dates[dates.length - 1];
      ed.holding_date = formatHoldingDate(dates);
    }
    if (ed.description) {
      ed.description = cleanFeedDescription(ed.description) || ed.description;
    }

    const catName = ed.events_category_id ? catMap[String(ed.events_category_id)] : null;
    if (!ed.specialization || ed.specialization === 'Event' || /^none$/i.test(String(ed.specialization))) {
      ed.specialization = (catName && !/^none$/i.test(catName)) ? catName : 'Другое';
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
      desc_has_dates_meta: /Дат[аы]\s*:/i.test(String(it.event_data.description || '')),
      puid: it.parser_unique_id,
      has_tm: Object.prototype.hasOwnProperty.call(it.event_data, 'ticketmaster_id'),
      has_fp: Object.prototype.hasOwnProperty.call(it, 'fingerprint'),
    })),
  }, null, 2));
}

main();
