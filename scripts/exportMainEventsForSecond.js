/* eslint-disable no-console */
/**
 * Read-only: pull active non-nomad events from main, map to ParsedEvents shape,
 * merge by fingerprint, write JSON. No DB writes.
 *
 * Env:
 *   MAIN_MONGO_URI  default mongodb://127.0.0.1:27018/nomad
 *   OUT             output path (default ./tmp/main-to-second-export.json)
 *   LIMIT=N
 *   BATCH=200
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { MongoClient, ObjectId } = require('mongodb');

const MAIN_URI = process.env.MAIN_MONGO_URI || 'mongodb://127.0.0.1:27018/nomad';
const OUT = process.env.OUT
  || path.resolve(__dirname, '../tmp/main-to-second-export.json');
const LIMIT = process.env.LIMIT ? Number(process.env.LIMIT) : null;
const BATCH = Math.max(50, Number(process.env.BATCH || 200));

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

const cityKey = (cityId) => {
  if (cityId == null || cityId === '') return '';
  return String(cityId);
};

const eventFingerprint = (name, cityId) => {
  const raw = `${normalize(name)}\n${cityKey(cityId)}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
};

const sourceRank = (s) => SOURCE_PRIORITY[s] ?? 0;

const toNumCoord = (v) => {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

const collectDates = (ev) => {
  const out = [];
  if (ev.date_start) out.push(new Date(ev.date_start));
  if (ev.date_end) out.push(new Date(ev.date_end));
  return out.filter((d) => !Number.isNaN(d.getTime()));
};

const mergeTwo = (a, b) => {
  const winnerIsB = sourceRank(b.source) > sourceRank(a.source)
    || (sourceRank(b.source) === sourceRank(a.source)
      && new Date(b.updatedAt || 0) > new Date(a.updatedAt || 0));
  const primary = winnerIsB ? { ...b } : { ...a };
  const secondary = winnerIsB ? a : b;

  const dates = [...collectDates(a), ...collectDates(b)];
  if (dates.length) {
    primary.date_start = new Date(Math.min(...dates.map((d) => d.getTime())));
    primary.date_end = new Date(Math.max(...dates.map((d) => d.getTime())));
  }
  if (String(secondary.holding_date || '').length > String(primary.holding_date || '').length) {
    primary.holding_date = secondary.holding_date;
  }
  if (String(secondary.description || '').length > String(primary.description || '').length) {
    primary.description = secondary.description;
  }
  const prices = [a.min_price, a.max_price, b.min_price, b.max_price]
    .filter((p) => p != null && !Number.isNaN(Number(p)))
    .map(Number);
  if (prices.length) {
    primary.min_price = Math.min(...prices);
    primary.max_price = Math.max(...prices);
  }
  if (!primary.photos?.length && secondary.photos?.length) primary.photos = secondary.photos;
  if (!primary.contacts?.website && secondary.contacts?.website) {
    primary.contacts = { ...primary.contacts, ...secondary.contacts };
  }
  primary.ticketmaster_id = primary.ticketmaster_id || secondary.ticketmaster_id || null;
  primary._mainIds = [...(a._mainIds || [a._mainId]), ...(b._mainIds || [b._mainId])].filter(Boolean);
  primary._winnerMainId = primary._mainId;
  return primary;
};

async function loadMappedBatch(mainDb, events) {
  const contactIds = [...new Set(events.map((e) => String(e.contacts)).filter(Boolean))];
  const photoIds = [...new Set(events.flatMap((e) => (e.carousel_photos || []).map(String)))];

  const contacts = contactIds.length
    ? await mainDb.collection('contacts').find({
      _id: { $in: contactIds.map((id) => new ObjectId(id)) },
    }).toArray()
    : [];
  const photos = photoIds.length
    ? await mainDb.collection('photos').find({
      _id: { $in: photoIds.map((id) => new ObjectId(id)) },
    }).toArray()
    : [];

  const contactMap = Object.fromEntries(contacts.map((c) => [String(c._id), c]));
  const photoMap = Object.fromEntries(photos.map((p) => [String(p._id), p]));

  return events.map((ev) => {
    const c = contactMap[String(ev.contacts)] || {};
    const contactsObj = {};
    ['website', 'phone_number', 'instagram', 'telegram', 'whatsapp', 'viber', 'facebook']
      .forEach((k) => {
        if (c[k]) contactsObj[k] = c[k];
      });

    const photoList = (ev.carousel_photos || [])
      .map((id) => photoMap[String(id)])
      .filter(Boolean)
      .map((p) => ({ full_url: p.full_url || p.url }))
      .filter((p) => p.full_url);

    const lat = toNumCoord(ev.coordinates?.lat);
    const lon = toNumCoord(ev.coordinates?.lon);

    const mapped = {
      _mainId: String(ev._id),
      _mainIds: [String(ev._id)],
      _winnerMainId: String(ev._id),
      updatedAt: ev.updatedAt,
      name: ev.name,
      description: ev.description || '',
      specialization: ev.specialization || 'Event',
      holding_date: ev.holding_date || '',
      date_start: ev.date_start || null,
      date_end: ev.date_end || null,
      address: ev.address || '',
      source: ev.source,
      country_id: ev.country ? String(ev.country) : null,
      city_id: ev.city ? String(ev.city) : null,
      events_category_id: ev.events_category ? String(ev.events_category) : null,
      category_resolved_by: ev.category_resolved_by || undefined,
      contacts: contactsObj,
      photos: photoList,
      min_price: typeof ev.min_price === 'number' ? ev.min_price : null,
      max_price: typeof ev.max_price === 'number' ? ev.max_price : null,
      ticketmaster_id: ev.ticketmaster_id || null,
      admin_id: ev.creator_admin ? String(ev.creator_admin) : null,
    };
    if (lat != null) mapped.lat = lat;
    if (lon != null) mapped.lon = lon;
    if (ev.coordinates?.is_special_point_on_map != null) {
      mapped.is_special_point_on_map = Boolean(ev.coordinates.is_special_point_on_map);
    }
    return mapped;
  });
}

async function main() {
  console.log({ MAIN_URI, OUT, LIMIT, BATCH });
  fs.mkdirSync(path.dirname(OUT), { recursive: true });

  const mainClient = new MongoClient(MAIN_URI);
  await mainClient.connect();
  const mainDb = mainClient.db();

  const filter = {
    source: { $ne: 'nomad' },
    is_active: true,
    is_hidden: false,
  };

  const totalMain = await mainDb.collection('events').countDocuments(filter);
  console.log(`main matching=${totalMain}`);

  const cursor = mainDb.collection('events').find(filter).sort({ _id: 1 }).batchSize(BATCH);
  if (LIMIT) cursor.limit(LIMIT);

  const byFp = new Map();
  let scanned = 0;
  let skippedNoCity = 0;

  while (await cursor.hasNext()) {
    const chunk = [];
    while (chunk.length < BATCH && await cursor.hasNext()) {
      chunk.push(await cursor.next());
    }
    scanned += chunk.length;
    const mapped = await loadMappedBatch(mainDb, chunk);
    for (const m of mapped) {
      if (!m.city_id || !m.name) {
        skippedNoCity += 1;
        continue;
      }
      const fp = eventFingerprint(m.name, m.city_id);
      if (!byFp.has(fp)) byFp.set(fp, m);
      else byFp.set(fp, mergeTwo(byFp.get(fp), m));
    }
    process.stdout.write(`\rscanned=${scanned} unique_fp=${byFp.size}`);
  }
  console.log('');

  const exportedAt = new Date().toISOString();
  const items = [];

  for (const [fp, ev] of byFp.entries()) {
    const puid = crypto.randomUUID();
    const eventData = { ...ev, parser_unique_id: puid };
    const meta = {
      main_winner_id: ev._winnerMainId,
      main_ids: ev._mainIds || [ev._mainId],
      main_updatedAt: ev.updatedAt || null,
    };
    delete eventData._mainId;
    delete eventData._mainIds;
    delete eventData._winnerMainId;
    delete eventData.updatedAt;

    items.push({
      source: ev.source,
      fingerprint: fp,
      parser_unique_id: puid,
      exported_at: exportedAt,
      event_data: eventData,
      _meta: meta,
    });
  }

  const bySource = {};
  for (const it of items) {
    bySource[it.source] = (bySource[it.source] || 0) + 1;
  }

  const payload = {
    meta: {
      created_at: exportedAt,
      filter,
      main_scanned: scanned,
      skipped_no_city: skippedNoCity,
      unique_fingerprints: items.length,
      merged_away: scanned - skippedNoCity - items.length,
      by_source: bySource,
      note: 'Ready for ParsedEvents insert. _meta is for linking back to main; strip before insert if needed.',
    },
    items,
  };

  fs.writeFileSync(OUT, JSON.stringify(payload));
  const st = fs.statSync(OUT);
  console.log(JSON.stringify({
    out: OUT,
    bytes: st.size,
    mb: Math.round(st.size / 1024 / 1024 * 10) / 10,
    scanned,
    unique: items.length,
    skippedNoCity,
    bySource,
    sample: items.slice(0, 2).map((it) => ({
      name: it.event_data.name,
      source: it.source,
      fp: it.fingerprint.slice(0, 12),
      puid: it.parser_unique_id,
      main_ids: it._meta.main_ids.length,
      website: it.event_data.contacts?.website,
      photos: it.event_data.photos?.length,
    })),
  }, null, 2));

  await mainClient.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
