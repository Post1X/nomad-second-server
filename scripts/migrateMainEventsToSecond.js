/* eslint-disable no-console */
/**
 * Migrate active non-nomad events from main → second ParsedEvents.
 *
 * Env:
 *   MAIN_MONGO_URI   default mongodb://127.0.0.1:27018/nomad
 *   SECOND_MONGO_URI default mongodb://127.0.0.1:27019/nomad_second
 *   DRY_RUN=1        no writes
 *   LIMIT=N          process only first N main events (after filter)
 *   BATCH=200
 *   WRITE_PUID_TO_MAIN=1  write parser_unique_id back to winner main event (default 1)
 */
const crypto = require('crypto');
const { MongoClient, ObjectId } = require('mongodb');

const MAIN_URI = process.env.MAIN_MONGO_URI || 'mongodb://127.0.0.1:27018/nomad';
const SECOND_URI = process.env.SECOND_MONGO_URI || 'mongodb://127.0.0.1:27019/nomad_second';
const DRY_RUN = process.env.DRY_RUN === '1' || process.env.DRY_RUN === 'true';
const LIMIT = process.env.LIMIT ? Number(process.env.LIMIT) : null;
const BATCH = Math.max(50, Number(process.env.BATCH || 200));
const WRITE_PUID = process.env.WRITE_PUID_TO_MAIN !== '0';

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
  // keep richer holding_date
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
  console.log({
    DRY_RUN, LIMIT, BATCH, WRITE_PUID, MAIN_URI, SECOND_URI,
  });

  const mainClient = new MongoClient(MAIN_URI);
  const secondClient = new MongoClient(SECOND_URI);
  await mainClient.connect();
  await secondClient.connect();
  const mainDb = mainClient.db();
  const secondDb = secondClient.db();

  const filter = {
    source: { $ne: 'nomad' },
    is_active: true,
    is_hidden: false,
  };

  const totalMain = await mainDb.collection('events').countDocuments(filter);
  const secondBefore = await secondDb.collection('parsedevents').countDocuments();
  console.log(`main matching=${totalMain}, second parsedevents=${secondBefore}`);

  // city/country presence on second
  const sampleCities = await mainDb.collection('events').aggregate([
    { $match: filter },
    { $group: { _id: '$city' } },
    { $limit: 5000 },
  ]).toArray();
  const cityIds = sampleCities.map((c) => c._id).filter(Boolean);
  const secondCities = cityIds.length
    ? await secondDb.collection('cities').countDocuments({ _id: { $in: cityIds } })
    : 0;
  console.log(`cities on main(sample groups)=${cityIds.length}, present on second=${secondCities}`);

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

  const rows = [...byFp.entries()];
  console.log(`unique fingerprints=${rows.length}, skippedNoCity=${skippedNoCity}, mergedAway=${scanned - skippedNoCity - rows.length}`);

  let inserted = 0;
  let skippedExisting = 0;
  let puidWritten = 0;
  let puidSkipped = 0;
  const errors = [];
  const exportedAt = new Date();

  for (let i = 0; i < rows.length; i += BATCH) {
    const slice = rows.slice(i, i + BATCH);
    const fps = slice.map(([fp]) => fp);
    const existing = await secondDb.collection('parsedevents').find(
      { fingerprint: { $in: fps } },
      { projection: { fingerprint: 1, parser_unique_id: 1 } },
    ).toArray();
    const existingMap = Object.fromEntries(existing.map((e) => [e.fingerprint, e]));

    const toInsert = [];
    const mainPuidOps = [];

    for (const [fp, ev] of slice) {
      if (existingMap[fp]) {
        skippedExisting += 1;
        continue;
      }
      const puid = crypto.randomUUID();
      const eventData = { ...ev, parser_unique_id: puid };
      delete eventData._mainId;
      delete eventData._mainIds;
      delete eventData._winnerMainId;
      delete eventData.updatedAt;

      toInsert.push({
        source: ev.source,
        fingerprint: fp,
        parser_unique_id: puid,
        event_data: eventData,
        parse_run: null,
        exported_at: exportedAt,
        createdAt: new Date(),
        updatedAt: new Date(),
      });

      if (WRITE_PUID && ev._winnerMainId) {
        mainPuidOps.push({
          mainId: ev._winnerMainId,
          puid,
          allIds: ev._mainIds || [ev._winnerMainId],
        });
      }
    }

    if (!DRY_RUN && toInsert.length) {
      try {
        await secondDb.collection('parsedevents').insertMany(toInsert, { ordered: false });
        inserted += toInsert.length;
      } catch (e) {
        if (e?.writeErrors) {
          inserted += toInsert.length - e.writeErrors.length;
          e.writeErrors.forEach((we) => errors.push(we.errmsg || String(we)));
        } else {
          errors.push(e.message);
        }
      }
    } else if (DRY_RUN) {
      inserted += toInsert.length;
    }

    if (!DRY_RUN && WRITE_PUID && mainPuidOps.length) {
      for (const op of mainPuidOps) {
        try {
          // only set if missing — avoid unique conflicts
          const r = await mainDb.collection('events').updateOne(
            {
              _id: new ObjectId(op.mainId),
              $or: [
                { parser_unique_id: { $exists: false } },
                { parser_unique_id: null },
                { parser_unique_id: '' },
              ],
            },
            { $set: { parser_unique_id: op.puid } },
          );
          if (r.modifiedCount) puidWritten += 1;
          else puidSkipped += 1;
        } catch (e) {
          puidSkipped += 1;
          errors.push(`main puid ${op.mainId}: ${e.message}`);
        }
      }
    } else if (DRY_RUN && WRITE_PUID) {
      puidWritten += mainPuidOps.length;
    }

    console.log(`batch ${i / BATCH + 1}: insert+=${toInsert.length} totalInserted~${inserted}`);
  }

  const secondAfter = DRY_RUN
    ? secondBefore
    : await secondDb.collection('parsedevents').countDocuments();

  console.log(JSON.stringify({
    DRY_RUN,
    scanned,
    uniqueFingerprints: rows.length,
    skippedNoCity,
    skippedExisting,
    inserted,
    puidWritten,
    puidSkipped,
    secondBefore,
    secondAfter,
    errors: errors.slice(0, 20),
    errorCount: errors.length,
    sample: rows.slice(0, 2).map(([fp, ev]) => ({
      fp: fp.slice(0, 12),
      name: ev.name,
      source: ev.source,
      city_id: ev.city_id,
      mainIds: ev._mainIds?.length,
      website: ev.contacts?.website,
      photos: ev.photos?.length,
    })),
  }, null, 2));

  await mainClient.close();
  await secondClient.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
