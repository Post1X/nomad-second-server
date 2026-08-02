/* eslint-disable no-console */
/**
 * Import tmp/main-to-second-export.transformed.json into second ParsedEvents.
 *
 *   SECOND_MONGO_URI=mongodb://127.0.0.1:27019/nomad_second \
 *   node scripts/importTransformedParsedEvents.js
 */
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');

const URI = process.env.SECOND_MONGO_URI || 'mongodb://127.0.0.1:27019/nomad_second';
const IN = process.env.IN
  || path.resolve(__dirname, '../tmp/main-to-second-export.transformed.json');
const BATCH = Math.max(50, Number(process.env.BATCH || 500));

async function main() {
  const payload = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const items = payload.items || [];
  console.log({ URI, IN, items: items.length });

  const client = new MongoClient(URI);
  await client.connect();
  const col = client.db().collection('parsedevents');

  const before = await col.countDocuments();
  console.log('before', before);

  // Align indexes with new schema (no fingerprint / exported_at)
  const indexes = await col.indexes();
  const names = indexes.map((i) => i.name);
  for (const drop of [
    'fingerprint_1',
    'exported_at_1',
    'source_1_exported_at_1_updatedAt_1',
  ]) {
    if (names.includes(drop)) {
      await col.dropIndex(drop);
      console.log('dropped index', drop);
    }
  }

  const afterDrop = await col.indexes();
  const afterNames = afterDrop.map((i) => i.name);
  if (!afterNames.includes('name_key_1_city_id_1')) {
    await col.createIndex(
      { name_key: 1, city_id: 1 },
      { unique: true, name: 'name_key_1_city_id_1' },
    );
    console.log('created name_key_1_city_id_1');
  }
  if (!afterNames.some((n) => n === 'source_1_updatedAt_1')) {
    await col.createIndex({ source: 1, updatedAt: 1 });
  }

  const now = new Date();
  let inserted = 0;
  let errors = [];

  for (let i = 0; i < items.length; i += BATCH) {
    const slice = items.slice(i, i + BATCH);
    const docs = slice.map((it) => {
      const eventData = { ...(it.event_data || {}) };
      delete eventData.ticketmaster_id;
      delete eventData.is_hidden;
      delete eventData.fingerprint;
      delete eventData.exported_at;
      delete eventData.coordinates;

      return {
        source: it.source || eventData.source,
        name_key: it.name_key || String(eventData.name || '').trim().toLowerCase().replace(/\s+/g, ' '),
        city_id: String(it.city_id || eventData.city_id || ''),
        parser_unique_id: it.parser_unique_id || eventData.parser_unique_id,
        event_data: {
          ...eventData,
          source: it.source || eventData.source,
          city_id: String(it.city_id || eventData.city_id || ''),
          parser_unique_id: it.parser_unique_id || eventData.parser_unique_id,
        },
        parse_run: null,
        createdAt: now,
        updatedAt: now,
      };
    });

    try {
      const r = await col.insertMany(docs, { ordered: false });
      inserted += r.insertedCount;
    } catch (e) {
      if (e.insertedCount) inserted += e.insertedCount;
      const wes = e.writeErrors || [];
      wes.slice(0, 5).forEach((we) => errors.push(we.errmsg || String(we)));
      if (!wes.length) errors.push(e.message);
    }
    console.log(`batch ${Math.floor(i / BATCH) + 1}: inserted~${inserted}`);
  }

  const after = await col.countDocuments();
  const bySource = await col.aggregate([
    { $group: { _id: '$source', n: { $sum: 1 } } },
    { $sort: { n: -1 } },
  ]).toArray();

  console.log(JSON.stringify({
    before,
    after,
    inserted,
    bySource,
    indexes: (await col.indexes()).map((x) => x.name),
    errors: errors.slice(0, 10),
    errorCount: errors.length,
  }, null, 2));

  await client.close();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
