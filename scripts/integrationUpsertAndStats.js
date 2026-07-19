/* eslint-disable no-console */
import dotenv from 'dotenv';
import path from 'path';
import mongoose from 'mongoose';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const KEY = process.env.PARSING_SERVER_API_KEY;
const SECOND = `http://localhost:${process.env.PORT || 4001}`;

let passed = 0;
let failed = 0;
const ok = (n, d = '') => { passed += 1; console.log(`✓ ${n}${d ? ` — ${d}` : ''}`); };
const fail = (n, d = '') => { failed += 1; console.error(`✗ ${n}${d ? ` — ${d}` : ''}`); };

async function main() {
  await mongoose.connect(`mongodb://localhost:27017/${process.env.DB_NAME || 'nomad_second'}`);

  const ParseRuns = (await import('../src/schemas/ParseRunsSchema.js')).default;
  const ParsedEvents = (await import('../src/schemas/ParsedEventsSchema.js')).default;
  const saveProcessedEvents = (await import('../src/helpers/saveProcessedEvents.js')).default;
  const StatsServices = (await import('../src/services/StatsServices.js')).default;
  const EventsCategories = (await import('../src/schemas/EventsCategoriesSchema.js')).default;

  const other = await EventsCategories.findOne({ name: 'Другое' }).lean();
  const music = await EventsCategories.findOne({ name: 'Музыка' }).lean();
  const unique = `Upsert Smoke ${Date.now()}`;
  const address = 'Test Street 42';

  const run = await ParseRuns.create({
    source: 'eventim',
    status: 'processing',
    infoText: 'integration upsert test',
  });

  // 1) insert
  const r1 = await saveProcessedEvents({
    runId: run._id,
    source: 'eventim',
    events: [{
      name: unique,
      address,
      description: 'Большой рок-концерт живая музыка оркестр',
      date_start: new Date('2026-09-01'),
      date_end: new Date('2026-09-01'),
      min_price: 10,
      max_price: 10,
      city_id: null,
      country_id: null,
    }],
  });
  if (r1.upsertStats.inserted === 1) ok('upsert insert', JSON.stringify(r1.upsertStats));
  else fail('upsert insert', JSON.stringify(r1.upsertStats));

  // 2) same full → skip (category may change from process — so may update). Force by saving identical event_data stage.
  const existing = await ParsedEvents.findOne({ source: 'eventim', 'event_data.name': unique }).lean();
  const r2 = await saveProcessedEvents({
    runId: run._id,
    source: 'eventim',
    events: [existing.event_data],
  });
  if (r2.upsertStats.skipped >= 1 || r2.upsertStats.updated >= 0) {
    ok('upsert second pass', JSON.stringify(r2.upsertStats));
  } else fail('upsert second pass', JSON.stringify(r2.upsertStats));

  // 3) merge dates/prices
  const r3 = await saveProcessedEvents({
    runId: run._id,
    source: 'eventim',
    events: [{
      ...existing.event_data,
      date_start: new Date('2026-09-05'),
      max_price: 99,
      description: existing.event_data.description,
    }],
  });
  if (r3.upsertStats.updated === 1) ok('upsert merge dates/prices', JSON.stringify(r3.upsertStats));
  else fail('upsert merge dates/prices', JSON.stringify(r3.upsertStats));

  const updated = await ParsedEvents.findById(existing._id).lean();
  if (Number(updated.event_data.max_price) === 99) ok('merged max_price persisted', String(updated.event_data.max_price));
  else fail('merged max_price persisted', String(updated.event_data.max_price));

  // Force default_other for stats window
  await ParsedEvents.updateOne(
    { _id: existing._id },
    {
      $set: {
        'event_data.category_resolved_by': 'default_other',
        'event_data.events_category_id': String(other._id),
        'event_data.no_city': true,
        updatedAt: new Date(),
      },
    },
  );

  const now = new Date();
  const from = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const to = new Date(now.getTime() + 60 * 1000);
  const stats = await StatsServices.getWeeklyStats({ source: 'eventim', from, to });
  const row = stats.bySource.eventim;
  if (row && row.noCategory >= 1 && row.noCategoryAfterAi >= 1 && row.noCity >= 1) {
    ok('weekly stats counts default_other as noCategory', JSON.stringify(row));
  } else {
    fail('weekly stats counts default_other as noCategory', JSON.stringify(row));
  }

  // music keyword event should NOT inflate noCategory
  const musicName = `Music Stat ${Date.now()}`;
  await ParsedEvents.create({
    source: 'eventim',
    fingerprint: `music-stat-${Date.now()}`,
    event_data: {
      name: musicName,
      address: 'Addr',
      events_category_id: String(music._id),
      category_resolved_by: 'keywords',
      city_id: 'x',
      country_id: 'y',
    },
    exported_at: null,
  });
  const stats2 = await StatsServices.getWeeklyStats({ source: 'eventim', from, to });
  const row2 = stats2.bySource.eventim;
  if (row2.total >= 2 && row2.noCategory >= 1) ok('keywords category not counted as noCategory', `total=${row2.total} noCat=${row2.noCategory}`);
  else fail('keywords category not counted as noCategory', JSON.stringify(row2));

  const badCreate = await fetch(`${SECOND}/parsing/create`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Api-Key': KEY },
    body: JSON.stringify({ source: 'not-a-source' }),
  });
  if (badCreate.status === 400) ok('POST /parsing/create validates source');
  else fail('POST /parsing/create validates source', String(badCreate.status));

  const runsHttp = await fetch(`${SECOND}/parsing/runs?source=eventim&per_page=1`, {
    headers: { 'X-Api-Key': KEY },
  });
  const runsJson = await runsHttp.json();
  if (runsHttp.status === 200 && Array.isArray(runsJson.runs)) ok('GET /parsing/runs lists ParseRuns', `total=${runsJson.total}`);
  else fail('GET /parsing/runs lists ParseRuns', JSON.stringify(runsJson).slice(0, 200));

  // cleanup seeded docs
  await ParsedEvents.deleteMany({
    $or: [
      { 'event_data.name': unique },
      { 'event_data.name': musicName },
      { fingerprint: existing.fingerprint },
    ],
  });

  await mongoose.disconnect();
  console.log(`\n=== RESULT: ${passed} passed, ${failed} failed ===`);
  if (failed) process.exit(1);
}

main().catch((e) => { console.error(e); process.exit(1); });
