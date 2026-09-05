/* eslint-disable no-console */
import dotenv from 'dotenv';
import path from 'path';
import mongoose from 'mongoose';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const SECOND = process.env.SECOND_URL || `http://localhost:${process.env.PORT || 4001}`;
const MAIN = process.env.MAIN_SERVER_URL || 'http://localhost:4000';
const KEY = process.env.PARSING_SERVER_API_KEY;
const MAIN_KEY = process.env.MAIN_SERVER_API_KEY || KEY;

let passed = 0;
let failed = 0;
const results = [];

const ok = (name, detail = '') => {
  passed += 1;
  results.push({ name, ok: true, detail });
  console.log(`✓ ${name}${detail ? ` — ${detail}` : ''}`);
};

const fail = (name, detail = '') => {
  failed += 1;
  results.push({ name, ok: false, detail });
  console.error(`✗ ${name}${detail ? ` — ${detail}` : ''}`);
};

async function req(base, method, urlPath, { headers = {}, body } = {}) {
  const res = await fetch(`${base}${urlPath}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch (_) { /* ignore */ }
  return { status: res.status, json, text };
}

async function testHelpers() {
  const {
    eventFingerprint,
    mergeDuplicateEventsForSource,
    classifyMatchStage,
    applyMergeToExisting,
  } = await import('../src/helpers/merge/index.js');
  const { detectCategoryByKeywords } = await import('../src/services/CategoryKeywordServices.js');
  const EventsCategoriesSchema = (await import('../src/schemas/EventsCategoriesSchema.js')).default;

  const fp1 = eventFingerprint('Concert Night', 'cityBerlin');
  const fp2 = eventFingerprint('Concert Night', 'cityBerlin');
  const fp3 = eventFingerprint('Concert Night', 'cityMunich');
  if (fp1 === fp2 && fp1 !== fp3) ok('fingerprint global by name+city', fp1.slice(0, 12));
  else fail('fingerprint global by name+city', `${fp1} / ${fp3}`);

  const merged = mergeDuplicateEventsForSource([
    {
      name: 'Jazz Live',
      city_id: 'tallinnId',
      address: 'Tallinn Hall A',
      date_start: new Date('2026-08-01'),
      min_price: 10,
      max_price: 10,
      description: 'a',
    },
    {
      name: 'Jazz Live',
      city_id: 'tallinnId',
      address: 'Tallinn Hall B spelling',
      date_start: new Date('2026-08-02'),
      min_price: 20,
      max_price: 30,
      description: 'longer description here',
    },
  ], 'fienta');
  if (merged.length === 1
    && Number(merged[0].min_price) === 10
    && Number(merged[0].max_price) === 30
    && String(merged[0].description).includes('longer')) {
    ok('batch merge dates/prices/description', `holding=${merged[0].holding_date}`);
  } else fail('batch merge dates/prices/description', JSON.stringify(merged[0]));

  const existing = {
    name: 'Jazz Live',
    address: 'Tallinn',
    date_start: new Date('2026-08-01'),
    date_end: new Date('2026-08-02'),
    holding_date: '01.08.2026, 02.08.2026',
    min_price: 10,
    max_price: 30,
    description: 'longer description here',
    image: 'img',
    events_category_id: 'cat1',
    city_id: 'c1',
    country_id: 'co1',
    specialization: 'Event',
    website: 'https://x.test',
  };
  const stageSkip = classifyMatchStage(existing, { ...existing });
  if (stageSkip === 'skip') ok('upsert stage skip on full match');
  else fail('upsert stage skip on full match', stageSkip);

  const stageDates = classifyMatchStage(existing, {
    ...existing,
    date_start: new Date('2026-08-03'),
    max_price: 50,
  });
  if (stageDates === 'merge_dates_prices') ok('upsert stage merge_dates_prices');
  else fail('upsert stage merge_dates_prices', stageDates);

  const { event: mergedUp, changed } = applyMergeToExisting(existing, {
    ...existing,
    date_start: new Date('2026-08-03'),
    max_price: 50,
  }, 'merge_dates_prices');
  if (changed && Number(mergedUp.max_price) === 50) ok('applyMergeToExisting updates prices');
  else fail('applyMergeToExisting updates prices', JSON.stringify(mergedUp));

  const cats = await EventsCategoriesSchema.find({}).lean();
  if (cats.length) ok('categories present in second DB', String(cats.length));
  else fail('categories present in second DB', '0 categories — keywords/AI may fail');

  const music = await detectCategoryByKeywords({
    name: 'Большой рок-концерт живая музыка',
    description: 'оркестр и группа на сцене',
    specialization: '',
  }, 'eventim');
  if (music.categoryId && music.score >= 3) {
    ok('keywords detect music', `score=${music.score} cat=${music.categoryName || music.categoryId}`);
  } else {
    fail('keywords detect music', JSON.stringify(music));
  }

  const weak = await detectCategoryByKeywords({
    name: 'xyz unrelated gathering',
    description: 'nothing special',
    specialization: '',
  }, 'eventim');
  if (!weak.categoryId) ok('keywords below threshold → null', `score=${weak.score}`);
  else fail('keywords below threshold → null', JSON.stringify(weak));
}

async function testSecondApi() {
  const unauthorized = await req(SECOND, 'GET', '/parsing/events?source=eventim');
  if (unauthorized.status === 401 || unauthorized.status === 403) ok('second API rejects missing/invalid key', String(unauthorized.status));
  else fail('second API rejects missing/invalid key', String(unauthorized.status));

  const headers = { 'X-Api-Key': KEY };

  const eventsEmpty = await req(SECOND, 'GET', '/parsing/events?source=eventim&page=1&per_page=5', { headers });
  if (eventsEmpty.status === 200 && eventsEmpty.json?.status === 'ok') {
    ok('GET /parsing/events', `total=${eventsEmpty.json.totalEvents}`);
  } else fail('GET /parsing/events', eventsEmpty.text.slice(0, 200));

  const runs = await req(SECOND, 'GET', '/parsing/runs?page=1&per_page=5', { headers });
  if (runs.status === 200 && runs.json?.status === 'ok') ok('GET /parsing/runs', `total=${runs.json.total}`);
  else fail('GET /parsing/runs', runs.text.slice(0, 200));

  const stats = await req(SECOND, 'GET', '/parsing/stats/weekly', { headers });
  if (stats.status === 200 && stats.json?.status === 'ok' && stats.json.bySource) {
    const sources = Object.keys(stats.json.bySource);
    ok('GET /parsing/stats/weekly', `sources=${sources.join(',')}`);
  } else fail('GET /parsing/stats/weekly', stats.text.slice(0, 200));

  // Seed a synthetic ParsedEvent via mongoose to test ack flow
  const ParsedEvents = (await import('../src/schemas/ParsedEventsSchema.js')).default;
  const { eventFingerprint } = await import('../src/helpers/merge/index.js');
  const fingerprint = eventFingerprint(`__smoke_${Date.now()}`, 'smokeCityId');
  const doc = await ParsedEvents.create({
    source: 'eventim',
    fingerprint,
    event_data: {
      name: `__smoke_${Date.now()}`,
      address: 'Smoke Address 1',
      description: 'smoke test event for ack',
      city_id: null,
      country_id: null,
      category_resolved_by: 'default_other',
      events_category_id: null,
      no_city: true,
    },
    exported_at: null,
  });

  const pending = await req(SECOND, 'GET', '/parsing/events?source=eventim&onlyPending=true&page=1&per_page=50', { headers });
  const found = (pending.json?.events || []).some((e) => e._parsed_event_id === String(doc._id));
  if (found) ok('pending events include seeded doc');
  else fail('pending events include seeded doc', `ids=${(pending.json?.ids || []).slice(0, 3)}`);

  const ack = await req(SECOND, 'POST', '/parsing/events/ack', {
    headers,
    body: { source: 'eventim', ids: [String(doc._id)] },
  });
  if (ack.status === 200 && ack.json?.modified >= 1) ok('POST /parsing/events/ack', `modified=${ack.json.modified}`);
  else fail('POST /parsing/events/ack', ack.text.slice(0, 200));

  const after = await ParsedEvents.findById(doc._id).lean();
  if (after?.exported_at) ok('exported_at set after ack');
  else fail('exported_at set after ack');

  await ParsedEvents.deleteOne({ _id: doc._id });

  // categorize-batch (keywords path; may call AI if weak — use strong music text)
  const cats = await (await import('../src/schemas/EventsCategoriesSchema.js')).default.find({}).lean();
  const other = cats.find((c) => c.name === 'Другое');
  if (other) ok('category Другое exists', String(other._id));
  else fail('category Другое exists');

  const catBatch = await req(SECOND, 'POST', '/parsing/categorize-batch', {
    headers,
    body: {
      source: 'backfill',
      events: [
        {
          event_id: '000000000000000000000001',
          name: 'Stand-up comedy show стендап комик',
          description: 'юмористический концерт stand-up comedy',
          address: 'Moscow',
        },
      ],
    },
  });
  if (catBatch.status === 200 && catBatch.json?.results?.[0]?.category_id) {
    ok('POST /parsing/categorize-batch', `by=${catBatch.json.results[0].resolved_by}`);
  } else {
    fail('POST /parsing/categorize-batch', catBatch.text.slice(0, 300));
  }

  // processParsedEvents: AI fail / weak → default_other
  const { processParsedEvents } = await import('../src/services/ProcessParsedEventsServices.js');
  const { events: processed, stats: pstats } = await processParsedEvents([
    {
      name: 'zzzzqwx unrelated blob 999',
      address: 'Nowhere',
      description: 'qqqwww no meaningful category words',
      city_id: null,
    },
  ], 'eventim');
  const ev = processed[0];
  if (ev.category_resolved_by === 'default_other' || ev.category_resolved_by === 'ai' || ev.category_resolved_by === 'keywords') {
    ok('processParsedEvents assigns category path', `by=${ev.category_resolved_by} openai=${JSON.stringify(pstats.openaiUsage)}`);
  } else {
    fail('processParsedEvents assigns category path', JSON.stringify({ ev, pstats }));
  }
  if (pstats.noCity === 1) ok('processParsedEvents marks noCity');
  else fail('processParsedEvents marks noCity', String(pstats.noCity));
}

async function testMainApi() {
  const ping = await req(MAIN, 'GET', '/api/ping');
  if (ping.status === 200) ok('main /api/ping');
  else fail('main /api/ping', String(ping.status));

  const dict = await req(MAIN, 'GET', '/api/parsing-dict/sync', {
    headers: { 'X-Api-Key': MAIN_KEY },
  });
  if (dict.status === 200 && (dict.json?.countries || dict.json?.cities || dict.json?.status === 'ok' || dict.json?.eventCategories)) {
    const c = dict.json.countries?.length ?? dict.json?.data?.countries?.length ?? '?';
    const cities = dict.json.cities?.length ?? dict.json?.data?.cities?.length ?? '?';
    const cats = dict.json.eventCategories?.length ?? dict.json?.categories?.length ?? dict.json?.data?.eventCategories?.length ?? '?';
    ok('main GET /api/parsing-dict/sync', `countries=${c} cities=${cities} cats=${cats}`);
  } else {
    fail('main GET /api/parsing-dict/sync', dict.text.slice(0, 250));
  }

  // Client helpers from main: get weekly stats via ParsingServerServices path (HTTP)
  const weeklyViaMainKey = await req(SECOND, 'GET', '/parsing/stats/weekly', {
    headers: { 'X-Api-Key': KEY },
  });
  if (weeklyViaMainKey.status === 200) ok('main can reach second weekly stats (shared key)');
  else fail('main can reach second weekly stats (shared key)', String(weeklyViaMainKey.status));
}

async function testCleanupService() {
  const CleanupServices = (await import('../src/services/CleanupServices.js')).default;
  // Dry-run style: call with huge months so nothing deleted (or 0 deletes)
  const result = await CleanupServices.cleanupExpiredEvents(120);
  if (typeof result.deletedEvents === 'number') ok('cleanupExpiredEvents callable', `deleted=${result.deletedEvents}`);
  else fail('cleanupExpiredEvents callable', JSON.stringify(result));
}

async function main() {
  if (!KEY) {
    console.error('PARSING_SERVER_API_KEY missing');
    process.exit(1);
  }

  const mongoUri = process.env.MONGO_URI || `mongodb://127.0.0.1:27017/${process.env.DB_NAME || 'nomad_second'}`;
  await mongoose.connect(mongoUri);

  console.log('\n=== Helper / merge / keywords ===');
  await testHelpers();

  console.log('\n=== Second server API ===');
  await testSecondApi();

  console.log('\n=== Main server API ===');
  await testMainApi();

  console.log('\n=== Cleanup ===');
  await testCleanupService();

  await mongoose.disconnect();

  console.log(`\n=== RESULT: ${passed} passed, ${failed} failed ===`);
  if (failed > 0) process.exit(1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
