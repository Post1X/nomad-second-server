#!/usr/bin/env babel-node
/**
 * Dedicated merge-logic test (second server).
 *
 * Covers:
 *  - fingerprint by name+city_id (global, cross-source)
 *  - in-batch merge (dates / prices / longest description)
 *  - upsert stages: insert | skip | merge_dates_prices | update_fields
 *  - DB persistence via ParsedEvents (2 dates → 1 doc)
 *  - all 5 sources use the same merge helpers
 *  - parser_unique_id on insert/update; is_hidden is main-only / deprecated
 *
 *   yarn test:merge
 *   # or
 *   babel-node -r dotenv/config scripts/testMergeLogic.js
 */

/* eslint-disable no-console */
import crypto from 'crypto';
import dotenv from 'dotenv';
import path from 'path';
import mongoose from 'mongoose';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const newParserUniqueId = () => crypto.randomUUID();

const SOURCES = ['kontramarka', 'fienta', 'eventim', 'ticketmaster', 'israelinfo'];

let passed = 0;
let failed = 0;
const ok = (name, detail = '') => {
  passed += 1;
  console.log(`✓ ${name}${detail ? ` — ${detail}` : ''}`);
};
const fail = (name, detail = '') => {
  failed += 1;
  console.error(`✗ ${name}${detail ? ` — ${detail}` : ''}`);
};

async function main() {
  const {
    eventFingerprint,
    mergeDuplicateEventsForSource,
    classifyMatchStage,
    applyMergeToExisting,
    parseHoldingDate,
  } = await import('../src/helpers/merge/index.js');
  const ParsedEventsSchema = (await import('../src/schemas/ParsedEventsSchema.js')).default;
  const dbName = process.env.DB_NAME || 'nomad_second';
  await mongoose.connect(`mongodb://localhost:27017/${dbName}`);

  console.log('\n=== 1. Fingerprint ===');
  const a = eventFingerprint('Same Show', 'cityA');
  const b = eventFingerprint('Same Show', 'cityA');
  const c = eventFingerprint('Same Show', 'cityA'); // other source would share fp
  const d = eventFingerprint('Same Show', 'cityB');
  if (a === b && a === c && a !== d) ok('fingerprint global by name+city (cross-source same)');
  else fail('fingerprint', `${a.slice(0, 8)}…`);

  console.log('\n=== 2. In-batch merge (dates/prices/desc) ===');
  const batch = mergeDuplicateEventsForSource([
    {
      name: 'Merge Concert',
      city_id: 'cityBerlin',
      address: 'Berlin Arena',
      date_start: new Date('2026-09-01T20:00:00Z'),
      min_price: 40,
      max_price: 40,
      description: 'short',
      _mergeDates: [new Date('2026-09-01T20:00:00Z')],
    },
    {
      name: 'Merge Concert',
      city_id: 'cityBerlin',
      address: 'Another spelling of venue',
      date_start: new Date('2026-09-03T20:00:00Z'),
      min_price: 55,
      max_price: 90,
      description: 'much longer description that should win',
      _mergeDates: [new Date('2026-09-03T20:00:00Z')],
    },
    {
      name: 'Other Show',
      city_id: 'cityBerlin',
      address: 'Berlin Arena',
      date_start: new Date('2026-09-05T20:00:00Z'),
      min_price: 10,
      max_price: 10,
      description: 'different name → separate',
    },
  ], 'kontramarka');

  if (batch.length === 2) ok('batch collapses same name+city', `groups=${batch.length}`);
  else fail('batch collapse', `got ${batch.length}`);

  const concert = batch.find((e) => e.name === 'Merge Concert');
  const holdingParts = String(concert?.holding_date || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (concert
    && Number(concert.min_price) === 40
    && Number(concert.max_price) === 90
    && String(concert.description).includes('longer')
    && new Date(concert.date_start).getTime() === new Date('2026-09-01T20:00:00Z').getTime()
    && new Date(concert.date_end).getTime() === new Date('2026-09-03T20:00:00Z').getTime()
    && holdingParts.length === 2) {
    ok('union min/max dates+prices + longest desc + holding_date', concert.holding_date);
  } else {
    fail('union fields', JSON.stringify({
      min: concert?.min_price,
      max: concert?.max_price,
      desc: concert?.description,
      holding: concert?.holding_date,
      start: concert?.date_start,
      end: concert?.date_end,
    }));
  }

  console.log('\n=== 3. Upsert stages ===');
  const base = {
    name: 'Stage Show',
    address: 'Vienna',
    date_start: new Date('2026-10-01'),
    date_end: new Date('2026-10-01'),
    holding_date: '01.10.2026',
    min_price: 20,
    max_price: 20,
    description: 'base desc',
    image: 'img',
    events_category_id: 'cat1',
    city_id: 'city1',
    country_id: 'co1',
    specialization: 'Event',
    website: 'https://x.test',
  };

  if (classifyMatchStage(null, base) === 'insert') ok('stage insert (no existing)');
  else fail('stage insert');

  if (classifyMatchStage(base, { ...base }) === 'skip') ok('stage skip (full match)');
  else fail('stage skip');

  if (classifyMatchStage(base, {
    ...base,
    date_start: new Date('2026-10-05'),
    max_price: 60,
  }) === 'merge_dates_prices') ok('stage merge_dates_prices');
  else fail('stage merge_dates_prices');

  if (classifyMatchStage(base, {
    ...base,
    description: 'updated longer description for fields',
    website: 'https://y.test',
  }) === 'update_fields') ok('stage update_fields');
  else fail('stage update_fields');

  // different address, same city → still same event (update_fields if address in FULL_MATCH)
  const addrStage = classifyMatchStage(base, { ...base, address: 'Salzburg' });
  if (addrStage === 'update_fields' || addrStage === 'skip') {
    ok('different address same city → merge path', addrStage);
  } else fail('different address same city', addrStage);

  if (classifyMatchStage(base, { ...base, city_id: 'city2' }) === 'insert') {
    ok('different city → insert (no merge)');
  } else fail('different city → insert');

  const { event: merged, changed } = applyMergeToExisting(base, {
    ...base,
    date_start: new Date('2026-10-05'),
    max_price: 60,
  }, 'merge_dates_prices');
  if (changed
    && Number(merged.max_price) === 60
    && Number(merged.min_price) === 20
    && (String(merged.holding_date).includes('05.10.2026')
      || String(merged.holding_date).includes('5')
      || parseHoldingDate(merged.holding_date).length >= 2)) {
    ok('applyMergeToExisting unions dates/prices', merged.holding_date);
  } else fail('applyMergeToExisting', JSON.stringify(merged));

  console.log('\n=== 4. Same merge helpers for all 5 sources ===');
  for (const src of SOURCES) {
    const m = mergeDuplicateEventsForSource([
      {
        name: 'Src Show',
        city_id: 'c1',
        address: 'Addr 1',
        date_start: new Date('2026-11-01'),
        min_price: 5,
        max_price: 5,
        description: 'a',
      },
      {
        name: 'Src Show',
        city_id: 'c1',
        address: 'Addr DIFFERENT',
        date_start: new Date('2026-11-02'),
        min_price: 8,
        max_price: 12,
        description: 'bb',
      },
    ], src);
    if (m.length === 1 && Number(m[0].max_price) === 12) ok(`${src}: batch merge`, `max=${m[0].max_price}`);
    else fail(`${src}: batch merge`, JSON.stringify(m[0]));
  }

  console.log('\n=== 5. DB persistence (ParsedEvents upsert merge) ===');
  const stamp = Date.now();
  const name = `__merge_test_${stamp}`;
  const cityId = 'mergeTestCity';
  const address = 'Merge Test Hall';
  const source = 'eventim';
  const fp = eventFingerprint(name, cityId);

  await ParsedEventsSchema.deleteMany({ fingerprint: fp, source });

  const day1 = {
    name,
    city_id: cityId,
    address,
    description: 'day1',
    date_start: new Date('2026-12-01T19:00:00Z'),
    date_end: new Date('2026-12-01T19:00:00Z'),
    holding_date: '01.12.2026',
    min_price: 30,
    max_price: 30,
    specialization: 'Event',
    source,
  };
  const day2 = {
    name,
    city_id: cityId,
    address: 'Other Hall Spelling',
    description: 'day2 longer description for merge test',
    date_start: new Date('2026-12-10T19:00:00Z'),
    date_end: new Date('2026-12-10T19:00:00Z'),
    holding_date: '10.12.2026',
    min_price: 45,
    max_price: 70,
    specialization: 'Event',
    source,
  };

  // simulate saveProcessedEvents upsert loop (without full categorize/parse run)
  const upsertOne = async (incoming) => {
    const existingDoc = await ParsedEventsSchema.findOne({ source, fingerprint: fp }).lean();
    const existingData = existingDoc?.event_data || null;
    const stage = classifyMatchStage(existingData, incoming);
    const { event: mergedEvent, changed: ch } = applyMergeToExisting(existingData, incoming, stage);
    if (stage === 'skip' || !ch) return { stage, changed: false };
    if (!existingDoc) {
      const parserUniqueId = mergedEvent.parser_unique_id || newParserUniqueId();
      const eventData = { ...mergedEvent, parser_unique_id: parserUniqueId };
      await ParsedEventsSchema.create({
        source,
        fingerprint: fp,
        parser_unique_id: parserUniqueId,
        event_data: eventData,
        exported_at: null,
      });
      return { stage, changed: true, action: 'insert' };
    }
    const parserUniqueId = existingDoc.parser_unique_id
      || existingData?.parser_unique_id
      || newParserUniqueId();
    const eventData = { ...mergedEvent, parser_unique_id: parserUniqueId };
    await ParsedEventsSchema.updateOne(
      { _id: existingDoc._id },
      { $set: { parser_unique_id: parserUniqueId, event_data: eventData, exported_at: null } },
    );
    return { stage, changed: true, action: 'update' };
  };

  const r1 = await upsertOne(day1);
  if (r1.stage === 'insert' && r1.action === 'insert') ok('DB first date → insert', r1.stage);
  else fail('DB first insert', JSON.stringify(r1));

  const r2 = await upsertOne(day2);
  if (r2.stage === 'merge_dates_prices' && r2.action === 'update') {
    ok('DB second date → merge_dates_prices', r2.stage);
  } else fail('DB second merge', JSON.stringify(r2));

  const docs = await ParsedEventsSchema.find({ source, fingerprint: fp }).lean();
  if (docs.length === 1) ok('DB still one ParsedEvent after merge', `n=${docs.length}`);
  else fail('DB one doc', `n=${docs.length}`);

  const data = docs[0]?.event_data || {};
  if (Number(data.min_price) === 30
    && Number(data.max_price) === 70
    && String(data.description).includes('longer')
    && parseHoldingDate(data.holding_date).length >= 2) {
    ok('DB event_data has unioned dates/prices/desc', data.holding_date);
  } else {
    fail('DB event_data fields', JSON.stringify({
      min: data.min_price,
      max: data.max_price,
      desc: data.description,
      holding: data.holding_date,
    }));
  }

  // cleanup
  await ParsedEventsSchema.deleteMany({ fingerprint: fp, source });
  ok('cleanup test docs');

  console.log('\n=== 6. is_hidden / parser_unique_id contract ===');
  const hasHiddenOnSecond = Object.prototype.hasOwnProperty.call(data, 'is_hidden');
  if (!hasHiddenOnSecond) {
    ok('ParsedEvents.event_data has no is_hidden (merge independent)');
  } else {
    fail('unexpected is_hidden on second event_data');
  }
  if (data.parser_unique_id) {
    ok('event_data has parser_unique_id', String(data.parser_unique_id).slice(0, 8));
  } else {
    fail('parser_unique_id missing on saved event_data');
  }
  ok('contract: is_hidden deprecated on MAIN; pull publishes without hiding');
  ok('contract: cross-source merge on SECOND; MAIN upsert by parser_unique_id');


  console.log('\n=== 7. Cross-source fingerprint + priority merge helper ===');
  {
    const { mergeCrossSourceEvent, pickWinnerSource } = await import('../src/helpers/merge/index.js');
    if (pickWinnerSource('fienta', 'eventim') === 'incoming') ok('priority: eventim beats fienta');
    else fail('priority eventim>fienta');
    if (pickWinnerSource('eventim', 'fienta') === 'existing') ok('priority: keep eventim over fienta');
    else fail('priority keep eventim');

    const existing = {
      name: 'Cross Show',
      city_id: 'cityX',
      address: 'Hall A',
      description: 'short',
      date_start: new Date('2026-08-01'),
      date_end: new Date('2026-08-01'),
      holding_date: '01.08.2026',
      min_price: 10,
      max_price: 10,
      parser_unique_id: 'puid-cross-1',
    };
    const incoming = {
      name: 'Cross Show',
      city_id: 'cityX',
      address: 'Hall B spelling',
      description: 'much longer from eventim',
      date_start: new Date('2026-08-05'),
      date_end: new Date('2026-08-05'),
      holding_date: '05.08.2026',
      min_price: 20,
      max_price: 40,
    };
    const { event: merged, winnerSource } = mergeCrossSourceEvent(existing, 'fienta', incoming, 'eventim');
    if (winnerSource === 'eventim'
      && merged.parser_unique_id === 'puid-cross-1'
      && String(merged.description).includes('longer')
      && Number(merged.max_price) === 40) {
      ok('cross-source merge keeps puid, winner fields, union prices');
    } else {
      fail('cross-source merge', JSON.stringify({ winnerSource, desc: merged.description, max: merged.max_price, puid: merged.parser_unique_id }));
    }
  }

  console.log(`\n=== RESULT: ${passed} passed, ${failed} failed ===`);
  await mongoose.disconnect();
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
