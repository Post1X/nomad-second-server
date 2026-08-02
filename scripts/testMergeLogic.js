#!/usr/bin/env babel-node
/**
 * Merge-logic tests (second server) — product rules 2026-08-02.
 *
 *   yarn test:merge
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
    nameKey,
    cityKey,
    mergeDuplicateEventsForSource,
    classifyMatchStage,
    applyMergeToExisting,
    parseHoldingDate,
    pickWinnerSource,
    mergeCrossSourceEvent,
  } = await import('../src/helpers/merge/index.js');
  const { filterIngestEvents, isEventInPast } = await import('../src/helpers/eventFilters.js');
  const ParsedEventsSchema = (await import('../src/schemas/ParsedEventsSchema.js')).default;
  const dbName = process.env.DB_NAME || 'nomad_second';
  await mongoose.connect(`mongodb://localhost:27017/${dbName}`);

  console.log('\n=== 1. name_key + city identity ===');
  if (nameKey('Same Show') === nameKey('  same   show ')
    && cityKey('cityA') === 'cityA'
    && nameKey('Same Show') !== nameKey('Other')) {
    ok('name_key normalizes; city_key stable');
  } else fail('name_key');

  console.log('\n=== 2. In-batch merge ===');
  const batch = mergeDuplicateEventsForSource([
    {
      name: 'Merge Concert',
      city_id: 'cityBerlin',
      date_start: new Date('2026-09-01T20:00:00Z'),
      min_price: 40,
      max_price: 40,
      description: 'short',
      _mergeDates: [new Date('2026-09-01T20:00:00Z')],
    },
    {
      name: 'Merge Concert',
      city_id: 'cityBerlin',
      date_start: new Date('2026-09-03T20:00:00Z'),
      min_price: 55,
      max_price: 90,
      description: 'much longer description that should win',
      _mergeDates: [new Date('2026-09-03T20:00:00Z')],
    },
    {
      name: 'Other Show',
      city_id: 'cityBerlin',
      date_start: new Date('2026-09-05T20:00:00Z'),
      min_price: 10,
      max_price: 10,
    },
  ], 'kontramarka');
  if (batch.length === 2) ok('batch collapses same name+city');
  else fail('batch', `n=${batch.length}`);
  const concert = batch.find((e) => e.name === 'Merge Concert');
  if (concert && Number(concert.min_price) === 40 && Number(concert.max_price) === 90
    && String(concert.description).includes('longer')) {
    ok('batch unions prices + longest desc');
  } else fail('batch fields');

  console.log('\n=== 3. Past / no-city filters ===');
  if (isEventInPast({ date_end: new Date('2020-01-01') })) ok('past detected');
  else fail('past');
  if (!isEventInPast({ date_end: new Date('2099-01-01') })) ok('future kept');
  else fail('future');
  const filtered = filterIngestEvents([
    { name: 'a', city_id: 'c1', date_end: new Date('2099-01-01') },
    { name: 'b', city_id: null, date_end: new Date('2099-01-01') },
    { name: 'c', city_id: 'c1', date_end: new Date('2020-01-01') },
  ]);
  if (filtered.events.length === 1 && filtered.skippedNoCity === 1 && filtered.skippedPast === 1) {
    ok('filterIngestEvents');
  } else fail('filterIngestEvents', JSON.stringify(filtered));

  console.log('\n=== 4. Priority: discard lower / equal→newer ===');
  if (pickWinnerSource('fienta', 'eventim') === 'incoming') ok('eventim > fienta');
  else fail('priority high');
  if (pickWinnerSource('eventim', 'fienta') === 'discard_incoming') ok('lower discarded');
  else fail('priority discard', pickWinnerSource('eventim', 'fienta'));
  if (pickWinnerSource('eventim', 'ticketmaster') === 'incoming') ok('equal → incoming(newer)');
  else fail('priority equal');

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
    photos: [{ full_url: 'http://a' }],
  };
  const incoming = {
    name: 'Cross Show',
    city_id: 'cityX',
    address: 'Hall B',
    description: 'much longer from eventim',
    date_start: new Date('2026-08-05'),
    date_end: new Date('2026-08-05'),
    holding_date: '05.08.2026',
    min_price: 20,
    max_price: 40,
    photos: [{ full_url: 'http://b' }],
    contacts: { website: 'https://eventim.example' },
  };
  const { event: merged, winnerSource, discarded } = mergeCrossSourceEvent(
    existing, 'fienta', incoming, 'eventim',
  );
  if (!discarded && winnerSource === 'eventim'
    && merged.parser_unique_id === 'puid-cross-1'
    && merged.address === 'Hall B'
    && merged.photos?.length === 2
    && merged.photos.some((p) => p.full_url === 'http://a')
    && merged.photos.some((p) => p.full_url === 'http://b')
    && Number(merged.max_price) === 40
    && parseHoldingDate(merged.holding_date).length >= 2) {
    ok('cross merge: address by priority, photos union, dates via holding_date');
  } else {
    fail('cross merge', JSON.stringify({
      winnerSource,
      discarded,
      address: merged.address,
      photos: merged.photos,
      holding: merged.holding_date,
      max: merged.max_price,
    }));
  }

  // equal priority → merge (not discard)
  const eq = mergeCrossSourceEvent(existing, 'eventim', { ...incoming, source: 'ticketmaster' }, 'ticketmaster');
  if (!eq.discarded && eq.event.photos?.length === 2) ok('equal priority merges (photos union)');
  else fail('equal merge', JSON.stringify({ discarded: eq.discarded, photos: eq.event?.photos }));

  const low = mergeCrossSourceEvent(existing, 'eventim', incoming, 'fienta');
  if (low.discarded) ok('lower priority fully discarded');
  else fail('lower discard');

  console.log('\n=== 5. Same-source upsert stages ===');
  for (const src of SOURCES) {
    const m = mergeDuplicateEventsForSource([
      {
        name: 'S', city_id: 'c', date_start: new Date('2026-10-01'), min_price: 1, max_price: 2, _mergeDates: [new Date('2026-10-01')],
      },
      {
        name: 'S', city_id: 'c', date_start: new Date('2026-10-02'), min_price: 3, max_price: 12, _mergeDates: [new Date('2026-10-02')],
      },
    ], src);
    if (m.length === 1 && Number(m[0].max_price) === 12) ok(`${src}: batch merge`);
    else fail(`${src}: batch`);
  }

  console.log('\n=== 6. DB persistence by name_key+city_id ===');
  const stamp = Date.now();
  const name = `__merge_test_${stamp}`;
  const cityId = 'mergeTestCity';
  const source = 'eventim';
  const nk = nameKey(name);
  const cid = cityKey(cityId);

  await ParsedEventsSchema.deleteMany({ name_key: nk, city_id: cid });

  const upsertOne = async (incomingEv) => {
    const existingDoc = await ParsedEventsSchema.findOne({ name_key: nk, city_id: cid }).lean();
    const existingData = existingDoc?.event_data || null;
    const stage = classifyMatchStage(existingData, incomingEv);
    const { event: mergedEvent, changed: ch } = applyMergeToExisting(existingData, incomingEv, stage);
    if (stage === 'skip' || !ch) return { stage, changed: false };
    if (!existingDoc) {
      const parserUniqueId = mergedEvent.parser_unique_id || newParserUniqueId();
      const eventData = { ...mergedEvent, parser_unique_id: parserUniqueId };
      await ParsedEventsSchema.create({
        source,
        name_key: nk,
        city_id: cid,
        parser_unique_id: parserUniqueId,
        event_data: eventData,
      });
      return { stage, changed: true, action: 'insert' };
    }
    const parserUniqueId = existingDoc.parser_unique_id || newParserUniqueId();
    await ParsedEventsSchema.updateOne(
      { _id: existingDoc._id },
      { $set: { parser_unique_id: parserUniqueId, event_data: { ...mergedEvent, parser_unique_id: parserUniqueId } } },
    );
    return { stage, changed: true, action: 'update' };
  };

  const day1 = {
    name,
    city_id: cityId,
    address: 'Hall',
    description: 'day1',
    date_start: new Date('2026-12-01T19:00:00Z'),
    date_end: new Date('2026-12-01T19:00:00Z'),
    holding_date: '01.12.2026',
    min_price: 30,
    max_price: 30,
    source,
  };
  const day2 = {
    name,
    city_id: cityId,
    address: 'Other Hall',
    description: 'day2 longer description for merge test',
    date_start: new Date('2026-12-10T19:00:00Z'),
    date_end: new Date('2026-12-10T19:00:00Z'),
    holding_date: '10.12.2026',
    min_price: 45,
    max_price: 70,
    source,
  };

  const r1 = await upsertOne(day1);
  if (r1.action === 'insert') ok('DB insert');
  else fail('DB insert', JSON.stringify(r1));
  const r2 = await upsertOne(day2);
  if (r2.action === 'update') ok('DB update merge');
  else fail('DB update', JSON.stringify(r2));

  const docs = await ParsedEventsSchema.find({ name_key: nk, city_id: cid }).lean();
  if (docs.length === 1) ok('one doc');
  else fail('one doc', `n=${docs.length}`);
  const data = docs[0]?.event_data || {};
  if (Number(data.min_price) === 30 && Number(data.max_price) === 70
    && data.address === 'Other Hall'
    && parseHoldingDate(data.holding_date).length >= 2
    && data.parser_unique_id
    && !Object.prototype.hasOwnProperty.call(data, 'is_hidden')
    && !Object.prototype.hasOwnProperty.call(data, 'ticketmaster_id')) {
    ok('DB fields: union dates/prices, address from newer, no is_hidden/tm');
  } else {
    fail('DB fields', JSON.stringify({
      min: data.min_price, max: data.max_price, address: data.address, holding: data.holding_date,
    }));
  }

  await ParsedEventsSchema.deleteMany({ name_key: nk, city_id: cid });
  ok('cleanup');

  console.log(`\n=== RESULT: ${passed} passed, ${failed} failed ===`);
  await mongoose.disconnect();
  process.exit(failed ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
