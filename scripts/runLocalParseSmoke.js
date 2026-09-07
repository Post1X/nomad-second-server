/**
 * Limited local parse smoke (writes to local Mongo only).
 *
 * Usage:
 *   yarn babel-node -r dotenv/config scripts/runLocalParseSmoke.js kontramarka
 *   yarn babel-node -r dotenv/config scripts/runLocalParseSmoke.js ticketmaster
 */
import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

import startParseRun from '../src/helpers/startParseRun';
import ParseRunsSchema from '../src/schemas/ParseRunsSchema';
import ParsedEventsSchema from '../src/schemas/ParsedEventsSchema';
import { EVENT_SOURCE, OPERATION_STATUSES } from '../src/helpers/constants';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitRun(runId) {
  // eslint-disable-next-line no-constant-condition
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    await sleep(4000);
    // eslint-disable-next-line no-await-in-loop
    const run = await ParseRunsSchema.findById(runId).lean();
    if (!run) throw new Error('parse run disappeared');
    const tail = String(run.infoText || '').split('\n').slice(-3).join(' | ');
    console.log(`[${run.status}] ${tail.slice(0, 220)}`);
    if (run.status === OPERATION_STATUSES.success || run.status === OPERATION_STATUSES.error) {
      return run;
    }
  }
}

async function main() {
  const source = (process.argv[2] || 'kontramarka').toLowerCase();
  const uri = process.env.MONGO_URI || process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
  await mongoose.connect(uri, { dbName: process.env.DB_NAME || 'nomad_second' });

  let meta = {};
  if (source === 'kontramarka') {
    meta = {
      maxCities: 1,
      cityName: 'Berlin',
      maxTours: 3,
      maxEvents: 5,
    };
  } else if (source === 'ticketmaster') {
    meta = {
      countryCode: 'EE',
      maxPages: 1,
    };
  } else if (source === 'israelinfo') {
    meta = {};
  } else if (source === 'fienta') {
    meta = { maxCities: 1, cityName: 'Tallinn' };
  } else if (source === 'eventim') {
    meta = { maxCities: 1 };
  } else {
    throw new Error(`Unknown source: ${source}`);
  }

  console.log('START', { source, meta, db: process.env.DB_NAME || 'nomad_second' });
  const before = await ParsedEventsSchema.countDocuments({ source });
  const runId = await startParseRun(source, meta);
  console.log('runId', String(runId));
  const run = await waitRun(runId);

  let stats = {};
  try {
    stats = run.statistics ? JSON.parse(run.statistics) : {};
  } catch (e) {
    stats = { raw: run.statistics };
  }

  const afterDocs = await ParsedEventsSchema.find({ source, parse_run: runId }).lean();
  const sample = afterDocs.slice(0, 12).map((d) => {
    const ed = d.event_data || {};
    return {
      name: String(ed.name || '').slice(0, 80),
      descLen: String(ed.description || '').length,
      desc: String(ed.description || '').slice(0, 140).replace(/\n/g, ' '),
      category_resolved_by: ed.category_resolved_by,
      is_active: ed.is_active,
      needs_manual_review: ed.needs_manual_review,
      events_category_id: ed.events_category_id,
      hasSpecialization: Object.prototype.hasOwnProperty.call(ed, 'specialization'),
    };
  });

  const byResolved = {};
  const byActive = { true: 0, false: 0, unset: 0 };
  for (const d of afterDocs) {
    const ed = d.event_data || {};
    const rb = ed.category_resolved_by || 'unset';
    byResolved[rb] = (byResolved[rb] || 0) + 1;
    if (ed.is_active === true) byActive.true += 1;
    else if (ed.is_active === false) byActive.false += 1;
    else byActive.unset += 1;
  }

  console.log(JSON.stringify({
    status: run.status,
    errorText: run.errorText || null,
    beforeCount: before,
    insertedThisRun: afterDocs.length,
    upsert: stats.upsert || null,
    descriptionEnrich: stats.upsert?.descriptionEnrich || stats.process?.descriptionEnrich || null,
    byResolved,
    byActive,
    sample,
  }, null, 2));

  await mongoose.disconnect();
  if (run.status === OPERATION_STATUSES.error) process.exit(1);
}

main().catch(async (e) => {
  console.error(e);
  try { await mongoose.disconnect(); } catch (err) { /* ignore */ }
  process.exit(1);
});
