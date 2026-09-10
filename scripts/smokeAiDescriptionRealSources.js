/**
 * Real-sample description enrich across all sources (local, no DB writes).
 * yarn babel-node -r dotenv/config scripts/smokeAiDescriptionRealSources.js
 */
import fs from 'fs';
import path from 'path';
import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

import ParsedEventsSchema from '../src/schemas/ParsedEventsSchema';
import { enrichEventDescriptions } from '../src/services/AiDescriptionServices';

const SOURCES = ['ticketmaster', 'eventim', 'israelinfo', 'kontramarka', 'fienta'];
const PER_BUCKET = 3;

const strip = (s) => String(s || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

const bucketOf = (ed) => {
  const name = strip(ed.name);
  const desc = strip(ed.description);
  if (!desc) return 'empty';
  if (desc === name) return 'title_copy';
  if (desc.length < 40) return 'short';
  if (desc.length < 120) return 'medium';
  return 'long';
};

async function pickSamples() {
  const picked = [];
  const seen = new Set();

  for (const source of SOURCES) {
    const docs = await ParsedEventsSchema.find({ source })
      .sort({ updatedAt: -1 })
      .limit(400)
      .lean();

    const buckets = {
      empty: [],
      title_copy: [],
      short: [],
      medium: [],
      long: [],
    };

    for (const doc of docs) {
      const ed = doc.event_data || {};
      const b = bucketOf(ed);
      if (buckets[b].length >= PER_BUCKET) continue;
      const key = `${source}:${doc.parser_unique_id || doc._id}`;
      if (seen.has(key)) continue;
      seen.add(key);
      buckets[b].push({
        source,
        bucket: b,
        parser_unique_id: doc.parser_unique_id || null,
        name: strip(ed.name).slice(0, 120),
        description: strip(ed.description).slice(0, 500),
        address: strip(ed.address).slice(0, 160),
      });
    }

    for (const b of Object.keys(buckets)) {
      for (const row of buckets[b]) picked.push(row);
    }
  }

  return picked;
}

async function main() {
  const uri = process.env.MONGO_URI || process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
  const dbName = process.env.DB_NAME || 'nomad_second';
  await mongoose.connect(uri, { dbName });

  const samples = await pickSamples();
  console.log(`picked ${samples.length} real samples`);

  const bySourceBefore = {};
  for (const s of samples) {
    bySourceBefore[s.source] = (bySourceBefore[s.source] || 0) + 1;
  }
  console.log('bySource', bySourceBefore);

  // Clone for enrich (mutates)
  const working = samples.map((s) => ({
    name: s.name,
    description: s.description,
    address: s.address,
    source: s.source,
  }));

  const { events, stats } = await enrichEventDescriptions(working);

  const rows = samples.map((s, i) => {
    const after = events[i];
    const beforeDesc = s.description || '';
    const afterDesc = after.description || '';
    return {
      source: s.source,
      bucket: s.bucket,
      name: s.name,
      address: s.address,
      before: beforeDesc,
      after: afterDesc,
      changed: beforeDesc !== afterDesc,
      description_resolved_by: after.description_resolved_by || null,
      beforeLen: beforeDesc.length,
      afterLen: afterDesc.length,
    };
  });

  const summary = {
    total: rows.length,
    changed: rows.filter((r) => r.changed).length,
    unchanged: rows.filter((r) => !r.changed).length,
    bySource: {},
    byBucket: {},
    enrichStats: stats,
  };

  for (const r of rows) {
    if (!summary.bySource[r.source]) summary.bySource[r.source] = { n: 0, changed: 0 };
    summary.bySource[r.source].n += 1;
    if (r.changed) summary.bySource[r.source].changed += 1;

    if (!summary.byBucket[r.bucket]) summary.byBucket[r.bucket] = { n: 0, changed: 0 };
    summary.byBucket[r.bucket].n += 1;
    if (r.changed) summary.byBucket[r.bucket].changed += 1;
  }

  const outDir = path.resolve(__dirname, '../tmp');
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, 'smoke-ai-description-real.json');
  fs.writeFileSync(outPath, JSON.stringify({ summary, rows }, null, 2));

  console.log('SUMMARY', JSON.stringify(summary, null, 2));
  console.log('CHANGED_SAMPLES');
  for (const r of rows.filter((x) => x.changed).slice(0, 25)) {
    console.log(JSON.stringify({
      source: r.source,
      bucket: r.bucket,
      name: r.name,
      before: r.before.slice(0, 140),
      after: r.after.slice(0, 140),
    }, null, 2));
  }
  console.log('UNCHANGED_SAMPLES');
  for (const r of rows.filter((x) => !x.changed).slice(0, 15)) {
    console.log(JSON.stringify({
      source: r.source,
      bucket: r.bucket,
      name: r.name,
      before: r.before.slice(0, 140),
    }, null, 2));
  }
  console.log('WRITTEN', outPath);

  await mongoose.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
