/**
 * Run enrich on pre-exported real samples (no DB writes).
 * yarn babel-node -r dotenv/config scripts/smokeAiDescriptionFromFile.js
 */
import fs from 'fs';
import path from 'path';
import { enrichEventDescriptions } from '../src/services/AiDescriptionServices';

const IN = process.env.IN
  || path.resolve(__dirname, '../tmp/real-samples.json');
const OUT = process.env.OUT
  || path.resolve(__dirname, '../tmp/smoke-ai-description-real.json');

async function main() {
  const raw = JSON.parse(fs.readFileSync(IN, 'utf8'));
  const samples = raw.samples || raw;
  console.log(`loaded ${samples.length} samples from ${IN}`);

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
      address: s.address || '',
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

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify({ summary, rows }, null, 2));
  console.log('SUMMARY', JSON.stringify(summary, null, 2));

  console.log('\n=== CHANGED ===');
  for (const r of rows.filter((x) => x.changed)) {
    console.log(`\n[${r.source}/${r.bucket}] ${r.name}`);
    console.log(`  BEFORE(${r.beforeLen}): ${r.before.slice(0, 220)}`);
    console.log(`  AFTER (${r.afterLen}): ${r.after.slice(0, 220)}`);
  }

  console.log('\n=== UNCHANGED (sample) ===');
  for (const r of rows.filter((x) => !x.changed).slice(0, 20)) {
    console.log(`\n[${r.source}/${r.bucket}] ${r.name}`);
    console.log(`  KEEP(${r.beforeLen}): ${r.before.slice(0, 220)}`);
  }

  console.log('\nWRITTEN', OUT);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
