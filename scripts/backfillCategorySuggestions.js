#!/usr/bin/env babel-node
/**
 * Walk ParsedEvents without a real category and run AI categorization
 * so missing topics land in CategorySuggestions (candidates UI).
 *
 *   yarn babel-node -r dotenv/config scripts/backfillCategorySuggestions.js
 *   LIMIT=100 yarn babel-node -r dotenv/config scripts/backfillCategorySuggestions.js
 *   DRY_RUN=1 LIMIT=20 yarn babel-node -r dotenv/config scripts/backfillCategorySuggestions.js
 *
 * Env:
 *   LIMIT   — max events (default: all)
 *   DRY_RUN — if 1, call AI but skip writing CategorySuggestions / event_data
 *   APPLY_CATEGORY — if 1, also write events_category_id onto ParsedEvents.event_data
 *   CHUNK   — events per AI chunk (default 40)
 *
 * UI: /parsing/stats-ui#catcands → «Запустить AI backfill»
 */
/* eslint-disable no-console */
import dotenv from 'dotenv';
import path from 'path';
import mongoose from 'mongoose';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

import {
  getCategorySuggestionsBackfillJob,
  runCategorySuggestionsBackfill,
} from '../src/services/CategorySuggestionsBackfillServices';

const LIMIT = process.env.LIMIT ? Math.max(1, parseInt(process.env.LIMIT, 10) || 0) : null;
const DRY_RUN = String(process.env.DRY_RUN || '') === '1';
const APPLY_CATEGORY = String(process.env.APPLY_CATEGORY || '') === '1';
const CHUNK = Math.max(10, Math.min(80, parseInt(process.env.CHUNK || '40', 10) || 40));

async function main() {
  if (!process.env.DB_NAME) throw new Error('DB_NAME missing');
  if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY missing');

  await mongoose.connect(`mongodb://localhost:27017/${process.env.DB_NAME}`);
  console.log('DB:', process.env.DB_NAME);
  console.log({ LIMIT, DRY_RUN, APPLY_CATEGORY, CHUNK });

  await runCategorySuggestionsBackfill({
    limit: LIMIT,
    applyCategory: APPLY_CATEGORY,
    dryRun: DRY_RUN,
    chunk: CHUNK,
  });

  let printed = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const snap = getCategorySuggestionsBackfillJob();
    const logs = snap.logs || [];
    while (printed < logs.length) {
      console.log(logs[printed].msg);
      printed += 1;
    }
    if (!snap.running) {
      if (snap.error) {
        console.error('Job error:', snap.error);
        process.exitCode = 1;
      } else if (snap.result) {
        console.log('\nResult:', JSON.stringify(snap.result, null, 2));
      }
      break;
    }
    // eslint-disable-next-line no-await-in-loop
    await new Promise((r) => setTimeout(r, 400));
  }

  await mongoose.disconnect();
  console.log('\nDone. UI: /parsing/stats-ui#catcands');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
