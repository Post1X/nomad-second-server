#!/usr/bin/env babel-node
/**
 * Smoke-test AI categorize + category suggestions.
 *
 * Uses ParsedEvents that currently have no real category / default_other,
 * or a built-in sample list if DB is empty.
 *
 *   yarn babel-node -r dotenv/config scripts/testCategorySuggestionsAi.js
 *   LIMIT=20 yarn babel-node -r dotenv/config scripts/testCategorySuggestionsAi.js
 */
/* eslint-disable no-console */
import dotenv from 'dotenv';
import path from 'path';
import mongoose from 'mongoose';
import crypto from 'crypto';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

import EventsCategoriesSchema from '../src/schemas/EventsCategoriesSchema';
import ParsedEventsSchema from '../src/schemas/ParsedEventsSchema';
import CategorySuggestions from '../src/schemas/CategorySuggestionsSchema';
import { categorizeEventsWithAi, rebuildAiPromptIfNeeded } from '../src/services/AiCategoryServices';

const LIMIT = Math.max(1, Math.min(120, parseInt(process.env.LIMIT || '16', 10) || 16));

const SAMPLE_EVENTS = [
  {
    name: 'Кино под открытым небом: летний показ',
    description: 'Показ фильма на площади. Приносите пледы. Классика и новинки.',
    source: 'eventim',
  },
  {
    name: 'Мастер-класс по керамике',
    description: 'Лепим чашки своими руками, обжиг и роспись для взрослых.',
    source: 'fienta',
  },
  {
    name: 'Стендап вечер открытого микрофона',
    description: 'Комедия, шутки начинающих комиков, вход свободный.',
    source: 'ticketmaster',
  },
  {
    name: 'Йога на рассвете в парке',
    description: 'Утренняя практика хатха-йоги для всех уровней.',
    source: 'israelinfo',
  },
  {
    name: 'Детский квест в музее',
    description: 'Интерактивная игра для детей 6–10 лет с аниматорами.',
    source: 'kontramarka',
  },
  {
    name: 'Винная дегустация и сыры',
    description: 'Гастрономический вечер с сомелье, закуски и дегустация.',
    source: 'eventim',
  },
  {
    name: 'Турнир по настольному теннису',
    description: 'Любительский спорт, регистрация команд на месте.',
    source: 'fienta',
  },
  {
    name: 'Выставка современного искусства',
    description: 'Инсталляции и живопись молодых авторов.',
    source: 'ticketmaster',
  },
];

async function loadEventsFromDb() {
  const other = await EventsCategoriesSchema.findOne({ name: 'Другое' }).lean();
  const otherId = other?._id ? String(other._id) : null;
  const hardFilter = otherId
    ? {
      $or: [
        { 'event_data.events_category_id': null },
        { 'event_data.events_category_id': { $exists: false } },
        { 'event_data.events_category_id': otherId },
        { 'event_data.category_resolved_by': 'default_other' },
      ],
    }
    : {};

  let docs = await ParsedEventsSchema.find(hardFilter)
    .sort({ updatedAt: -1 })
    .limit(LIMIT)
    .lean();

  // Not enough uncategorized — top up with any ParsedEvents for a larger AI batch
  if (docs.length < LIMIT) {
    const seen = new Set(docs.map((d) => String(d._id)));
    const extra = await ParsedEventsSchema.find({})
      .sort({ updatedAt: -1 })
      .limit(LIMIT * 3)
      .lean();
    for (const d of extra) {
      if (seen.has(String(d._id))) continue;
      docs.push(d);
      seen.add(String(d._id));
      if (docs.length >= LIMIT) break;
    }
  }

  return docs.map((d) => ({
    tempId: crypto.randomUUID(),
    name: d.event_data?.name || '',
    description: d.event_data?.description || '',
    source: d.source,
    fromDb: true,
    parsedId: String(d._id),
  })).filter((e) => e.name);
}

async function main() {
  const dbName = process.env.DB_NAME;
  if (!dbName) throw new Error('DB_NAME missing');
  if (!process.env.OPENAI_API_KEY) throw new Error('OPENAI_API_KEY missing');

  await mongoose.connect(`mongodb://localhost:27017/${dbName}`);
  console.log('DB ok:', dbName);

  const cats = await EventsCategoriesSchema.find({}).sort({ sort: 1 }).lean();
  console.log('\nExisting categories:', cats.map((c) => c.name).join(', '));

  await rebuildAiPromptIfNeeded();

  let events = await loadEventsFromDb();
  if (events.length < 4) {
    console.log(`\nDB gave ${events.length} events — using SAMPLE_EVENTS`);
    events = SAMPLE_EVENTS.slice(0, LIMIT).map((e) => ({
      ...e,
      tempId: crypto.randomUUID(),
      fromDb: false,
    }));
  } else {
    console.log(`\nLoaded ${events.length} events from ParsedEvents for AI`);
  }

  console.log('\n--- Sending to AI ---');
  events.forEach((e, i) => console.log(`${i + 1}. [${e.source}] ${e.name.slice(0, 80)}`));

  const started = Date.now();
  const {
    map, suggestions, usage, suggestionUpsert, tokensBySuggestion,
  } = await categorizeEventsWithAi(events);
  const ms = Date.now() - started;

  console.log('\n=== OpenAI usage (total) ===');
  console.log(JSON.stringify(usage, null, 2));
  console.log(`elapsed_ms: ${ms}`);

  console.log('\n=== Per-event results ===');
  for (const ev of events) {
    const catId = map.get(ev.tempId);
    const sug = suggestions.get(ev.tempId);
    const catName = catId
      ? (cats.find((c) => String(c._id) === String(catId))?.name || catId)
      : null;
    console.log({
      name: ev.name.slice(0, 70),
      category: catName,
      suggestedName: sug || null,
    });
  }

  console.log('\n=== Tokens ≈ by suggested category ===');
  if (!tokensBySuggestion.length) {
    console.log('(no new suggestions — all matched existing or null)');
  } else {
    for (const row of tokensBySuggestion) {
      console.log(`- ${row.name}: events=${row.events}, tokens≈${row.tokens}`);
    }
  }

  console.log('\n=== Upsert into CategorySuggestions ===');
  console.log(JSON.stringify(suggestionUpsert, null, 2));

  const pending = await CategorySuggestions.find({ status: 'pending' })
    .sort({ hit_count: -1 })
    .limit(20)
    .lean();
  console.log('\n=== Pending CategorySuggestions (top 20) ===');
  pending.forEach((p) => {
    console.log({
      name: p.raw_name,
      hits: p.hit_count,
      tokens_total: Math.round(p.tokens_total || 0),
      examples: (p.example_events || []).slice(0, 2),
    });
  });

  await mongoose.disconnect();
  console.log('\nDone.');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
