import crypto from 'crypto';
import https from 'https';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { AI_CATEGORY_BATCH_CHARS, ENV, SETTINGS_KEYS } from '../helpers/constants';
import {
  normalizeCategoryKey,
  upsertCategorySuggestions,
} from './CategorySuggestionServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('AI_CATEGORY');

/** Bump when prompt text changes so cached Settings prompt is rebuilt. */
const AI_CATEGORY_PROMPT_VERSION = 'v3-strict-no-dupes';

const NAME_MAX = 120;
const DESC_MAX = 200;

const buildSystemPrompt = (categories) => {
  const usable = categories.filter((c) => c.name !== 'Другое');
  const list = usable.map((c) => `${c._id}:${c.name}`).join('\n');
  const namesOnly = usable.map((c) => c.name).join(', ');

  return `You categorize events for Nomad. Existing categories ONLY:
${list}

Names for quick scan: ${namesOnly}

HARD RULES (follow strictly):
1) ALWAYS assign an existing categoryId if the event is even roughly related by meaning.
   Examples that MUST map to existing (do NOT suggest new names):
   - music night / вечер музыки / концерт / DJ / оркестр → Музыка (or closest music-related id)
   - stand-up / comedy / юмор → Юмор
   - ballet / musical / circus show → Шоу/Мюзиклы or Танцы / Театр (pick closest existing)
   - kids / family / детский → Семейное
   - exhibition / museum show → Выставки or Искусство
   - lecture / talk / мастер-класс talk-like → Лекции/Семинары
2) Prefer existing over inventing. When in doubt → existing categoryId, suggestedName=null.
3) suggestedName ONLY if the topic is clearly outside ALL existing categories (e.g. cinema when no film category exists).
4) suggestedName MUST NOT be a synonym, subtype, or rephrase of any existing name.
   Forbidden near-duplicates of existing: "Концерты"≈Музыка, "Комедия"≈Юмор, "Балет"≈Танцы/Шоу, "Спектакль"≈Театр, "Для детей"≈Семейное, "Гастрономия вечер" if a food category exists, etc.
5) suggestedName: 1–2 words, broad, reusable; same language as the list; no city/date/artist/venue.
6) If categoryId is set → suggestedName MUST be null. Never invent category ids.

Return JSON only:
{"results":[{"id":"...","categoryId":"...|null","suggestedName":"...|null"}]}`;
};

export const computeCategoriesHash = (categories) => {
  const payload = `${AI_CATEGORY_PROMPT_VERSION}|${(categories || [])
    .map((c) => `${c._id}:${c.name}`)
    .sort()
    .join('|')}`;
  return crypto.createHash('sha256').update(payload).digest('hex');
};

export async function rebuildAiPromptIfNeeded() {
  const categories = await EventsCategoriesSchema.find({}).sort({ sort: 1 }).lean();
  const hash = computeCategoriesHash(categories);
  const existingHash = await SettingsSchema.findOne({ key: SETTINGS_KEYS.categoriesHash }).lean();
  const existingPrompt = await SettingsSchema.findOne({ key: SETTINGS_KEYS.aiCategoryPrompt }).lean();

  if (existingHash?.value === hash && existingPrompt?.value) {
    return { updated: false, prompt: existingPrompt.value, categories };
  }

  const prompt = buildSystemPrompt(categories);
  await SettingsSchema.findOneAndUpdate(
    { key: SETTINGS_KEYS.categoriesHash },
    { $set: { value: hash } },
    { upsert: true },
  );
  await SettingsSchema.findOneAndUpdate(
    { key: SETTINGS_KEYS.aiCategoryPrompt },
    { $set: { value: prompt } },
    { upsert: true },
  );

  logger.info(`AI category prompt rebuilt (categories=${categories.length}, ${AI_CATEGORY_PROMPT_VERSION})`);
  return { updated: true, prompt, categories };
}

const callOpenAi = async (systemPrompt, userContent) => {
  const apiKey = ENV.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  const body = JSON.stringify({
    model: ENV.OPENAI_MODEL || 'gpt-4o-mini',
    temperature: 0,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: systemPrompt },
      {
        role: 'user',
        content: `${userContent}\n\nJSON: {"results":[{"id":"...","categoryId":null,"suggestedName":null}]}`,
      },
    ],
  });

  return new Promise((resolve, reject) => {
    const url = new URL('https://api.openai.com/v1/chat/completions');
    const req = https.request({
      hostname: url.hostname,
      path: url.pathname,
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          if (res.statusCode < 200 || res.statusCode >= 300) {
            reject(new Error(`OpenAI HTTP ${res.statusCode}: ${data.slice(0, 500)}`));
            return;
          }
          const parsed = JSON.parse(data);
          const content = parsed?.choices?.[0]?.message?.content || '';
          const usage = parsed?.usage || {};
          resolve({ content, usage });
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
};

const parseAiResults = (content) => {
  try {
    const parsed = JSON.parse(content);
    const list = Array.isArray(parsed) ? parsed : (parsed.results || parsed.items || []);
    if (!Array.isArray(list)) return [];
    return list
      .map((item) => ({
        id: String(item.id || item.eventId || ''),
        categoryId: item.categoryId || item.category_id || null,
        suggestedName: item.suggestedName || item.suggested_name || item.suggestion || null,
      }))
      .filter((item) => item.id);
  } catch (e) {
    logger.error(`Failed to parse AI response: ${e.message}`);
    return [];
  }
};

const eventPayloadLine = (ev) => JSON.stringify({
  id: ev.tempId,
  name: (ev.name || '').slice(0, NAME_MAX),
  description: (ev.description || '').slice(0, DESC_MAX),
});

/**
 * Collapse suggestions that are synonyms / near-duplicates of existing categories.
 * Returns existing categoryId or null if truly new.
 */
const resolveSuggestionAgainstExisting = (suggestedName, categoryByKey, categories) => {
  const key = normalizeCategoryKey(suggestedName);
  if (!key) return null;
  if (categoryByKey.has(key)) return categoryByKey.get(key);

  // substring / containment against existing names (Музыка ↔ концерты музыки)
  for (const cat of categories || []) {
    if (!cat?.name || cat.name === 'Другое') continue;
    const existingKey = normalizeCategoryKey(cat.name);
    if (!existingKey || existingKey.length < 3) continue;
    if (key.includes(existingKey) || existingKey.includes(key)) {
      return String(cat._id);
    }
  }

  // common synonym stems → existing Russian categories
  const SYN = [
    [/^(концерт|музыка|оркестр|джаз|рок|симфон|dj|диджей)/, 'музыка'],
    [/^(юмор|комеди|стендап|standup|kvn|квн)/, 'юмор'],
    [/^(театр|спектакл|драм)/, 'театр'],
    [/^(балет|танц|dance)/, 'танцы'],
    [/^(мюзикл|шоу|цирк)/, 'шоу/мюзиклы'],
    [/^(дет|семь|family|kids)/, 'семейное'],
    [/^(выставк|экспозиц)/, 'выставки'],
    [/^(лекци|семинар|talk|мастер.?класс)/, 'лекции/семинары'],
    [/^(спорт|матч|турнир)/, 'спорт'],
    [/^(фестивал)/, 'фестивали'],
    [/^(духовн|религ)/, 'духовное'],
    [/^(искусств)/, 'искусство'],
  ];
  for (const [re, targetKey] of SYN) {
    if (re.test(key) && categoryByKey.has(targetKey)) {
      return categoryByKey.get(targetKey);
    }
  }
  return null;
};

/**
 * @returns {{
 *  map: Map<string, string|null>,
 *  suggestions: Map<string, string|null>,
 *  usage: object,
 *  suggestionUpsert: object|null,
 *  tokensBySuggestion: Array<{name:string, events:number, tokens:number}>
 * }}
 */
/**
 * @param {Array} events
 * @param {{ persistSuggestions?: boolean }} [options]
 */
export async function categorizeEventsWithAi(events, options = {}) {
  const persistSuggestions = options.persistSuggestions !== false;
  const map = new Map();
  const suggestions = new Map();
  const usageTotals = {
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    batches: 0,
    failedBatches: 0,
  };
  const tokensBySuggestionMap = new Map();

  if (!events?.length) {
    return {
      map, suggestions, usage: usageTotals, suggestionUpsert: null, tokensBySuggestion: [],
    };
  }

  if (!ENV.OPENAI_API_KEY) {
    logger.warn('OPENAI_API_KEY missing — skip AI categorization');
    events.forEach((e) => {
      map.set(e.tempId, null);
      suggestions.set(e.tempId, null);
    });
    return {
      map, suggestions, usage: usageTotals, suggestionUpsert: null, tokensBySuggestion: [],
    };
  }

  const { prompt, categories } = await rebuildAiPromptIfNeeded();
  const validCategoryIds = new Set(
    (categories || await EventsCategoriesSchema.find({}).select('_id name').lean())
      .map((c) => String(c._id)),
  );
  const cats = categories || await EventsCategoriesSchema.find({}).select('_id name').lean();
  const categoryByKey = new Map(
    cats.map((c) => [normalizeCategoryKey(c.name), String(c._id)]),
  );

  const batches = [];
  let current = [];
  let currentLen = 0;

  for (const ev of events) {
    const line = eventPayloadLine(ev);
    if (current.length && currentLen + line.length > AI_CATEGORY_BATCH_CHARS) {
      batches.push(current);
      current = [];
      currentLen = 0;
    }
    current.push(ev);
    currentLen += line.length + 1;
  }
  if (current.length) batches.push(current);

  logger.info(`AI categorization: ${events.length} events in ${batches.length} batch(es)`);

  const upsertItems = [];

  for (let i = 0; i < batches.length; i += 1) {
    const batch = batches[i];
    const userContent = batch.map(eventPayloadLine).join('\n');

    try {
      // eslint-disable-next-line no-await-in-loop
      const { content, usage } = await callOpenAi(prompt, userContent);
      const batchPrompt = usage.prompt_tokens || 0;
      const batchCompletion = usage.completion_tokens || 0;
      const batchTotal = usage.total_tokens || (batchPrompt + batchCompletion);
      usageTotals.prompt_tokens += batchPrompt;
      usageTotals.completion_tokens += batchCompletion;
      usageTotals.total_tokens += batchTotal;
      usageTotals.batches += 1;

      const results = parseAiResults(content);
      const byId = new Map(results.map((r) => [r.id, r]));
      const tokensPerEvent = batch.length ? batchTotal / batch.length : 0;

      for (const ev of batch) {
        const row = byId.get(ev.tempId) || {};
        let catId = row.categoryId || null;
        let suggested = row.suggestedName ? String(row.suggestedName).trim() : null;

        if (catId && !validCategoryIds.has(String(catId))) {
          catId = null;
        }

        // Collapse synonyms / near-duplicates of existing categories
        if (!catId && suggested) {
          const existingId = resolveSuggestionAgainstExisting(suggested, categoryByKey, cats);
          if (existingId) {
            catId = existingId;
            suggested = null;
          }
        }

        if (catId) suggested = null;

        map.set(ev.tempId, catId);
        suggestions.set(ev.tempId, suggested);

        if (suggested) {
          const key = normalizeCategoryKey(suggested);
          const prev = tokensBySuggestionMap.get(key) || {
            name: suggested,
            events: 0,
            tokens: 0,
          };
          prev.events += 1;
          prev.tokens += tokensPerEvent;
          tokensBySuggestionMap.set(key, prev);

          upsertItems.push({
            name: suggested,
            source: ev.source || undefined,
            exampleEvent: ev.name || '',
            tokens: tokensPerEvent,
          });
        }
      }
    } catch (e) {
      logger.error(`AI batch ${i + 1}/${batches.length} failed: ${e.message}`);
      usageTotals.failedBatches += 1;
      batch.forEach((ev) => {
        map.set(ev.tempId, null);
        suggestions.set(ev.tempId, null);
      });
    }
  }

  let suggestionUpsert = null;
  if (upsertItems.length && persistSuggestions) {
    suggestionUpsert = await upsertCategorySuggestions(upsertItems);
  } else if (upsertItems.length && !persistSuggestions) {
    suggestionUpsert = { skippedPersist: true, wouldUpsert: upsertItems.length };
  }

  const tokensBySuggestion = [...tokensBySuggestionMap.values()]
    .map((r) => ({
      name: r.name,
      events: r.events,
      tokens: Math.round(r.tokens),
    }))
    .sort((a, b) => b.tokens - a.tokens);

  logger.info(`OpenAI usage: ${JSON.stringify(usageTotals)}`);
  if (tokensBySuggestion.length) {
    logger.info(`Suggestion token shares: ${JSON.stringify(tokensBySuggestion)}`);
  }

  return {
    map,
    suggestions,
    usage: usageTotals,
    suggestionUpsert,
    tokensBySuggestion,
  };
}

export default {
  rebuildAiPromptIfNeeded,
  categorizeEventsWithAi,
  computeCategoriesHash,
};
