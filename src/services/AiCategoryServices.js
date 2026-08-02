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
const AI_CATEGORY_PROMPT_VERSION = 'v2-suggestions';

const NAME_MAX = 120;
const DESC_MAX = 200;

const buildSystemPrompt = (categories) => {
  const list = categories
    .filter((c) => c.name !== 'Другое')
    .map((c) => `${c._id}:${c.name}`)
    .join('\n');

  return `Event categorizer for Nomad. Existing categories (id:name):
${list}

For each event:
1) Prefer an existing categoryId from the list if it fits reasonably.
2) If NONE fit, set categoryId=null and suggestedName to ONE broad reusable category (same language/style as the list).
Rules for suggestedName: short (1-3 words), general (e.g. "Фильмы", not "Кино под открытым небом"); no city/date/artist; no near-duplicates of existing names.
If categoryId is set, suggestedName must be null.
Never invent category ids.

Return JSON: {"results":[{"id":"...","categoryId":"...|null","suggestedName":"...|null"}]}`;
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
 * @returns {{
 *  map: Map<string, string|null>,
 *  suggestions: Map<string, string|null>,
 *  usage: object,
 *  suggestionUpsert: object|null,
 *  tokensBySuggestion: Array<{name:string, events:number, tokens:number}>
 * }}
 */
export async function categorizeEventsWithAi(events) {
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
  const categoryByKey = new Map(
    (categories || await EventsCategoriesSchema.find({}).select('_id name').lean())
      .map((c) => [normalizeCategoryKey(c.name), String(c._id)]),
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

        // If model suggested a name that already exists — use existing id
        if (!catId && suggested) {
          const existingId = categoryByKey.get(normalizeCategoryKey(suggested));
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
  if (upsertItems.length) {
    suggestionUpsert = await upsertCategorySuggestions(upsertItems);
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
