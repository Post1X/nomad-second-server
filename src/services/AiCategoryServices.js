import crypto from 'crypto';
import https from 'https';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { AI_CATEGORY_BATCH_CHARS, ENV, SETTINGS_KEYS } from '../helpers/constants';
import { normalizeCategoryKey } from './CategorySuggestionServices';
import {
  CATEGORY_CARDS_VERSION,
  formatCategoryCardsForPrompt,
} from '../config/categoryCards';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('AI_CATEGORY');

/** Bump when prompt text / cards change so cached Settings prompt is rebuilt. */
const AI_CATEGORY_PROMPT_VERSION = `v11-cards-existing-only+${CATEGORY_CARDS_VERSION}`;

const NAME_MAX = 120;
const DESC_MAX = 200;

/** Hot-path prompt: assign EXISTING categories only (option A cards, no invent). */
const buildSystemPrompt = (categories) => {
  const usable = (categories || []).filter((c) => c.name !== 'Другое');
  const cards = formatCategoryCardsForPrompt(usable);

  return `You categorize events for Nomad. Use EXISTING categories ONLY.

CATEGORY CARDS (id | name + when to use):
${cards}

RULES:
1) Return categoryId from the cards above if the event fits the "use" guidance.
2) Respect NOT: never put concerts/bands/DJ/tour upgrades into anything except Музыка.
3) If nothing fits → categoryId=null. Do NOT invent new category names here.
4) Never invent ids.

JSON only:
{"results":[{"id":"...","categoryId":"...|null"}]}`;
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

const callOpenAi = async (systemPrompt, userContent, jsonHint = null) => {
  const apiKey = ENV.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  const hint = jsonHint
    || '{"results":[{"id":"...","categoryId":null}]}';

  const body = JSON.stringify({
    model: ENV.OPENAI_MODEL || 'gpt-4o-mini',
    temperature: 0,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: systemPrompt },
      {
        role: 'user',
        content: `${userContent}\n\nJSON: ${hint}`,
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
    [/^(дет|семь|family|kids|children|child)/, 'семейное'],
    [/^(выставк|экспозиц)/, 'выставки'],
    [/^(лекци|семинар|talk|мастер.?класс)/, 'лекции/семинары'],
    [/^(спорт|матч|турнир)/, 'спорт'],
    [/^(фестивал)/, 'фестивали'],
    [/^(духовн|религ)/, 'духовное'],
    [/^(искусств)/, 'искусство'],
    [/^(фильм|кино|film|movie|cinema|screening)/, 'фильмы'],
    [/^(экскурс|tour|sightseeing)/, 'экскурсии'],
  ];
  for (const [re, targetKey] of SYN) {
    if (re.test(key) && categoryByKey.has(targetKey)) {
      return categoryByKey.get(targetKey);
    }
  }
  return null;
};

/**
 * Hot path: assign EXISTING categoryId only (cards in system prompt).
 * New category candidates come from proposeCategoriesFromEvents (discovery job).
 *
 * @param {Array} events
 * @param {object} [options] unused, kept for call-site compat
 */
export async function categorizeEventsWithAi(events, options = {}) {
  void options;
  const map = new Map();
  const suggestions = new Map();
  const usageTotals = {
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    batches: 0,
    failedBatches: 0,
  };

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

  logger.info(`AI categorization (existing-only): ${events.length} in ${batches.length} batch(es)`);

  for (let i = 0; i < batches.length; i += 1) {
    const batch = batches[i];
    const userContent = batch.map(eventPayloadLine).join('\n');

    try {
      // eslint-disable-next-line no-await-in-loop
      const { content, usage } = await callOpenAi(
        prompt,
        userContent,
        '{"results":[{"id":"...","categoryId":null}]}',
      );
      usageTotals.prompt_tokens += usage.prompt_tokens || 0;
      usageTotals.completion_tokens += usage.completion_tokens || 0;
      usageTotals.total_tokens += usage.total_tokens
        || ((usage.prompt_tokens || 0) + (usage.completion_tokens || 0));
      usageTotals.batches += 1;

      const results = parseAiResults(content);
      const byId = new Map(results.map((r) => [r.id, r]));

      for (const ev of batch) {
        const row = byId.get(ev.tempId) || {};
        let catId = row.categoryId || null;

        if (catId && !validCategoryIds.has(String(catId))) {
          catId = null;
        }

        // If model still returns a name, only fold into existing
        const suggested = row.suggestedName ? String(row.suggestedName).trim() : null;
        if (!catId && suggested) {
          const existingId = resolveSuggestionAgainstExisting(suggested, categoryByKey, cats);
          if (existingId) catId = existingId;
        }

        map.set(ev.tempId, catId);
        suggestions.set(ev.tempId, null);
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

  logger.info(`OpenAI usage: ${JSON.stringify(usageTotals)}`);

  return {
    map,
    suggestions,
    usage: usageTotals,
    suggestionUpsert: null,
    tokensBySuggestion: [],
  };
}

/**
 * Discovery (option B): one-shot over a sample of uncategorized events.
 * Uses category cards (A). Proposes ≤ maxCategories NEW types with verified exampleIds.
 * Hits = number of events assigned to that newCategory in the same response.
 *
 * @param {Array<{ tempId: string, name?: string, description?: string, source?: string }>} events
 * @param {{ maxCategories?: number, categories?: Array<{_id:any,name:string}> }} [options]
 */
export async function proposeCategoriesFromEvents(events, options = {}) {
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));
  const sample = (events || []).slice(0, 180);

  if (!sample.length) {
    return { categories: [], assignments: [], usage: null };
  }
  if (!ENV.OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  const dbCats = options.categories
    || await EventsCategoriesSchema.find({}).sort({ sort: 1 }).lean();
  const usable = (dbCats || []).filter((c) => c.name && c.name !== 'Другое');
  const existingNames = usable.map((c) => c.name);
  const existingKeySet = new Set(existingNames.map((n) => normalizeCategoryKey(n)));
  const cards = formatCategoryCardsForPrompt(usable);
  const byId = new Map(sample.map((e) => [e.tempId, e]));

  const systemPrompt = `You discover MISSING event-type categories for Nomad.

EXISTING category cards (prefer these over inventing):
${cards}

TASK — look at the whole event list together (not one-by-one in isolation):
1) For each event: if it fits an EXISTING card, set existingName to that exact Russian name; newCategory=null.
2) Only if it fits NONE of the existing cards → existingName=null and you may assign a newCategory.
3) Invent at most ${maxCategories} NEW broad Russian names (1–2 Cyrillic words), reusable types.
4) Every newCategories[].exampleIds MUST be events you also marked with that newCategory in assignments.
5) Synonyms of existing are forbidden (Концерты≈Музыка, Кино≈ only if no cinema category exists yet as «Фильмы»).
6) Junk (Cancelled, VIP, Premium, Fans, pure venue names) → both null, no new category.
7) keywords: 4–10 RU+EN stems per new category.

JSON only:
{
  "assignments":[{"id":"...","existingName":"Музыка|null","newCategory":"Фильмы|null"}],
  "newCategories":[{"name":"Фильмы","exampleIds":["..."],"keywords":[{"word":"фильм","value":3},{"word":"movie","value":2}]}]
}`;

  const userContent = JSON.stringify(sample.map((e) => ({
    id: e.tempId,
    name: (e.name || '').slice(0, NAME_MAX),
    description: (e.description || '').slice(0, 140),
  })));

  const { content, usage } = await callOpenAi(
    systemPrompt,
    userContent,
    '{"assignments":[],"newCategories":[]}',
  );

  let parsed;
  try {
    parsed = JSON.parse(content);
  } catch (e) {
    logger.error(`proposeCategories parse failed: ${e.message}`);
    throw new Error('Failed to parse proposeCategories AI response');
  }

  const assignments = Array.isArray(parsed?.assignments) ? parsed.assignments : [];
  const rawNew = Array.isArray(parsed?.newCategories) ? parsed.newCategories : [];

  // Count cluster size from assignments
  const hitsByNewKey = new Map();
  for (const a of assignments) {
    if (a?.existingName) continue;
    const nk = normalizeCategoryKey(a?.newCategory || '');
    if (!nk || existingKeySet.has(nk)) continue;
    hitsByNewKey.set(nk, (hitsByNewKey.get(nk) || 0) + 1);
  }

  const categories = [];
  for (const row of rawNew.slice(0, maxCategories)) {
    const name = String(row?.name || '').trim();
    const key = normalizeCategoryKey(name);
    if (!name || !key || existingKeySet.has(key)) continue;
    if (!/\p{Script=Cyrillic}/u.test(name)) continue;
    if ((name.match(/[A-Za-z]/g) || []).length > (name.match(/\p{Script=Cyrillic}/gu) || []).length) {
      continue;
    }

    const exampleIds = Array.isArray(row.exampleIds) ? row.exampleIds.map(String) : [];
    const examples = [];
    for (const id of exampleIds) {
      const ev = byId.get(id);
      if (!ev?.name) continue;
      const asg = assignments.find((a) => String(a.id) === id);
      if (!asg) continue;
      if (asg.existingName) continue;
      const claimed = normalizeCategoryKey(asg.newCategory || '');
      if (claimed !== key) continue;
      examples.push(String(ev.name).slice(0, 160));
      if (examples.length >= 8) break;
    }
    // Fallback: pull examples from assignments if exampleIds incomplete
    if (!examples.length) {
      for (const a of assignments) {
        if (normalizeCategoryKey(a?.newCategory || '') !== key) continue;
        if (a.existingName) continue;
        const ev = byId.get(String(a.id));
        if (!ev?.name) continue;
        examples.push(String(ev.name).slice(0, 160));
        if (examples.length >= 8) break;
      }
    }
    if (!examples.length) continue;

    const keywords = Array.isArray(row.keywords)
      ? row.keywords.map((k) => ({
        word: String(k?.word || '').trim().toLowerCase(),
        value: Number(k?.value) || 1,
      })).filter((k) => k.word)
      : [];

    categories.push({
      name,
      sources: [],
      keywords,
      examples,
      hit_count: hitsByNewKey.get(key) || examples.length,
    });
  }

  logger.info(`proposeCategories → ${categories.length} (max=${maxCategories})`);
  return {
    categories,
    assignments,
    usage: {
      prompt_tokens: usage.prompt_tokens || 0,
      completion_tokens: usage.completion_tokens || 0,
      total_tokens: usage.total_tokens || (
        (usage.prompt_tokens || 0) + (usage.completion_tokens || 0)
      ),
    },
  };
}

export default {
  rebuildAiPromptIfNeeded,
  categorizeEventsWithAi,
  proposeCategoriesFromEvents,
  computeCategoriesHash,
};
