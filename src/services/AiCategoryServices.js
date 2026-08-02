import crypto from 'crypto';
import https from 'https';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { AI_CATEGORY_BATCH_CHARS, ENV, SETTINGS_KEYS } from '../helpers/constants';
import { normalizeCategoryKey } from './CategorySuggestionServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('AI_CATEGORY');

/** Bump when prompt text changes so cached Settings prompt is rebuilt. */
const AI_CATEGORY_PROMPT_VERSION = 'v9-existing-only';

const NAME_MAX = 120;
const DESC_MAX = 200;

const buildSystemPrompt = (categories) => {
  const usable = categories.filter((c) => c.name !== 'Другое');
  const list = usable.map((c) => `${c._id}:${c.name}`).join('\n');
  const namesOnly = usable.map((c) => c.name).join(', ');

  return `You categorize events for Nomad using EXISTING categories ONLY.
${list}

Names: ${namesOnly}

RULES:
1) Return categoryId from the list above if the event roughly fits. Prefer existing over null.
2) Concerts / bands / DJ / tour / ticket upgrades → Музыка (or closest music id).
3) Theatre / shows / sports / etc. → matching existing id.
4) If nothing fits → categoryId=null. Do NOT invent new category names.
5) Never invent ids.

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
    || '{"results":[{"id":"...","categoryId":null,"suggestedName":null}]}';

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
 * Assign existing EventsCategories only. Does NOT invent new category names.
 * New gaps are proposed separately via proposeCategoriesFromEvents.
 *
 * @param {Array} events
 * @param {{ persistSuggestions?: boolean }} [options] persistSuggestions ignored (always off)
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

  logger.info(`AI categorization: ${events.length} events in ${batches.length} batch(es)`);

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

      for (const ev of batch) {
        const row = byId.get(ev.tempId) || {};
        let catId = row.categoryId || null;

        if (catId && !validCategoryIds.has(String(catId))) {
          catId = null;
        }

        // Legacy model may still return suggestedName — fold into existing if possible
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
 * One-shot: look at a sample of uncategorized events and propose ≤ maxCategories
 * NEW broad RU types. Each proposed category MUST list exampleIds that truly belong to it.
 * Concerts must map to existing Музыка via assignments, not into «Фильмы».
 *
 * @param {Array<{ tempId: string, name?: string, description?: string, source?: string }>} events
 * @param {{ maxCategories?: number, existingNames?: string[] }} [options]
 */
export async function proposeCategoriesFromEvents(events, options = {}) {
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));
  const existingNames = (options.existingNames || []).filter((n) => n && n !== 'Другое');
  const sample = (events || []).slice(0, 180);

  if (!sample.length) {
    return { categories: [], assignments: [], usage: null };
  }
  if (!ENV.OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  const byId = new Map(sample.map((e) => [e.tempId, e]));

  const systemPrompt = `You analyze uncategorized Nomad events and propose a SHORT list of missing event-type categories.

Existing categories (already in DB — do NOT recreate synonyms):
${existingNames.join(', ') || '(none)'}

TASK:
1) For each event: if it fits an EXISTING category, set existingName to that exact name; else existingName=null.
2) Among events with existingName=null, invent at most ${maxCategories} NEW broad Russian category names (1–2 words, Cyrillic).
3) Every new category MUST include exampleIds of events that clearly belong to THAT type only.
   - Concerts / bands / Lakeside / Golden Circle / ticket upgrades → existingName=Музыка (if present), NEVER a new «Фильмы».
   - «Фильмы» only for real cinema (title/desc about film/movie/cinema/сеанс).
4) Drop junk (Cancelled, VIP, Premium, Fans, venues, brands) — leave existingName=null and do not create a category for them.
5) keywords: 4–10 RU+EN match words for each new category.

JSON only:
{
  "assignments":[{"id":"...","existingName":"Музыка|null","newCategory":"Фильмы|null"}],
  "newCategories":[{"name":"Фильмы","exampleIds":["..."],"keywords":[{"word":"фильм","value":3},{"word":"movie","value":2}]}]
}`;

  const userContent = JSON.stringify(sample.map((e) => ({
    id: e.tempId,
    name: (e.name || '').slice(0, NAME_MAX),
    description: (e.description || '').slice(0, 120),
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

  const existingKeySet = new Set(existingNames.map((n) => normalizeCategoryKey(n)));
  const assignments = Array.isArray(parsed?.assignments) ? parsed.assignments : [];
  const rawNew = Array.isArray(parsed?.newCategories) ? parsed.newCategories : [];

  // Build example titles only from AI-claimed exampleIds that exist in sample
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
      // Cross-check: AI must also have assigned this id to this newCategory
      const asg = assignments.find((a) => String(a.id) === id);
      const claimed = asg?.newCategory ? normalizeCategoryKey(asg.newCategory) : '';
      if (claimed && claimed !== key) continue;
      if (asg?.existingName) continue; // belongs to existing — not a new-cat example
      examples.push(String(ev.name).slice(0, 160));
      if (examples.length >= 8) break;
    }
    if (!examples.length) continue; // no verified examples → drop category

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
      hit_count: examples.length,
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

/**
 * Legacy name: consolidate pending label noise by re-proposing from their examples
 * is no longer the main path — prefer proposeCategoriesFromEvents on real events.
 * Kept for UI button: collapses pending names only (no event invent).
 */
export async function consolidateCategorySuggestionsWithAi(pending, options = {}) {
  const maxCategories = Math.max(5, Math.min(20, Number(options.maxCategories) || 20));
  const existingNames = (options.existingNames || []).filter((n) => n && n !== 'Другое');

  if (!pending?.length) {
    return { categories: [], usage: null };
  }
  if (!ENV.OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  // Treat pending rows as pseudo-events so one propose pass verifies examples
  const pseudo = [];
  for (const p of pending.slice(0, 80)) {
    for (const [idx, ex] of (p.example_events || []).slice(0, 3).entries()) {
      pseudo.push({
        tempId: `${String(p._id || p.normalized_key || p.raw_name)}:${idx}`,
        name: ex,
        description: '',
      });
    }
    if (!pseudo.length) {
      pseudo.push({
        tempId: String(p._id || p.normalized_key || p.raw_name),
        name: p.raw_name,
        description: '',
      });
    }
  }

  const { categories, usage } = await proposeCategoriesFromEvents(pseudo, {
    maxCategories,
    existingNames,
  });

  return {
    categories: categories.map((c) => ({
      name: c.name,
      sources: c.sources || [],
      keywords: c.keywords || [],
      examples: c.examples || [],
      hit_count: c.hit_count,
    })),
    usage,
  };
}

export default {
  rebuildAiPromptIfNeeded,
  categorizeEventsWithAi,
  proposeCategoriesFromEvents,
  consolidateCategorySuggestionsWithAi,
  computeCategoriesHash,
};
