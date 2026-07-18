import crypto from 'crypto';
import https from 'https';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { AI_CATEGORY_BATCH_CHARS, ENV, SETTINGS_KEYS } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('AI_CATEGORY');

const buildSystemPrompt = (categories) => {
  const list = categories
    .filter((c) => c.name !== 'Другое')
    .map((c) => `- ${c._id}: ${c.name}`)
    .join('\n');

  return `You are an event categorization assistant.
Given a list of events, assign each event exactly one category from the list below.
Return ONLY valid JSON array of objects: [{"id":"<event temp id>","categoryId":"<category mongo id>"}].
If none fit well, omit that event or set categoryId to null.
Do not invent category ids. Use only ids from this list:

${list}`;
};

export const computeCategoriesHash = (categories) => {
  const payload = (categories || [])
    .map((c) => `${c._id}:${c.name}`)
    .sort()
    .join('|');
  return crypto.createHash('sha256').update(payload).digest('hex');
};

export async function rebuildAiPromptIfNeeded() {
  const categories = await EventsCategoriesSchema.find({}).sort({ sort: 1 }).lean();
  const hash = computeCategoriesHash(categories);
  const existingHash = await SettingsSchema.findOne({ key: SETTINGS_KEYS.categoriesHash }).lean();
  const existingPrompt = await SettingsSchema.findOne({ key: SETTINGS_KEYS.aiCategoryPrompt }).lean();

  if (existingHash?.value === hash && existingPrompt?.value) {
    return { updated: false, prompt: existingPrompt.value };
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

  logger.info(`AI category prompt rebuilt (categories=${categories.length})`);
  return { updated: true, prompt };
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
        content: `${userContent}\n\nRespond as JSON object: {"results":[{"id":"...","categoryId":"..."}]}`,
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
          resolve(content);
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
      }))
      .filter((item) => item.id);
  } catch (e) {
    logger.error(`Failed to parse AI response: ${e.message}`);
    return [];
  }
};

export async function categorizeEventsWithAi(events) {
  const map = new Map();
  if (!events?.length) return map;

  if (!ENV.OPENAI_API_KEY) {
    logger.warn('OPENAI_API_KEY missing — skip AI categorization');
    events.forEach((e) => map.set(e.tempId, null));
    return map;
  }

  const { prompt } = await rebuildAiPromptIfNeeded();
  const validCategoryIds = new Set(
    (await EventsCategoriesSchema.find({}).select('_id').lean()).map((c) => String(c._id)),
  );

  const batches = [];
  let current = [];
  let currentLen = 0;

  for (const ev of events) {
    const line = JSON.stringify({
      id: ev.tempId,
      name: (ev.name || '').slice(0, 200),
      description: (ev.description || '').slice(0, 400),
      address: (ev.address || '').slice(0, 150),
    });
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
    const userContent = batch.map((ev) => JSON.stringify({
      id: ev.tempId,
      name: (ev.name || '').slice(0, 200),
      description: (ev.description || '').slice(0, 400),
      address: (ev.address || '').slice(0, 150),
    })).join('\n');

    try {
      const content = await callOpenAi(prompt, userContent);
      const results = parseAiResults(content);
      const byId = new Map(results.map((r) => [r.id, r.categoryId]));

      for (const ev of batch) {
        let catId = byId.get(ev.tempId) || null;
        if (catId && !validCategoryIds.has(String(catId))) {
          catId = null;
        }
        map.set(ev.tempId, catId);
      }
    } catch (e) {
      logger.error(`AI batch ${i + 1}/${batches.length} failed: ${e.message}`);
      batch.forEach((ev) => map.set(ev.tempId, null));
    }
  }

  return map;
}

export default {
  rebuildAiPromptIfNeeded,
  categorizeEventsWithAi,
  computeCategoriesHash,
};
