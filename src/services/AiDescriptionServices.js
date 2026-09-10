import https from 'https';
import crypto from 'crypto';
import { ENV } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('AI_DESCRIPTION');

const NAME_MAX = 160;
const DESC_MAX = 400;
const ADDR_MAX = 160;
const BATCH = 25;

const SUSPICION_SYSTEM = `You review event listing descriptions for Nomad.

Mark suspicious=true ONLY when the description is empty, useless, or bad for a public page:
- empty / whitespace
- only repeats the title
- placeholder junk ("Package", "Event", "N/A", "VIP", one opaque word)
- so vague that a user learns nothing

Mark suspicious=false when the description (even short) is already clear enough
together with the title — do NOT rewrite good text.

JSON only:
{"results":[{"id":"...","suspicious":true|false}]}`;

const REWRITE_SYSTEM = `You write event descriptions for Nomad (public event cards).

Target style — medium listing blurb, like other Nomad events:
- 2–4 sentences, roughly 120–450 characters
- warm, readable, inviting (RU examples: «Увлекательное шоу…», «Яркий концерт…»,
  «Классический балет…», «Встреча с…»)
- plain text only (no HTML/markdown/bullets)
- same language as the inputs (prefer RU if title/description are Russian)

Rules:
1) Use ONLY facts from name, description, and address. You may gently generalize
   the format from the title (шоу / концерт / балет / спектакль / встреча / тур).
2) Do NOT invent artists, plot details, dates, prices, guests, or venue lore
   that are not in the inputs.
3) Do NOT return the title alone or a near-copy of the title.
4) Soft mood words are OK («увлекательное», «яркий», «атмосферный») —
   hard sales spam is not («Не пропустите!!!», «лучший в мире»).
5) If you truly cannot understand what the event is — return description=null
   (original text will be kept).

JSON only:
{"results":[{"id":"...","description":"string|null"}]}`;

const callOpenAi = async (systemPrompt, userContent, jsonHint, temperature = 0.1) => {
  const apiKey = ENV.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY is not set');
  }

  const body = JSON.stringify({
    model: ENV.OPENAI_MODEL || 'gpt-4o-mini',
    temperature,
    response_format: { type: 'json_object' },
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: `${userContent}\n\nJSON: ${jsonHint}` },
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
      timeout: 180000,
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
          resolve({
            content: parsed?.choices?.[0]?.message?.content || '',
            usage: parsed?.usage || {},
          });
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('OpenAI request timeout (180s)'));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
};

const parseResults = (content) => {
  try {
    const parsed = JSON.parse(content);
    const list = Array.isArray(parsed) ? parsed : (parsed.results || parsed.items || []);
    return Array.isArray(list) ? list : [];
  } catch (e) {
    logger.error(`Failed to parse AI description response: ${e.message}`);
    return [];
  }
};

const compact = (event) => {
  const name = String(event.name || '').trim();
  let description = String(event.description || '').trim();
  if (description && description === name) description = '';
  return {
    name,
    description,
    address: String(event.address || '').trim().slice(0, ADDR_MAX),
    originalDescription: String(event.description || '').trim(),
  };
};

const isEmptyOrTitleCopy = (event) => {
  const { name, description } = compact(event);
  return !description || description === name;
};

const chunk = (arr, size) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

const addUsage = (total, usage) => {
  if (!usage || typeof usage !== 'object') return;
  total.prompt_tokens += Number(usage.prompt_tokens) || 0;
  total.completion_tokens += Number(usage.completion_tokens) || 0;
  total.total_tokens += Number(usage.total_tokens) || 0;
  total.batches += 1;
};

/**
 * Bad/empty descriptions → try rewrite from name+description only.
 * If AI cannot extract anything useful → leave original description as-is.
 * Good descriptions are not touched.
 *
 * Mutates events in place.
 */
export async function enrichEventDescriptions(events) {
  const stats = {
    checked: 0,
    emptyOrCopy: 0,
    markedSuspicious: 0,
    rewritten: 0,
    leftAsIs: 0,
    openaiUsage: {
      prompt_tokens: 0,
      completion_tokens: 0,
      total_tokens: 0,
      batches: 0,
      failedBatches: 0,
    },
  };

  if (!Array.isArray(events) || !events.length) return { events: events || [], stats };
  if (!ENV.OPENAI_API_KEY) {
    logger.warn('OPENAI_API_KEY missing — skip description enrichment');
    return { events, stats };
  }

  const withIds = events.map((ev) => {
    const tempId = crypto.randomUUID();
    return { ev, tempId, ...compact(ev) };
  });
  stats.checked = withIds.length;

  const needSuspicionCheck = [];
  const needRewrite = new Set();

  for (const row of withIds) {
    if (isEmptyOrTitleCopy(row.ev)) {
      stats.emptyOrCopy += 1;
      needRewrite.add(row.tempId);
    } else {
      needSuspicionCheck.push(row);
    }
  }

  for (const part of chunk(needSuspicionCheck, BATCH)) {
    try {
      const userContent = JSON.stringify(part.map((r) => ({
        id: r.tempId,
        name: r.name.slice(0, NAME_MAX),
        description: r.description.slice(0, DESC_MAX),
        address: r.address || undefined,
      })));
      // eslint-disable-next-line no-await-in-loop
      const { content, usage } = await callOpenAi(
        SUSPICION_SYSTEM,
        userContent,
        '{"results":[{"id":"...","suspicious":false}]}',
      );
      addUsage(stats.openaiUsage, usage);
      const results = parseResults(content);
      for (const item of results) {
        const id = String(item.id || '');
        if (!id) continue;
        if (item.suspicious === true || item.suspicious === 'true') {
          needRewrite.add(id);
          stats.markedSuspicious += 1;
        }
      }
    } catch (e) {
      stats.openaiUsage.failedBatches += 1;
      logger.error(`Suspicion batch failed: ${e.message || e}`);
    }
  }

  const toRewrite = withIds.filter((r) => needRewrite.has(r.tempId));
  const rewrittenMap = new Map();

  for (const part of chunk(toRewrite, BATCH)) {
    try {
      const userContent = JSON.stringify(part.map((r) => ({
        id: r.tempId,
        name: r.name.slice(0, NAME_MAX),
        description: r.originalDescription
          ? r.originalDescription.slice(0, DESC_MAX)
          : undefined,
        address: r.address || undefined,
      })));
      // eslint-disable-next-line no-await-in-loop
      const { content, usage } = await callOpenAi(
        REWRITE_SYSTEM,
        userContent,
        '{"results":[{"id":"...","description":null}]}',
        0.4,
      );
      addUsage(stats.openaiUsage, usage);
      const results = parseResults(content);
      for (const item of results) {
        const id = String(item.id || '');
        if (!id) continue;
        const text = item.description == null ? null : String(item.description).trim();
        const row = part.find((r) => r.tempId === id);
        const name = row?.name || '';
        // Reject title-echo / too thin — keep original instead
        const bad = !text
          || text === name
          || text.toLowerCase() === name.toLowerCase()
          || text.length < 80;
        rewrittenMap.set(id, bad ? null : text);
      }
    } catch (e) {
      stats.openaiUsage.failedBatches += 1;
      logger.error(`Rewrite-description batch failed: ${e.message || e}`);
      for (const r of part) {
        if (!rewrittenMap.has(r.tempId)) rewrittenMap.set(r.tempId, null);
      }
    }
  }

  for (const row of toRewrite) {
    const text = rewrittenMap.has(row.tempId) ? rewrittenMap.get(row.tempId) : null;
    delete row.ev.specialization;
    if (text) {
      row.ev.description = text;
      row.ev.description_resolved_by = 'ai';
      stats.rewritten += 1;
    } else {
      // Does not understand / nothing useful → leave original as-is
      row.ev.description = row.originalDescription;
      stats.leftAsIs += 1;
    }
    delete row.ev.description_ai_failed;
    delete row.ev.needs_manual_review;
  }

  for (const row of withIds) {
    delete row.ev.specialization;
  }

  logger.info(
    `Description enrich: checked=${stats.checked} empty=${stats.emptyOrCopy} `
    + `suspicious=${stats.markedSuspicious} rewritten=${stats.rewritten} `
    + `leftAsIs=${stats.leftAsIs}`,
  );

  return { events, stats };
}

export default { enrichEventDescriptions };
