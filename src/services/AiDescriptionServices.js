import https from 'https';
import crypto from 'crypto';
import { ENV } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';
import { fetchEventPageFacts } from '../helpers/fetchEventPageFacts';
import { webSearchEventFacts } from '../helpers/openaiWebFacts';

const logger = createLoggerWithSource('AI_DESCRIPTION');

const NAME_MAX = 160;
const DESC_MAX = 400;
const ADDR_MAX = 160;
const BATCH = 25;
const WEB_CONCURRENCY = 3;

const SUSPICION_SYSTEM = `You review event listing descriptions for Nomad.

Mark suspicious=true ONLY when the description is empty, useless, or bad for a public page:
- empty / whitespace
- only repeats the title
- placeholder junk ("Package", "Event", "N/A", "VIP", one opaque word)
- so vague that a user learns nothing
- generic sales filler with no concrete facts about THIS event
  (e.g. "Не пропустите…", "Ожидайте вечер полного смеха…" with no program details)

Mark suspicious=false when the description (even short) is already clear enough
together with the title — do NOT rewrite good text.

JSON only:
{"results":[{"id":"...","suspicious":true|false}]}`;

const REWRITE_SYSTEM = `You write event descriptions for Nomad (public event cards).

Target style — medium listing blurb, like other Nomad events:
- 2–4 sentences, roughly 120–450 characters
- warm, readable, inviting
- plain text only (no HTML/markdown/bullets)
- same language as the inputs (prefer RU if title/description are Russian)

Rules:
1) Use ONLY facts from name, description, and address. You may gently generalize
   the format from the title (шоу / концерт / балет / спектакль / встреча / тур).
2) Do NOT invent artists, plot details, dates, prices, guests, or venue lore
   that are not in the inputs.
3) Do NOT return the title alone or a near-copy of the title.
4) Soft mood words are OK — hard sales spam is not.
5) If you truly cannot understand what the event is — return description=null
   (original text will be kept).

JSON only:
{"results":[{"id":"...","description":"string|null"}]}`;

/** Grounded rewrite when we have page/web facts. */
const REWRITE_GROUNDED_SYSTEM = `You write factual event descriptions for Nomad public cards.

You receive:
- name, address (may be incomplete)
- optional original listing description
- SOURCE FACTS from the official event page and/or web search

Goal: a precise medium blurb (2–4 sentences, ~120–450 chars), plain text, same
language as the event (prefer RU for RU titles).

HARD RULES (accuracy first):
1) Use ONLY information present in SOURCE FACTS / name / address / original description.
2) Never invent cast, plot, songs, guests, awards, years, prices, or venue history.
3) Prefer concrete program facts from SOURCE FACTS over marketing fluff.
4) If SOURCE FACTS conflict with the title, trust SOURCE FACTS for details but keep the titled artist/show.
5) Do NOT copy ticket CTAs ("Купить билеты", "Заказать").
6) Do NOT start with "Не пропустите" / "Ожидайте вечер".
7) This card is for ONE venue (see address). Do NOT copy dates/cities of other tour stops
   unless SOURCE FACTS explicitly state that same date/city.
8) If SOURCE FACTS are too thin to say anything true beyond the title — return description=null.

JSON only:
{"description":"string|null","used_web":true|false,"confidence":0-100}`;

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
      {
        role: 'user',
        content: `${userContent}\n\nJSON: ${jsonHint}`,
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

/** Responses API + web_search for missing page facts — see helpers/openaiWebFacts.js */

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

const eventWebsite = (event) => {
  const raw = event?.contacts?.website
    || event?.website
    || event?.url
    || '';
  return String(raw || '').trim();
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
    website: eventWebsite(event),
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
  // chat completions
  if (usage.prompt_tokens != null || usage.completion_tokens != null) {
    total.prompt_tokens += Number(usage.prompt_tokens) || 0;
    total.completion_tokens += Number(usage.completion_tokens) || 0;
    total.total_tokens += Number(usage.total_tokens)
      || ((Number(usage.prompt_tokens) || 0) + (Number(usage.completion_tokens) || 0));
  }
  // responses API
  if (usage.input_tokens != null || usage.output_tokens != null) {
    total.prompt_tokens += Number(usage.input_tokens) || 0;
    total.completion_tokens += Number(usage.output_tokens) || 0;
    total.total_tokens += (Number(usage.input_tokens) || 0) + (Number(usage.output_tokens) || 0);
  }
  total.batches += 1;
};

const isBadRewrite = (text, name) => {
  if (!text) return true;
  const t = String(text).trim();
  if (!t || t.length < 80) return true;
  if (t === name || t.toLowerCase() === String(name || '').toLowerCase()) return true;
  return false;
};

const mapPool = async (items, concurrency, fn) => {
  const results = new Array(items.length);
  let idx = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (idx < items.length) {
      const cur = idx;
      idx += 1;
      // eslint-disable-next-line no-await-in-loop
      results[cur] = await fn(items[cur], cur);
    }
  });
  await Promise.all(workers);
  return results;
};

/**
 * Grounded rewrite for one event: scrape official URL, optional web_search, then GPT.
 */
export async function rewriteDescriptionWithWeb(event, { forceWebSearch = false } = {}) {
  const c = compact(event);
  let page = { ok: false, facts: '', metaDescription: '', url: c.website };
  let usedWeb = false;
  const usageAcc = {
    prompt_tokens: 0,
    completion_tokens: 0,
    total_tokens: 0,
    batches: 0,
  };

  if (c.website) {
    page = await fetchEventPageFacts(c.website);
  }

  let webFacts = '';
  const pageFactLen = String(page.metaDescription || '').length
    + String(page.bodyText || '').length;
  const needWeb = forceWebSearch
    || !page.ok
    || pageFactLen < 60;

  if (needWeb) {
    try {
      const web = await webSearchEventFacts({
        name: c.name,
        url: c.website || undefined,
        address: c.address || undefined,
      });
      webFacts = web.facts || '';
      usedWeb = Boolean(webFacts);
      addUsage(usageAcc, web.usage);
    } catch (e) {
      logger.warn(`web_search failed for "${c.name}": ${e.message || e}`);
    }
  }

  const sourceFacts = [
    page.facts || '',
    webFacts ? `web_facts:\n${webFacts}` : '',
  ].filter(Boolean).join('\n\n').trim();

  if (!sourceFacts && !c.originalDescription) {
    return {
      description: null,
      usedWeb,
      confidence: 0,
      pageOk: page.ok,
      usage: usageAcc,
      sourceFacts: '',
    };
  }

  const userContent = JSON.stringify({
    name: c.name.slice(0, NAME_MAX),
    address: c.address || undefined,
    original_description: c.originalDescription
      ? c.originalDescription.slice(0, DESC_MAX)
      : undefined,
    source_url: c.website || undefined,
    SOURCE_FACTS: sourceFacts.slice(0, 2800),
  });

  const { content, usage } = await callOpenAi(
    REWRITE_GROUNDED_SYSTEM,
    userContent,
    '{"description":null,"used_web":false,"confidence":0}',
    0.2,
  );
  addUsage(usageAcc, usage);

  let parsed = {};
  try {
    parsed = JSON.parse(content);
  } catch (e) {
    parsed = {};
  }
  const text = parsed.description == null ? null : String(parsed.description).trim();
  const confidence = Number(parsed.confidence);
  const bad = isBadRewrite(text, c.name);

  return {
    description: bad ? null : text,
    usedWeb,
    confidence: Number.isFinite(confidence) ? confidence : (bad ? 0 : 50),
    pageOk: page.ok,
    pageMeta: page.metaDescription || '',
    usage: usageAcc,
    sourceFacts,
  };
}

/**
 * Bad/empty descriptions → try rewrite.
 * If event has website (e.g. Showman) → scrape page + optional web_search (grounded).
 * Else → legacy batch rewrite from name/description only.
 *
 * Mutates events in place.
 */
export async function enrichEventDescriptions(events, options = {}) {
  const forceAll = options.forceAll === true;
  const stats = {
    checked: 0,
    emptyOrCopy: 0,
    markedSuspicious: 0,
    rewritten: 0,
    rewrittenWeb: 0,
    leftAsIs: 0,
    pageFetched: 0,
    webSearched: 0,
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
    } else if (forceAll) {
      needRewrite.add(row.tempId);
    } else {
      needSuspicionCheck.push(row);
    }
  }

  if (!forceAll) {
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
  }

  const toRewrite = withIds.filter((r) => needRewrite.has(r.tempId));
  const withSite = toRewrite.filter((r) => r.website);
  const withoutSite = toRewrite.filter((r) => !r.website);
  const rewrittenMap = new Map();

  await mapPool(withSite, WEB_CONCURRENCY, async (row) => {
    try {
      const result = await rewriteDescriptionWithWeb(row.ev);
      addUsage(stats.openaiUsage, result.usage);
      if (result.pageOk) stats.pageFetched += 1;
      if (result.usedWeb) stats.webSearched += 1;
      rewrittenMap.set(row.tempId, result.description);
      if (result.description) {
        row.ev.description_resolved_by = result.usedWeb ? 'ai_web' : 'ai_page';
        row.ev.description_confidence = result.confidence;
      }
    } catch (e) {
      stats.openaiUsage.failedBatches += 1;
      logger.error(`Web-grounded rewrite failed for "${row.name}": ${e.message || e}`);
      rewrittenMap.set(row.tempId, null);
    }
  });

  for (const part of chunk(withoutSite, BATCH)) {
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
        rewrittenMap.set(id, isBadRewrite(text, row?.name) ? null : text);
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
      if (!row.ev.description_resolved_by) row.ev.description_resolved_by = 'ai';
      stats.rewritten += 1;
      if (row.ev.description_resolved_by === 'ai_web' || row.ev.description_resolved_by === 'ai_page') {
        stats.rewrittenWeb += 1;
      }
    } else {
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
    + `webGrounded=${stats.rewrittenWeb} page=${stats.pageFetched} `
    + `webSearch=${stats.webSearched} leftAsIs=${stats.leftAsIs}`,
  );

  return { events, stats };
}

export default { enrichEventDescriptions, rewriteDescriptionWithWeb };
