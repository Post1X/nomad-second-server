import https from 'https';
import { ENV } from './constants';

/**
 * OpenAI Responses API + web_search — gather factual notes about an event.
 */
export async function webSearchEventFacts({ name, url, address } = {}) {
  const apiKey = ENV.OPENAI_API_KEY;
  if (!apiKey) throw new Error('OPENAI_API_KEY is not set');

  const queryParts = [];
  if (url) queryParts.push(`Official page: ${url}`);
  queryParts.push(`Event: ${name || ''}`);
  if (address) queryParts.push(`Venue/address hint: ${address}`);
  queryParts.push(
    'What KIND of event is this (stand-up comedy, music concert, theatre play, '
    + 'lecture, film screening, kids show, etc.)? Who performs? Extract ONLY '
    + 'verifiable facts. Prefer the official URL. Ignore ticket prices.',
  );

  const body = JSON.stringify({
    model: ENV.OPENAI_WEB_MODEL || ENV.OPENAI_MODEL || 'gpt-4o-mini',
    temperature: 0,
    tools: [{ type: 'web_search_preview' }],
    input: [
      {
        role: 'system',
        content: 'You gather factual background for event categorization and descriptions. '
          + 'Return JSON only: {"facts":"plain text focusing on event TYPE and artists","sources":["url"]}. '
          + 'If the artist is a comedian / humorist / stand-up — say so explicitly. '
          + 'If unsure, facts="".',
      },
      {
        role: 'user',
        content: queryParts.join('\n'),
      },
    ],
  });

  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.openai.com',
      path: '/v1/responses',
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
            reject(new Error(`OpenAI Responses HTTP ${res.statusCode}: ${data.slice(0, 500)}`));
            return;
          }
          const parsed = JSON.parse(data);
          const texts = [];
          for (const item of parsed.output || []) {
            if (item.type !== 'message') continue;
            for (const c of item.content || []) {
              if (c.type === 'output_text' && c.text) texts.push(c.text);
            }
          }
          const joined = texts.join('\n').trim();
          let facts = joined;
          try {
            const json = JSON.parse(joined);
            if (json?.facts != null) facts = String(json.facts);
          } catch (e) {
            // keep raw
          }
          resolve({
            facts: String(facts || '').slice(0, 2200),
            usage: parsed.usage || {},
            raw: joined.slice(0, 500),
          });
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('OpenAI Responses timeout (180s)'));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

export default { webSearchEventFacts };
