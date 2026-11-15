import { requestText } from '../services/cityDiscovery/http';

const decodeEntities = (s = '') => String(s)
  .replace(/&nbsp;/gi, ' ')
  .replace(/&amp;/gi, '&')
  .replace(/&quot;/gi, '"')
  .replace(/&#39;|&apos;/gi, "'")
  .replace(/&lt;/gi, '<')
  .replace(/&gt;/gi, '>')
  .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
  .replace(/&#x([0-9a-f]+);/gi, (_, n) => String.fromCharCode(parseInt(n, 16)));

const metaContent = (html, name) => {
  const re1 = new RegExp(
    `<meta[^>]*(?:name|property)=["']${name}["'][^>]*content=["']([^"']*)["']`,
    'i',
  );
  const re2 = new RegExp(
    `<meta[^>]*content=["']([^"']*)["'][^>]*(?:name|property)=["']${name}["']`,
    'i',
  );
  const m = html.match(re1) || html.match(re2);
  return m ? decodeEntities(m[1]).trim() : '';
};

const stripToText = (html = '') => {
  let t = String(html);
  t = t.replace(/<script[\s\S]*?<\/script>/gi, ' ');
  t = t.replace(/<style[\s\S]*?<\/style>/gi, ' ');
  t = t.replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ');
  t = t.replace(/<!--[\s\S]*?-->/g, ' ');
  t = t.replace(/<[^>]+>/g, ' ');
  t = decodeEntities(t);
  t = t.replace(/\s+/g, ' ').trim();
  return t;
};

/**
 * Fetch event page and extract grounded facts for AI description.
 * Prefers og/meta description + page title; supplements with body text.
 */
export async function fetchEventPageFacts(url, { timeoutMs = 25000, maxBody = 1800 } = {}) {
  if (!url || !/^https?:\/\//i.test(String(url))) {
    return { ok: false, url: '', title: '', metaDescription: '', bodyText: '', facts: '' };
  }

  try {
    const res = await requestText(String(url), {
      timeoutMs,
      headers: {
        Accept: 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru,uk,he,en;q=0.8',
      },
    });
    if (res.statusCode < 200 || res.statusCode >= 400) {
      return {
        ok: false,
        url: String(url),
        title: '',
        metaDescription: '',
        bodyText: '',
        facts: '',
        statusCode: res.statusCode,
      };
    }

    const html = res.text || '';
    const titleMatch = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
    const title = decodeEntities((titleMatch?.[1] || '').replace(/\s+/g, ' ').trim());
    const metaDescription = metaContent(html, 'og:description')
      || metaContent(html, 'description')
      || metaContent(html, 'twitter:description');

    let bodyChunk = '';
    const article = html.match(/<article[^>]*>([\s\S]*?)<\/article>/i);
    const entry = html.match(
      /<(?:div|section)[^>]*class=["'][^"']*(?:entry-content|event-content|event-description|post-content|content-area)[^"']*["'][^>]*>([\s\S]*?)<\/(?:div|section)>/i,
    );
    bodyChunk = stripToText((article?.[1] || entry?.[1] || '').slice(0, 12000));
    if (bodyChunk.length < 80) {
      bodyChunk = stripToText(html).slice(0, maxBody + 400);
    }
    bodyChunk = bodyChunk.slice(0, maxBody);

    const parts = [];
    if (title) parts.push(`page_title: ${title}`);
    if (metaDescription) parts.push(`page_description: ${metaDescription}`);
    if (bodyChunk) parts.push(`page_text: ${bodyChunk}`);
    const facts = parts.join('\n').slice(0, 2800);

    return {
      ok: Boolean(metaDescription || bodyChunk.length > 60),
      url: String(url),
      title,
      metaDescription,
      bodyText: bodyChunk,
      facts,
      statusCode: res.statusCode,
    };
  } catch (e) {
    return {
      ok: false,
      url: String(url),
      title: '',
      metaDescription: '',
      bodyText: '',
      facts: '',
      error: e?.message || String(e),
    };
  }
}

export default { fetchEventPageFacts };
