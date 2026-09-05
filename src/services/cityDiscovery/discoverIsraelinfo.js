import { ENV } from '../../helpers/constants';
import { isGarbageCityName } from '../../helpers/cityDiscoveryNormalize';
import { requestText } from './http';

const DEFAULT_FEED_HOST = 'https://nomadlifeapp.kassa.co.il';

const stripTags = (html = '') => String(html)
  .replace(/<script[\s\S]*?<\/script>/gi, ' ')
  .replace(/<style[\s\S]*?<\/style>/gi, ' ')
  .replace(/<[^>]+>/g, ' ')
  .replace(/&nbsp;/gi, ' ')
  .replace(/&amp;/gi, '&')
  .replace(/\s+/g, ' ')
  .trim();

const parseCitiesFromText = (text = '') => {
  const m = text.match(/Город[аы]?\s*:\s*([^\n]+?)(?:\s+Купить|\s+билеты|$)/i);
  if (!m) return [];
  return m[1]
    .split(/[,;|/]/)
    .map((s) => s.trim())
    .filter(Boolean);
};

const parseRssItems = (xml) => {
  const items = [];
  const re = /<item>([\s\S]*?)<\/item>/gi;
  let m;
  while ((m = re.exec(xml))) {
    const block = m[1];
    const descMatch = block.match(/<description>([\s\S]*?)<\/description>/i);
    let description = descMatch ? descMatch[1] : '';
    description = description
      .replace(/^<!\[CDATA\[/i, '')
      .replace(/\]\]>$/i, '')
      .trim();
    items.push({ description });
  }
  return items;
};

/**
 * Unique «Город:» values from partner RSS — no ParsedEvents writes.
 */
export default async function discoverIsraelinfoCities() {
  const feedUrl = ENV.ISRAELINFO_FEED_URL || `${DEFAULT_FEED_HOST}/xml/all.xml`;
  const res = await requestText(feedUrl, {
    headers: { Accept: 'application/rss+xml, application/xml, text/xml, */*' },
  });
  if (res.statusCode !== 200) {
    throw new Error(`Israelinfo feed HTTP ${res.statusCode}`);
  }

  const items = parseRssItems(res.text);
  const counts = new Map();

  for (const item of items) {
    const plain = stripTags(item.description);
    for (const city of parseCitiesFromText(plain)) {
      if (isGarbageCityName(city)) continue;
      counts.set(city, (counts.get(city) || 0) + 1);
    }
  }

  const candidates = [...counts.entries()]
    .map(([raw_name, hit_count]) => ({
      raw_name,
      slug: '',
      source_url: feedUrl,
      hit_count,
    }))
    .sort((a, b) => a.raw_name.localeCompare(b.raw_name, 'ru'));

  return {
    candidates,
    meta: {
      method: 'israelinfo_rss_gorod',
      feedUrl,
      itemsCount: items.length,
      uniqueCities: candidates.length,
    },
  };
}
