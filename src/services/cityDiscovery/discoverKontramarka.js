import { requestText } from './http';
import { isGarbageCityName } from '../../helpers/cityDiscoveryNormalize';

const HOMEPAGE = 'https://www.kontramarka.de/';

/**
 * City list from homepage only — no event pages.
 * Regex parse (avoid cheerio/undici on Node 18).
 */
export default async function discoverKontramarkaCities() {
  const res = await requestText(HOMEPAGE, {
    headers: { Accept: 'text/html,application/xhtml+xml' },
  });
  if (res.statusCode !== 200) {
    throw new Error(`Kontramarka homepage HTTP ${res.statusCode}`);
  }

  const byKey = new Map();
  const re = /href="(\/city\/([^"/]+)\/?)"[^>]*>([^<]{1,120})</gi;
  let m;
  while ((m = re.exec(res.text))) {
    const href = m[1];
    let slug = m[2];
    try {
      slug = decodeURIComponent(slug);
    } catch (e) {
      /* keep raw */
    }
    const label = String(m[3] || '').replace(/\s+/g, ' ').trim();
    const raw_name = label || slug.replace(/-/g, ' ');
    if (isGarbageCityName(raw_name)) continue;

    const source_url = `https://www.kontramarka.de${href.startsWith('/') ? '' : '/'}${href}`;
    const key = `${slug}::${raw_name.toLowerCase()}`;
    if (!byKey.has(key)) {
      byKey.set(key, { raw_name, slug, source_url });
    }
  }

  const candidates = [...byKey.values()].sort((a, b) => a.raw_name.localeCompare(b.raw_name, 'de'));
  return {
    candidates,
    meta: {
      method: 'homepage_city_links',
      homepage: HOMEPAGE,
      htmlBytes: res.text.length,
      linksFound: candidates.length,
    },
  };
}
