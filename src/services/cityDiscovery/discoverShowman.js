import { ENV } from '../../helpers/constants';
import { isGarbageCityName } from '../../helpers/cityDiscoveryNormalize';
import { requestText } from './http';

const DEFAULT_FEED_URL = 'https://showman.co.il/wp-content/uploads/feeds/afisha.xml';

const cdata = (block = '', tag) => {
  const re = new RegExp(
    `<${tag}>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([\\s\\S]*?))</${tag}>`,
    'i',
  );
  const m = String(block).match(re);
  if (!m) return '';
  return String(m[1] != null ? m[1] : m[2] || '').trim();
};

export default async function discoverShowmanCities() {
  const feedUrl = ENV.SHOWMAN_FEED_URL || DEFAULT_FEED_URL;
  const res = await requestText(feedUrl, {
    headers: { Accept: 'application/xml, text/xml, */*' },
  });
  if (res.statusCode !== 200) {
    throw new Error(`Showman feed HTTP ${res.statusCode}`);
  }

  const counts = new Map();
  const dateRe = /<date\b[^>]*>([\s\S]*?)<\/date>/gi;
  let m;
  while ((m = dateRe.exec(res.text))) {
    const venueBlock = (m[1].match(/<venue>([\s\S]*?)<\/venue>/i) || [])[1] || '';
    const city = cdata(venueBlock, 'city');
    if (!city || isGarbageCityName(city)) continue;
    counts.set(city, (counts.get(city) || 0) + 1);
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
      method: 'showman_afisha_xml',
      feedUrl,
      uniqueCities: candidates.length,
    },
  };
}
