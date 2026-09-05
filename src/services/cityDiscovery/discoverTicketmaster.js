import { ENV, TICKETMASTER_COUNTRY_CODES } from '../../helpers/constants';
import { isGarbageCityName } from '../../helpers/cityDiscoveryNormalize';
import { requestJson } from './http';

const DISCOVERY_BASE = 'https://app.ticketmaster.com/discovery/v2';
const PAGE_SIZE = 200;
const MAX_PAGES_PER_COUNTRY = 3;
const DELAY_MS = 200;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Venue cities via Discovery venues API — not event scrape.
 */
export default async function discoverTicketmasterCities({ countryCodes } = {}) {
  const apiKey = ENV.TICKETMASTER_API_KEY;
  if (!apiKey) {
    throw new Error('TICKETMASTER_API_KEY is not set');
  }

  const codes = Array.isArray(countryCodes) && countryCodes.length
    ? countryCodes.map((c) => String(c).toUpperCase())
    : TICKETMASTER_COUNTRY_CODES;

  const counts = new Map();
  const byCountry = {};

  for (const countryCode of codes) {
    let page = 0;
    let found = 0;
    while (page < MAX_PAGES_PER_COUNTRY) {
      const url = `${DISCOVERY_BASE}/venues.json?apikey=${apiKey}&countryCode=${countryCode}&size=${PAGE_SIZE}&page=${page}`;
      // eslint-disable-next-line no-await-in-loop
      const { statusCode, data } = await requestJson(url);
      if (statusCode !== 200) {
        break;
      }
      const venues = data?._embedded?.venues || [];
      if (!venues.length) break;

      for (const v of venues) {
        const city = String(v?.city?.name || '').trim();
        if (isGarbageCityName(city)) continue;
        counts.set(city, (counts.get(city) || 0) + 1);
        found += 1;
      }

      const totalPages = data?.page?.totalPages ?? 1;
      page += 1;
      if (page >= totalPages) break;
      // eslint-disable-next-line no-await-in-loop
      await sleep(DELAY_MS);
    }
    byCountry[countryCode] = found;
    // eslint-disable-next-line no-await-in-loop
    await sleep(DELAY_MS);
  }

  const candidates = [...counts.entries()]
    .map(([raw_name, hit_count]) => ({
      raw_name,
      slug: '',
      source_url: DISCOVERY_BASE,
      hit_count,
    }))
    .sort((a, b) => a.raw_name.localeCompare(b.raw_name, 'en'));

  return {
    candidates,
    meta: {
      method: 'ticketmaster_venues_api',
      countryCodes: codes,
      maxPagesPerCountry: MAX_PAGES_PER_COUNTRY,
      venuesSeenByCountry: byCountry,
      uniqueCities: candidates.length,
    },
  };
}
