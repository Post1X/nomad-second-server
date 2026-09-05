import https from 'https';
import { ENV } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';
import {
  hasSufficientTicketmasterText,
  pickTicketmasterBodyText,
  TM_MIN_DESCRIPTION_LENGTH,
} from '../helpers/ticketmasterDescription';

// re-export for callers / tests
export { TM_MIN_DESCRIPTION_LENGTH, hasSufficientTicketmasterText };

const logger = createLoggerWithSource('ENRICH_TM');
const DISCOVERY_BASE = 'https://app.ticketmaster.com/discovery/v2';

const fetchJson = (url, headers = {}) => new Promise((resolve, reject) => {
  const req = https.get(url, { headers }, (res) => {
    let data = '';
    res.on('data', (c) => { data += c; });
    res.on('end', () => {
      try {
        const parsed = JSON.parse(data || '{}');
        if (res.statusCode >= 400) {
          reject(new Error(
            parsed?.fault?.faultstring
            || parsed?.errors?.[0]?.message
            || parsed?.errors?.[0]?.detail
            || `HTTP ${res.statusCode}`,
          ));
          return;
        }
        resolve(parsed);
      } catch (e) {
        reject(e);
      }
    });
  });
  req.on('error', reject);
  req.setTimeout(15000, () => {
    req.destroy(new Error('timeout'));
  });
});

/** Body text only (no name fallback) — name-only is treated as missing. */
const pickDescription = (event) => pickTicketmasterBodyText(event);

const pickPrices = (event) => {
  const ranges = event?.priceRanges || [];
  if (!ranges.length) return { min_price: null, max_price: null, currency: null };
  const mins = ranges.map((r) => r.min).filter((n) => typeof n === 'number');
  const maxs = ranges.map((r) => r.max).filter((n) => typeof n === 'number');
  const currency = ranges.find((r) => r.currency)?.currency || null;
  return {
    min_price: mins.length ? Math.min(...mins) : null,
    max_price: maxs.length ? Math.max(...maxs) : null,
    currency,
  };
};

/** Biletix URL: /performance/{eventCode}/{performanceCode}/TURKIYE/tr */
const parseBiletixUrl = (website) => {
  const m = String(website || '').match(/biletix\.com\/performance\/([^/]+)\/([^/]+)/i);
  if (!m) return null;
  return { eventCode: m[1], performanceCode: m[2] };
};

/**
 * TR Discovery often omits priceRanges; Biletix API exposes minPrice in kuruş (88000 → 880 TRY).
 */
const pickPricesFromBiletix = async (website) => {
  const parsed = parseBiletixUrl(website);
  if (!parsed) return null;
  const apiUrl = `https://www.biletix.com/wbtxapi/api/v1/bxcached/event/getPerformanceList/${encodeURIComponent(parsed.eventCode)}/INTERNET/tr`;
  const json = await fetchJson(apiUrl, {
    Accept: 'application/json',
    'User-Agent': 'Mozilla/5.0 (compatible; NomadEnrich/1.0)',
    Referer: website,
  });
  const list = Array.isArray(json?.data) ? json.data : [];
  if (!list.length) return null;
  const row = list.find((p) => String(p.performanceCode) === String(parsed.performanceCode))
    || list[0];
  const raw = row?.minPrice;
  if (typeof raw !== 'number' || raw <= 0) return null;
  // Biletix stores minor units (kuruş); values like 880/900 are already major.
  const major = raw >= 1000 ? raw / 100 : raw;
  return {
    min_price: major,
    max_price: major,
    currency: 'TRY',
    price_source: 'biletix',
  };
};

const pickDates = (event) => {
  const start = event?.dates?.start || {};
  const end = event?.dates?.end || {};
  const localDate = start.localDate || null;
  const localTime = start.localTime || null;
  const dateStart = start.dateTime
    ? new Date(start.dateTime)
    : (localDate ? new Date(`${localDate}T${localTime || '00:00:00'}`) : null);
  const dateEnd = end.dateTime
    ? new Date(end.dateTime)
    : (end.localDate ? new Date(`${end.localDate}T${end.localTime || '00:00:00'}`) : null);
  let holdingDate = '';
  if (localDate) {
    holdingDate = localTime ? `${localDate} ${localTime.slice(0, 5)}` : localDate;
  } else if (start.dateTime) {
    holdingDate = start.dateTime;
  }
  return {
    holding_date: holdingDate,
    date_start: dateStart && !Number.isNaN(dateStart.getTime()) ? dateStart.toISOString() : null,
    date_end: dateEnd && !Number.isNaN(dateEnd.getTime()) ? dateEnd.toISOString() : null,
    timezone: event?.dates?.timezone || '',
  };
};

/**
 * Enrich one or many events from Ticketmaster Discovery by ticketmaster_id.
 * @param {Array<{ event_id?: string, ticketmaster_id: string, description?: string }>} items
 */
export async function enrichFromTicketmaster(items = []) {
  const apiKey = ENV.TICKETMASTER_API_KEY;
  if (!apiKey) {
    throw new Error('TICKETMASTER_API_KEY is not set');
  }

  const list = Array.isArray(items) ? items : [];
  const results = [];

  for (const item of list) {
    const tmId = item.ticketmaster_id || item.ticketmasterId;
    const eventId = item.event_id || item.eventId || item.id || null;
    if (!tmId) {
      results.push({
        event_id: eventId,
        found: false,
        reason: 'missing_ticketmaster_id',
      });
      continue;
    }

    try {
      const url = `${DISCOVERY_BASE}/events/${encodeURIComponent(tmId)}.json?apikey=${apiKey}`;
      // eslint-disable-next-line no-await-in-loop
      const event = await fetchJson(url);
      const description = pickDescription(event);
      let { min_price, max_price, currency } = pickPrices(event);
      let priceSource = (min_price != null || max_price != null) ? 'discovery' : null;
      const website = event.url || '';
      if (min_price == null && max_price == null && /biletix\.com/i.test(website)) {
        try {
          // eslint-disable-next-line no-await-in-loop
          const fromBx = await pickPricesFromBiletix(website);
          if (fromBx) {
            min_price = fromBx.min_price;
            max_price = fromBx.max_price;
            currency = fromBx.currency;
            priceSource = fromBx.price_source;
          }
        } catch (e) {
          logger.warn(`Biletix price fallback failed url=${website}: ${e.message || e}`);
        }
      }
      const dates = pickDates(event);
      const current = String(item.description || '').trim();
      const enriched = Boolean(description && (
        !current
        || (description !== current && description.length > current.length + 20)
      ));

      results.push({
        event_id: eventId,
        ticketmaster_id: tmId,
        found: true,
        enriched,
        sufficient_description: hasSufficientTicketmasterText(event),
        name: event.name || item.name || '',
        description,
        website,
        min_price,
        max_price,
        currency,
        price_source: priceSource,
        holding_date: dates.holding_date,
        date_start: dates.date_start,
        date_end: dates.date_end,
        timezone: dates.timezone,
        pleaseNote: event.pleaseNote || '',
        info: event.info || '',
      });
    } catch (e) {
      logger.warn(`TM enrich failed id=${tmId}: ${e.message || e}`);
      results.push({
        event_id: eventId,
        ticketmaster_id: tmId,
        found: false,
        reason: e.message || 'fetch_failed',
      });
    }
  }

  logger.info(`TM enrich: ${list.length} → found ${results.filter((r) => r.found).length}, enriched ${results.filter((r) => r.enriched).length}`);
  return results;
}

export default { enrichFromTicketmaster };
