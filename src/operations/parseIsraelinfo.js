import https from 'https';
import http from 'http';
import { URL } from 'url';
import moment from 'moment';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import { ENV, EVENT_SOURCE } from '../helpers/constants';
import findCityInDb from '../helpers/cityMatching';
import { findCountryByIso } from '../helpers/isoCountryAliases';
import saveProcessedEvents from '../helpers/saveProcessedEvents';
import { logParseRun } from '../helpers/logParseRun';
import { createLoggerWithSource } from '../helpers/logger';
import createCitySuggestionCollector from '../helpers/createCitySuggestionCollector';
import { formatHoldingDate } from '../helpers/holdingDate';
import { parseIsraelinfoDatesFromText } from '../helpers/israelinfoDates';

const logger = createLoggerWithSource('PARSE_ISRAELINFO');

moment.locale('ru');

const PARTNERS_BASE = (ENV.ISRAELINFO_PARTNERS_URL || 'https://partners.israelinfo.co.il').replace(/\/$/, '');
const DEFAULT_FEED_HOST = 'https://nomadlifeapp.kassa.co.il';
const USER_AGENT = 'Mozilla/5.0 (compatible; NomadParser/1.0)';

const request = (urlString, {
  method = 'GET',
  headers = {},
  body = null,
  cookieJar = null,
} = {}) => new Promise((resolve, reject) => {
  const url = new URL(urlString);
  const isHttps = url.protocol === 'https:';
  const mod = isHttps ? https : http;
  const opts = {
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    method,
    headers: {
      'User-Agent': USER_AGENT,
      Accept: '*/*',
      ...headers,
    },
  };

  if (cookieJar?.cookieHeader) {
    opts.headers.Cookie = cookieJar.cookieHeader;
  }

  const req = mod.request(opts, (res) => {
    const chunks = [];
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      const buf = Buffer.concat(chunks);
      const text = buf.toString('utf8');
      const setCookie = res.headers['set-cookie'] || [];
      if (cookieJar && setCookie.length) {
        const map = cookieJar.map || {};
        for (const c of setCookie) {
          const part = String(c).split(';')[0];
          const eq = part.indexOf('=');
          if (eq > 0) map[part.slice(0, eq)] = part.slice(eq + 1);
        }
        cookieJar.map = map;
        cookieJar.cookieHeader = Object.entries(map).map(([k, v]) => `${k}=${v}`).join('; ');
      }
      resolve({
        statusCode: res.statusCode || 0,
        headers: res.headers,
        text,
        buffer: buf,
      });
    });
  });
  req.on('error', reject);
  if (body) req.write(body);
  req.end();
});

const stripTags = (html = '') => String(html)
  .replace(/<script[\s\S]*?<\/script>/gi, ' ')
  .replace(/<style[\s\S]*?<\/style>/gi, ' ')
  .replace(/<[^>]+>/g, ' ')
  .replace(/&nbsp;/gi, ' ')
  .replace(/&amp;/gi, '&')
  .replace(/&quot;/gi, '"')
  .replace(/&#39;/gi, "'")
  .replace(/\s+/g, ' ')
  .trim();

const upgradeBravoImageUrl = (src = '') => {
  let url = String(src || '').replace(/^https:https:/i, 'https:');
  if (url.startsWith('//')) url = `https:${url}`;
    url = url.replace(
    /(\/show\/image\/)(?!\d+x\d+\/)(\d+\.(?:jpe?g|png|webp))/i,
    '$1360x248/$2',
  );
    url = url.replace(
    /https?:\/\/(?:nomadlifeapp\.kassa\.co\.il|bravo\.israelinfo\.co\.il|ru\.kupatbravo\.co\.il)/i,
    'https://katalog.co.il',
  );
  return url;
};

const extractImg = (html = '') => {
  const m = String(html).match(/<img[^>]+src=["']([^"']+)["']/i);
  if (!m) return null;
  return upgradeBravoImageUrl(m[1]);
};

const parseDatesFromText = parseIsraelinfoDatesFromText;

const parseCitiesFromText = (text = '') => {
  const m = text.match(/Город[аы]?\s*:\s*([^\n]+?)(?:\s+Купить|\s+билеты|$)/i);
  if (!m) return [];
  return m[1]
    .split(/[,;|/]/)
    .map((s) => s.trim())
    .filter(Boolean);
};

/** Strip feed meta lines that are not real event copy. */
const cleanFeedDescription = (text = '') => String(text || '')
  .replace(/\s*Дат[аы]\s*:\s*.*$/i, ' ')
  .replace(/\s*Город[аы]?\s*:\s*.*$/i, ' ')
  .replace(/\s*Купить билеты[:\s].*$/i, ' ')
  .replace(/\s+/g, ' ')
  .trim();

/**
 * Venues from announce HTML (schema.org Place preferred, table data-hall fallback).
 */
const parseVenuesFromAnnounceHtml = (html = '') => {
  const venues = [];
  const seen = new Set();

  const pushVenue = ({ city = '', hall = '', street = '' } = {}) => {
    const c = String(city || '').trim();
    const h = String(hall || '').trim();
    const s = String(street || '').trim();
    if (!h && !s) return;
    const key = `${h.toLowerCase()}|${s.toLowerCase()}|${c.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    venues.push({ city: c, hall: h, street: s });
  };

  const placeRe = /itemprop="location"[^>]*itemscope[\s\S]*?<\/div>/gi;
  let placeMatch;
  while ((placeMatch = placeRe.exec(html))) {
    const block = placeMatch[0];
    pushVenue({
      hall: (block.match(/itemprop="name"\s+content="([^"]*)"/i) || [])[1] || '',
      city: (block.match(/itemprop="addressLocality"\s+content="([^"]*)"/i) || [])[1] || '',
      street: (block.match(/itemprop="streetAddress"\s+content="([^"]*)"/i) || [])[1] || '',
    });
  }

  if (!venues.length) {
    const rowRe = /<tr[^>]*data-city="([^"]*)"[^>]*>([\s\S]*?)<\/tr>/gi;
    let rowMatch;
    while ((rowMatch = rowRe.exec(html))) {
      const hallMatch = (rowMatch[2] || '').match(/data-hall="([^"]+)"/i);
      pushVenue({
        city: rowMatch[1] || '',
        hall: hallMatch ? hallMatch[1] : '',
      });
    }
  }

  return venues;
};

const formatVenueAddress = (venues = [], fallbackCity = '') => {
  if (!venues.length) return String(fallbackCity || '').trim();

  if (venues.length === 1) {
    const v = venues[0];
    return [v.hall, v.street, !v.hall && !v.street ? v.city : '']
      .filter(Boolean)
      .join(', ');
  }

  // Multi-city tour: unique halls (street makes the string huge / unstable for merge)
  const halls = [...new Set(venues.map((v) => v.hall).filter(Boolean))];
  if (halls.length) return halls.join('; ');

  return venues
    .map((v) => [v.street, v.city].filter(Boolean).join(', '))
    .filter(Boolean)
    .join('; ') || String(fallbackCity || '').trim();
};

const fetchAnnounceVenues = async (link, cache) => {
  const url = String(link || '').trim();
  if (!url) return [];
  if (cache.has(url)) return cache.get(url);
  try {
    const res = await request(url, {
      headers: { Accept: 'text/html,application/xhtml+xml' },
    });
    if (res.statusCode !== 200) {
      cache.set(url, []);
      return [];
    }
    const venues = parseVenuesFromAnnounceHtml(res.text);
    cache.set(url, venues);
    return venues;
  } catch (e) {
    logger.warn(`Announce venue fetch failed (${url}): ${e.message || e}`);
    cache.set(url, []);
    return [];
  }
};

const parsePrices = (text = '') => {
  const range = text.match(/от\s+(\d+(?:[.,]\d+)?)\s+до\s+(\d+(?:[.,]\d+)?)/i);
  if (range) {
    return { min: Number(range[1].replace(',', '.')), max: Number(range[2].replace(',', '.')) };
  }
  const single = text.match(/(\d+(?:[.,]\d+)?)\s*шек/i);
  if (single) {
    const n = Number(single[1].replace(',', '.'));
    return { min: n, max: n };
  }
  return { min: null, max: null };
};

const parseRssItems = (xml) => {
  const items = [];
  const re = /<item>([\s\S]*?)<\/item>/gi;
  let m;
  while ((m = re.exec(xml))) {
    const block = m[1];
    const title = (block.match(/<title>([\s\S]*?)<\/title>/i) || [])[1] || '';
    const link = (block.match(/<link>([\s\S]*?)<\/link>/i) || [])[1] || '';
    const descMatch = block.match(/<description>([\s\S]*?)<\/description>/i);
    let description = descMatch ? descMatch[1] : '';
    description = description
      .replace(/^<!\[CDATA\[/i, '')
      .replace(/\]\]>$/i, '')
      .trim();
    const idMatch = link.match(/announce\/(\d+)/i);
    items.push({
      title: stripTags(title),
      link: stripTags(link),
      description,
      israelinfo_id: idMatch ? idMatch[1] : null,
    });
  }
  return items;
};

async function loginPartners(login, password) {
  const cookieJar = { map: {}, cookieHeader: '' };
  const body = new URLSearchParams({
    UserName: login,
    Password: password,
  }).toString();

  const res = await request(
    `${PARTNERS_BASE}/cgi-bin/partners/auth.pl?action=login&site=2`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      },
      body,
      cookieJar,
    },
  );

  const ok = String(res.text || '').trim() === 'ok';
  if (!ok) {
    throw new Error(`Partners login failed: ${String(res.text || '').slice(0, 120)}`);
  }
  return cookieJar;
}

async function discoverFeedBase(cookieJar) {
  const body = new URLSearchParams({
    Service: 'Partners',
    Action: 'Content',
    Content: 'Dashboard',
    WasLoaded: '0',
  }).toString();

  const res = await request(`${PARTNERS_BASE}/cgi-bin/partners/index.pl`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
      'Content-Length': Buffer.byteLength(body),
    },
    body,
    cookieJar,
  });

  try {
    const json = JSON.parse(res.text);
    const services = json?.data?.services || [];
    for (const service of services) {
      const sites = service?.sites || [];
      for (const site of sites) {
        if (site?.Domain) {
          const domain = String(site.Domain).replace(/^https?:\/\//, '').replace(/\/$/, '');
          return `https://${domain}`;
        }
      }
    }
  } catch (e) {
    logger.warn(`Failed to parse dashboard JSON: ${e.message}`);
  }
  return null;
}

async function parseIsraelinfo({ meta = {}, runId }) {
  const parseRunId = runId;
  const infoTexts = [];
  const errorTexts = [];
  const events = [];
  const citySuggestions = createCitySuggestionCollector(EVENT_SOURCE.israelinfo);

  const logProgress = async (msg) => {
    logger.info(msg);
    await logParseRun(parseRunId, `[${new Date().toISOString()}] ${msg}`);
  };

  try {
    await logProgress('Starting Israelinfo/BRAVO parsing...');

    const login = ENV.ISRAELINFO_LOGIN || ENV.ISRAELINFO_USERNAME;
    const password = ENV.ISRAELINFO_PASSWORD || '';
    if (!login || !password) {
      throw new Error('ISRAELINFO_LOGIN / ISRAELINFO_PASSWORD are not set');
    }

    let feedUrl = ENV.ISRAELINFO_FEED_URL || meta.feedUrl || null;

    await logProgress(`Logging into partners portal as ${login}...`);
    const cookieJar = await loginPartners(login, password);
    infoTexts.push('Partners login OK');

    if (!feedUrl) {
      const base = await discoverFeedBase(cookieJar);
      if (base) {
        feedUrl = `${base}/xml/all.xml`;
        infoTexts.push(`Discovered partner feed host: ${base}`);
      } else {
        feedUrl = `${DEFAULT_FEED_HOST}/xml/all.xml`;
        infoTexts.push(`Partner domain not found in dashboard, using default ${DEFAULT_FEED_HOST}`);
      }
    }

    await logProgress(`Fetching feed: ${feedUrl}`);
    const feedRes = await request(feedUrl, {
      headers: { Accept: 'application/rss+xml, application/xml, text/xml, */*' },
    });
    if (feedRes.statusCode !== 200) {
      throw new Error(`Feed HTTP ${feedRes.statusCode}`);
    }

    const items = parseRssItems(feedRes.text);
    await logProgress(`Feed items: ${items.length}`);

    const cities = await CitiesSchema.find({}).lean();
    const countries = await CountriesSchema.find({}).lean();
    const israel = findCountryByIso(countries, 'IL')
      || countries.find((c) => /израил|israel/i.test(c.name || ''));
    const defaultCountryId = meta.countryId || israel?._id || null;

    // Announce pages have real venue (hall + street); RSS only lists cities.
    const venueCache = new Map();
    const CONCURRENCY = 6;
    await logProgress(`Fetching venues from announce pages (concurrency=${CONCURRENCY})...`);
    for (let i = 0; i < items.length; i += CONCURRENCY) {
      const chunk = items.slice(i, i + CONCURRENCY);
      // eslint-disable-next-line no-await-in-loop
      await Promise.all(chunk.map(async (item) => {
        const venues = await fetchAnnounceVenues(item.link, venueCache);
        // eslint-disable-next-line no-param-reassign
        item._venues = venues;
      }));
      if ((i + CONCURRENCY) % 60 === 0 || i + CONCURRENCY >= items.length) {
        const done = items.slice(0, Math.min(i + CONCURRENCY, items.length));
        const ok = done.filter((it) => (it._venues || []).length).length;
        // eslint-disable-next-line no-await-in-loop
        await logProgress(
          `Venues progress: ${done.length}/${items.length} (ok=${ok}, miss=${done.length - ok})`,
        );
      }
    }
    const venueOk = items.filter((it) => (it._venues || []).length).length;
    const venueMiss = items.length - venueOk;

    for (const item of items) {
      const plainRaw = stripTags(item.description);
      const plain = cleanFeedDescription(plainRaw) || plainRaw;
      const dates = parseDatesFromText(plainRaw);
      const cityNames = parseCitiesFromText(plainRaw);
      const prices = parsePrices(plainRaw);
      const photo = extractImg(item.description);
      const venues = item._venues || [];

      let matchedCity = null;
      const cityCandidates = [
        ...venues.map((v) => v.city).filter(Boolean),
        ...cityNames,
      ];
      for (const cityName of cityCandidates) {
        matchedCity = findCityInDb(cities, cityName);
        if (matchedCity) break;
      }

      if (!matchedCity && !meta.cityId) {
        for (const cityName of cityNames) {
          citySuggestions.note(cityName, { source_url: item.link || feedUrl || '' });
        }
      }

      const cityId = meta.cityId || matchedCity?._id || null;
      const countryId = matchedCity?.country_id || defaultCountryId || null;
      const fallbackCity = matchedCity?.name || cityNames[0] || '';
      const address = formatVenueAddress(venues, fallbackCity);
      const dateStart = dates.length
        ? new Date(Math.min(...dates.map((d) => d.getTime())))
        : null;
      const dateEnd = dates.length
        ? new Date(Math.max(...dates.map((d) => d.getTime())))
        : null;

      // Consecutive days → "2–29 августа 2026"; separate days → comma list
      const holding = dates.length ? formatHoldingDate(dates) : '';

      const newEvent = {
        name: item.title || 'Event',
        description: plain || item.title || '',
        specialization: meta.specialization || 'Event',
        admin_id: meta.adminId || null,
        country_id: countryId ? String(countryId) : null,
        city_id: cityId ? String(cityId) : null,
        contacts: { website: item.link || '' },
        photos: photo ? [{ full_url: photo }] : [],
        holding_date: holding,
        date_start: dateStart,
        date_end: dateEnd,
        source: EVENT_SOURCE.israelinfo,
        address: String(address || ''),
        _mergeDates: dates,
        israelinfo_id: item.israelinfo_id,
      };

      if (prices.min != null) newEvent.min_price = prices.min;
      if (prices.max != null) newEvent.max_price = prices.max;

      if (matchedCity?.coordinates?.lat && matchedCity?.coordinates?.lon) {
        const lat = Number(matchedCity.coordinates.lat);
        const lon = Number(matchedCity.coordinates.lon);
        if (!Number.isNaN(lat) && !Number.isNaN(lon)) {
          newEvent.lat = lat;
          newEvent.lon = lon;
          newEvent.is_special_point_on_map = true;
        }
      }

      events.push(newEvent);
    }

    await logProgress(
      `Mapped ${events.length} events (venues ok=${venueOk}, miss=${venueMiss}, fallback city only)`,
    );
  } catch (e) {
    if (e?.cancelled) throw e;
    const errMsg = e?.message || 'Unknown error while parsing Israelinfo';
    errorTexts.push(errMsg);
    logger.error(errMsg, e);
    await logProgress(`FATAL ERROR: ${errMsg}`);
  }

  let citySuggestionStats = null;
  try {
    citySuggestionStats = await citySuggestions.flush();
    if (citySuggestionStats.candidatesSeen > 0) {
      infoTexts.push(
        `CitySuggestions: +${citySuggestionStats.created} new, ${citySuggestionStats.updated} updated, `
        + `${citySuggestionStats.alreadyInDb} already in DB`,
      );
    }
  } catch (e) {
    errorTexts.push(`CitySuggestions flush failed: ${e?.message || e}`);
  }

  try {
    await saveProcessedEvents({
      runId: parseRunId,
      events,
      source: EVENT_SOURCE.israelinfo,
      infoTexts,
      errorTexts,
      extraStatistics: { citySuggestions: citySuggestionStats },
    });
  } catch (error) {
    if (error?.cancelled) throw error;
    logger.error(`Error saving Israelinfo events: ${error.message || error}`);
  }
}

export default parseIsraelinfo;
