import https from 'https';
import http from 'http';
import { URL } from 'url';
import moment from 'moment';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import OperationsSchema from '../schemas/OperationsSchema';
import { ENV, EVENT_SOURCE } from '../helpers/constants';
import findCityInDb from '../helpers/cityMatching';
import { findCountryByIso } from '../helpers/isoCountryAliases';
import saveProcessedEvents from '../helpers/saveProcessedEvents';
import { createLoggerWithSource } from '../helpers/logger';

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

const parseDatesFromText = (text = '') => {
  const dates = [];
    const block = text.match(/Дат[аы]\s*:\s*([^\n.]+?)(?:\s+Город|$)/i);
  const src = block ? block[1] : text;
  const re = /(\d{1,2})[./](\d{1,2})[./](\d{2,4})/g;
  let m;
  while ((m = re.exec(src))) {
    let year = Number(m[3]);
    if (year < 100) year += 2000;
    const d = moment(`${year}-${m[2].padStart(2, '0')}-${m[1].padStart(2, '0')}`, 'YYYY-MM-DD', true);
    if (d.isValid()) dates.push(d.toDate());
  }
  return dates;
};

const parseCitiesFromText = (text = '') => {
  const m = text.match(/Город[аы]?\s*:\s*([^\n]+?)(?:\s+Купить|\s+билеты|$)/i);
  if (!m) return [];
  return m[1]
    .split(/[,;|/]/)
    .map((s) => s.trim())
    .filter(Boolean);
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

async function parseIsraelinfo({ meta = {}, operationId }) {
  const infoTexts = [];
  const errorTexts = [];
  const events = [];

  const logProgress = async (msg) => {
    logger.info(msg);
    const op = await OperationsSchema.findById(operationId);
    await OperationsSchema.findByIdAndUpdate(operationId, {
      infoText: `${op?.infoText || ''}\n${msg}`,
    });
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

    for (const item of items) {
      const plain = stripTags(item.description);
      const dates = parseDatesFromText(plain);
      const cityNames = parseCitiesFromText(plain);
      const prices = parsePrices(plain);
      const photo = extractImg(item.description);

      let matchedCity = null;
      for (const cityName of cityNames) {
        matchedCity = findCityInDb(cities, cityName);
        if (matchedCity) break;
      }

      const cityId = meta.cityId || matchedCity?._id || null;
      const countryId = matchedCity?.country_id || defaultCountryId || null;
      const address = cityNames.join(', ') || matchedCity?.name || '';
      const dateStart = dates.length
        ? new Date(Math.min(...dates.map((d) => d.getTime())))
        : null;
      const dateEnd = dates.length
        ? new Date(Math.max(...dates.map((d) => d.getTime())))
        : null;

      const holding = dates.length
        ? dates
          .slice()
          .sort((a, b) => a - b)
          .map((d) => moment(d).format('DD.MM.YYYY'))
          .join(', ')
        : '';

      const newEvent = {
        name: item.title || 'Event',
        description: plain || item.title || '',
        specialization: meta.specialization || 'Event',
        admin_id: meta.adminId || null,
        country_id: countryId ? String(countryId) : null,
        city_id: cityId ? String(cityId) : null,
        operationId,
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

    await logProgress(`Mapped ${events.length} events`);
  } catch (e) {
    const errMsg = e?.message || 'Unknown error while parsing Israelinfo';
    errorTexts.push(errMsg);
    logger.error(errMsg, e);
    await logProgress(`FATAL ERROR: ${errMsg}`);
  }

  try {
    await saveProcessedEvents({
      operationId,
      events,
      source: EVENT_SOURCE.israelinfo,
      infoTexts,
      errorTexts,
    });
  } catch (error) {
    logger.error(`Error saving Israelinfo events: ${error.message || error}`);
  }
}

export default parseIsraelinfo;
