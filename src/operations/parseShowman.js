import https from 'https';
import http from 'http';
import { URL } from 'url';
import moment from 'moment';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import { ENV, EVENT_SOURCE } from '../helpers/constants';
import findCityInDb from '../helpers/cityMatching';
import { findCountryByIso } from '../helpers/isoCountryAliases';
import saveProcessedEvents from '../helpers/saveProcessedEvents';
import { logParseRun } from '../helpers/logParseRun';
import { createLoggerWithSource } from '../helpers/logger';
import createCitySuggestionCollector from '../helpers/createCitySuggestionCollector';
import { formatHoldingDate } from '../helpers/holdingDate';

const logger = createLoggerWithSource('PARSE_SHOWMAN');

moment.locale('ru');

const DEFAULT_FEED_URL = 'https://showman.co.il/wp-content/uploads/feeds/afisha.xml';
const USER_AGENT = 'Mozilla/5.0 (compatible; NomadParser/1.0)';

const requestText = (urlString, { headers = {} } = {}) => new Promise((resolve, reject) => {
  const url = new URL(urlString);
  const isHttps = url.protocol === 'https:';
  const mod = isHttps ? https : http;
  const req = mod.request({
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    method: 'GET',
    headers: {
      'User-Agent': USER_AGENT,
      Accept: 'application/xml, text/xml, */*',
      ...headers,
    },
  }, (res) => {
    const chunks = [];
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      resolve({
        statusCode: res.statusCode || 0,
        text: Buffer.concat(chunks).toString('utf8'),
      });
    });
  });
  req.on('error', reject);
  req.setTimeout(120000, () => {
    req.destroy();
    reject(new Error('Showman feed request timeout'));
  });
  req.end();
});

const cdata = (block = '', tag) => {
  const re = new RegExp(
    `<${tag}>(?:<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>|([\\s\\S]*?))</${tag}>`,
    'i',
  );
  const m = String(block).match(re);
  if (!m) return '';
  return String(m[1] != null ? m[1] : m[2] || '').trim();
};

const attr = (tagOpen = '', name) => {
  const m = String(tagOpen).match(new RegExp(`${name}="([^"]*)"`, 'i'));
  return m ? m[1] : '';
};

const parseShowmanFeed = (xml = '') => {
  const events = [];
  const eventRe = /<event\b([^>]*)>([\s\S]*?)<\/event>/gi;
  let em;
  while ((em = eventRe.exec(xml))) {
    const eventAttrs = em[1] || '';
    const body = em[2] || '';
    const id = attr(eventAttrs, 'id') || '';
    const title = cdata(body, 'title');
    const description = cdata(body, 'description');
    const url = cdata(body, 'url');
    const image = cdata(body, 'image');
    const currency = cdata(body, 'currency') || 'ILS';
    const status = cdata(body, 'status');

    const dates = [];
    const dateRe = /<date\b([^>]*)>([\s\S]*?)<\/date>/gi;
    let dm;
    while ((dm = dateRe.exec(body))) {
      const dateAttrs = dm[1] || '';
      const dateBody = dm[2] || '';
      const venueBlock = (dateBody.match(/<venue>([\s\S]*?)<\/venue>/i) || [])[1] || '';
      const datetime = attr(dateAttrs, 'datetime') || '';
      const city = cdata(venueBlock, 'city');
      const hall = cdata(venueBlock, 'hall');
      const address = cdata(venueBlock, 'address');
      const priceMin = parseFloat(cdata(dateBody, 'price_min'));
      const priceMax = parseFloat(cdata(dateBody, 'price_max'));
      dates.push({
        id: attr(dateAttrs, 'id') || '',
        datetime,
        city,
        hall,
        address,
        price_min: Number.isFinite(priceMin) ? priceMin : null,
        price_max: Number.isFinite(priceMax) ? priceMax : null,
      });
    }

    events.push({
      id,
      title,
      description,
      url,
      image,
      currency,
      status,
      dates,
    });
  }
  return events;
};

const withPartnerParam = (url = '') => {
  const partnerId = ENV.SHOWMAN_PARTNER_ID || ENV.SHOWMAN_SM || '';
  if (!url || !partnerId) return url;
  try {
    const u = new URL(url);
    if (!u.searchParams.has('sm')) u.searchParams.set('sm', String(partnerId));
    return u.toString();
  } catch (e) {
    return url;
  }
};

const groupDatesByCity = (dates = []) => {
  const map = new Map();
  for (const d of dates) {
    const city = String(d.city || '').trim();
    if (!city) continue;
    if (!map.has(city)) map.set(city, []);
    map.get(city).push(d);
  }
  return map;
};

async function parseShowman({ meta = {}, runId }) {
  const parseRunId = runId;
  const infoTexts = [];
  const errorTexts = [];
  const events = [];
  const citySuggestions = createCitySuggestionCollector(EVENT_SOURCE.showman);

  const logProgress = async (msg) => {
    logger.info(msg);
    await logParseRun(parseRunId, `[${new Date().toISOString()}] ${msg}`);
  };

  try {
    const feedUrl = ENV.SHOWMAN_FEED_URL || meta.feedUrl || DEFAULT_FEED_URL;
    await logProgress(`Starting Showman parsing from ${feedUrl}`);

    const feedRes = await requestText(feedUrl);
    if (feedRes.statusCode !== 200) {
      throw new Error(`Showman feed HTTP ${feedRes.statusCode}`);
    }

    const feedEvents = parseShowmanFeed(feedRes.text);
    await logProgress(`Feed events: ${feedEvents.length}`);

    const cities = await CitiesSchema.find({}).lean();
    const countries = await CountriesSchema.find({}).lean();
    const israel = findCountryByIso(countries, 'IL')
      || countries.find((c) => /израил|israel/i.test(c.name || ''));
    const israelCountryId = israel?._id || null;
    const defaultCountryId = meta.countryId || israelCountryId || null;

    let skippedNoCity = 0;
    let skippedUnavailable = 0;

    for (const item of feedEvents) {
      const status = String(item.status || '').toLowerCase();
      if (status && status !== 'available' && status !== 'onsale' && status !== 'in_stock') {
        if (['soldout', 'sold_out', 'cancelled', 'canceled', 'unavailable'].includes(status)) {
          skippedUnavailable += 1;
          continue;
        }
      }

      const title = String(item.title || '').trim() || 'Event';
      const rawDesc = String(item.description || '').trim();
      const description = (!rawDesc || rawDesc === title) ? '' : rawDesc;
      const website = withPartnerParam(item.url || '');
      const photo = item.image || '';

      const byCity = groupDatesByCity(item.dates || []);
      if (!byCity.size) {
        citySuggestions.note(title, { source_url: website || feedUrl });
        skippedNoCity += 1;
        continue;
      }

      for (const [cityName, cityDates] of byCity.entries()) {
        const matchedCity = findCityInDb(cities, cityName, {
          preferCountryId: israelCountryId,
        });

        if (!matchedCity && !meta.cityId) {
          citySuggestions.note(cityName, { source_url: website || feedUrl });
          skippedNoCity += 1;
          continue;
        }

        const cityId = meta.cityId || matchedCity?._id || null;
        const countryId = matchedCity?.country_id || defaultCountryId || null;

        const parsedDates = cityDates
          .map((d) => (d.datetime ? new Date(d.datetime) : null))
          .filter((d) => d && !Number.isNaN(d.getTime()));

        const dateStart = parsedDates.length
          ? new Date(Math.min(...parsedDates.map((d) => d.getTime())))
          : null;
        const dateEnd = parsedDates.length
          ? new Date(Math.max(...parsedDates.map((d) => d.getTime())))
          : null;

        const prices = cityDates
          .flatMap((d) => [d.price_min, d.price_max])
          .filter((p) => p != null && !Number.isNaN(Number(p)))
          .map(Number);

        const hall = cityDates.map((d) => d.hall).find(Boolean) || '';
        const street = cityDates.map((d) => d.address).find(Boolean) || '';
        const address = [hall, street, cityName].filter(Boolean).join(', ');

        const newEvent = {
          name: title,
          description,
          admin_id: meta.adminId || null,
          country_id: countryId ? String(countryId) : null,
          city_id: cityId ? String(cityId) : null,
          contacts: { website },
          photos: photo ? [{ full_url: photo }] : [],
          holding_date: parsedDates.length ? formatHoldingDate(parsedDates) : '',
          holding_dates_list: parsedDates,
          date_start: dateStart,
          date_end: dateEnd,
          source: EVENT_SOURCE.showman,
          address,
          _mergeDates: parsedDates,
          parser_unique_id: item.id
            ? `showman:${item.id}:${String(cityId || cityName)}`
            : undefined,
        };

        if (prices.length) {
          newEvent.min_price = Math.min(...prices);
          newEvent.max_price = Math.max(...prices);
        }

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
    }

    await logProgress(
      `Mapped ${events.length} city-events `
      + `(feed=${feedEvents.length}, skippedNoCity=${skippedNoCity}, skippedUnavailable=${skippedUnavailable})`,
    );
  } catch (e) {
    if (e?.cancelled) throw e;
    const errMsg = e?.message || 'Unknown error while parsing Showman';
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
      source: EVENT_SOURCE.showman,
      infoTexts,
      errorTexts,
      extraStatistics: { citySuggestions: citySuggestionStats },
    });
  } catch (error) {
    if (error?.cancelled) throw error;
    logger.error(`Error saving Showman events: ${error.message || error}`);
  }
}

export default parseShowman;
