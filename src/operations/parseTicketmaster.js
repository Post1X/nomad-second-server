import moment from 'moment';
import https from 'https';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE, TICKETMASTER_COUNTRY_CODES } from '../helpers/constants';
import { findCountryByIso, resolveTicketmasterCountryCodes } from '../helpers/isoCountryAliases';
import findCityInDb from '../helpers/cityMatching';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSE_TICKETMASTER');

const DISCOVERY_BASE = 'https://app.ticketmaster.com/discovery/v2';
const PAGE_SIZE = 200;
const REQUEST_DELAY_MS = 250;
/** Ticketmaster: (page * size) must be < 1000 */
const TM_MAX_PAGE_OFFSET = 1000;

const citiesCache = { list: null };
const countriesCache = { list: null };

const normalize = (str = '') => str
  .toString()
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

const parseCoordinatesField = (coord) => {
  if (!coord) return null;
  if (typeof coord === 'object' && coord.lat && coord.lon) {
    return {
      lat: parseFloat(coord.lat),
      lon: parseFloat(coord.lon),
      is_special_point_on_map: false,
    };
  }
  return null;
};

const formatDateRange = (dateNumbers) => {
  if (!dateNumbers || dateNumbers.length === 0) return '';
  if (dateNumbers.length === 1) return dateNumbers[0];
  const numbers = dateNumbers.map((n) => parseInt(n, 10)).filter((n) => !Number.isNaN(n));
  if (numbers.length === 0) return dateNumbers.join(', ');
  const result = [];
  let start = numbers[0];
  let end = numbers[0];
  for (let i = 1; i < numbers.length; i += 1) {
    if (numbers[i] === end + 1) {
      end = numbers[i];
    } else {
      const count = end - start + 1;
      if (count === 1) result.push(start.toString());
      else if (count === 2) { result.push(start.toString()); result.push(end.toString()); }
      else result.push(`${start}–${end}`);
      start = numbers[i];
      end = numbers[i];
    }
  }
  const count = end - start + 1;
  if (count === 1) result.push(start.toString());
  else if (count === 2) { result.push(start.toString()); result.push(end.toString()); }
  else result.push(`${start}–${end}`);
  return result.join(', ');
};

const formatHoldingDate = (dateArray) => {
  if (!dateArray || dateArray.length === 0) return '';
  const seen = new Set();
  const uniques = [];
  for (const d of dateArray) {
    if (!d || !(d instanceof Date)) continue;
    const key = `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniques.push(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }
  uniques.sort((a, b) => a.getTime() - b.getTime());
  if (uniques.length === 1) {
    return moment(uniques[0]).format('D MMMM YYYY');
  }
  const years = [...new Set(uniques.map((d) => d.getFullYear()))];
  const multiYear = years.length > 1;
  const byMonth = new Map();
  for (const d of uniques) {
    const k = `${d.getFullYear()}-${d.getMonth()}`;
    if (!byMonth.has(k)) byMonth.set(k, []);
    byMonth.get(k).push(d);
  }
  const parts = [];
  for (const [, arr] of byMonth) {
    arr.sort((a, b) => a.getTime() - b.getTime());
    const m = moment(arr[0]);
    const withYear = multiYear ? ' YYYY' : '';
    if (arr.length >= 3 && arr.every((d, i) => i === 0 || d.getDate() === arr[i - 1].getDate() + 1)) {
      parts.push(`${moment(arr[0]).format('D')}–${moment(arr[arr.length - 1]).format('D')} ${m.format(`MMMM${withYear}`)}`);
    } else {
      const formattedDates = formatDateRange(arr.map((d) => moment(d).format('D')));
      parts.push(`${formattedDates} ${m.format(`MMMM${withYear}`)}`);
    }
  }
  const result = parts.join(', ');
  if (!multiYear && years[0] != null) {
    return `${result} ${years[0]}`;
  }
  return result;
};

const pickBestImage = (images = []) => {
  if (!images.length) return null;
  const preferred = images.find((img) => img.ratio === '16_9' && img.width >= 640 && !img.fallback)
    || images.find((img) => img.ratio === '16_9' && !img.fallback)
    || images[0];
  return preferred?.url || null;
};

const buildAddress = (venue = {}) => {
  const parts = [
    venue.name,
    venue.address?.line1,
    venue.address?.line2,
    venue.city?.name,
    venue.state?.stateCode || venue.state?.name,
    venue.postalCode,
  ].filter(Boolean);
  return parts.join(', ');
};

const parseEventDate = (dates = {}) => {
  const start = dates.start || {};
  if (!start.localDate) return null;
  const timePart = start.localTime || '00:00:00';
  const iso = `${start.localDate}T${timePart}`;
  const parsed = new Date(iso);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });

const formatStartDateTime = (date) => date.toISOString().replace(/\.\d{3}Z$/, 'Z');

const getMaxPageIndex = (size) => Math.floor((TM_MAX_PAGE_OFFSET - 1) / size);

const fetchJson = (url) => new Promise((resolve, reject) => {
  https.get(url, (res) => {
    let body = '';
    res.on('data', (chunk) => { body += chunk; });
    res.on('end', () => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        reject(new Error(`Ticketmaster API HTTP ${res.statusCode}: ${body.slice(0, 300)}`));
        return;
      }
      try {
        resolve(JSON.parse(body));
      } catch (e) {
        reject(new Error(`Invalid JSON from Ticketmaster: ${e.message}`));
      }
    });
  }).on('error', reject);
});

const loadCities = async () => {
  if (citiesCache.list) return citiesCache.list;
  citiesCache.list = await CitiesSchema.find({}).lean();
  return citiesCache.list;
};

const loadCountries = async () => {
  if (countriesCache.list) return countriesCache.list;
  countriesCache.list = await CountriesSchema.find({}).lean();
  return countriesCache.list;
};

const resolveCountryCodes = (meta, countries) => resolveTicketmasterCountryCodes(
  meta,
  countries,
  TICKETMASTER_COUNTRY_CODES,
);

const filterCitiesForCountry = (citiesAll, countries, countryCode, meta = {}) => {
  const {
    countryId: metaCountryId,
    cityId: metaCityId,
    cityName: metaCityName,
  } = meta;

  const matchedCountry = findCountryByIso(countries, countryCode);
  const countryFilterId = metaCountryId || matchedCountry?._id;

  let cities = citiesAll;
  if (countryFilterId) {
    cities = cities.filter((c) => String(c.country_id) === String(countryFilterId));
  }
  if (metaCityId) {
    cities = cities.filter((c) => String(c._id) === String(metaCityId));
  } else if (metaCityName) {
    const n = normalize(metaCityName);
    cities = cities.filter((c) => normalize(c.name).includes(n));
  }

  return { cities, matchedCountry };
};

const buildSkippedCitiesSummary = (skippedByCity, minCount = 5) => Object.fromEntries(
  Object.entries(skippedByCity)
    .filter(([, count]) => count > minCount)
    .sort((a, b) => b[1] - a[1]),
);

const parseEventsForCountry = async ({
  apiKey,
  countryCode,
  cities,
  countries,
  meta,
  operationId,
  startFrom,
  pageSize,
  maxPages,
  skippedByCity,
}) => {
  const events = [];
  let skippedNoVenue = 0;
  let skippedNoCity = 0;

  const {
    adminId,
    countryId: metaCountryId,
    cityId: metaCityId,
    specialization = 'Event',
  } = meta;

  const matchedCountry = findCountryByIso(countries, countryCode);
  const resolvedCountryIdFromIso = matchedCountry?._id?.toString() || null;

  const effectiveSize = Math.min(pageSize, 200);
  const maxPageIndex = getMaxPageIndex(effectiveSize);
  const seenEventIds = new Set();
  let cursorStart = startFrom;
  let hasMoreWindows = true;
  let pagesFetched = 0;

  while (hasMoreWindows) {
    let page = 0;
    let totalPages = 1;
    let lastBatchLastDate = null;
    let hitPageLimit = false;

    while (page < totalPages) {
      if (typeof maxPages === 'number' && pagesFetched >= maxPages) {
        hasMoreWindows = false;
        break;
      }
      if (page > maxPageIndex) {
        hitPageLimit = true;
        break;
      }

      const params = new URLSearchParams({
        apikey: apiKey,
        countryCode,
        size: String(effectiveSize),
        page: String(page),
        sort: 'date,asc',
        startDateTime: cursorStart,
      });

      const url = `${DISCOVERY_BASE}/events.json?${params.toString()}`;
      // eslint-disable-next-line no-await-in-loop
      const data = await fetchJson(url);
      const pageInfo = data.page || {};
      totalPages = pageInfo.totalPages ?? 1;

      const batch = data._embedded?.events || [];

      for (const event of batch) {
        if (seenEventIds.has(event.id)) continue;
        seenEventIds.add(event.id);

        const venue = event._embedded?.venues?.[0];
        if (!venue) {
          skippedNoVenue += 1;
          continue;
        }

        const dateStart = parseEventDate(event.dates);
        if (dateStart) lastBatchLastDate = dateStart;

        const venueCityName = venue.city?.name || '';
        const venueCountryCode = venue.country?.countryCode || countryCode;
        const matchedCity = findCityInDb(cities, venueCityName);
        const fallbackCoords = parseCoordinatesField(matchedCity?.coordinates);

        let resolvedCityId = metaCityId || matchedCity?._id || null;
        let resolvedCountryId = metaCountryId || matchedCity?.country_id || resolvedCountryIdFromIso || null;

        if (!resolvedCountryId && venueCountryCode) {
          const venueCountry = findCountryByIso(countries, venueCountryCode);
          resolvedCountryId = venueCountry?._id || null;
        }

        if (!resolvedCityId || !resolvedCountryId) {
          skippedNoCity += 1;
          const cityKey = venueCityName || 'Unknown';
          skippedByCity[cityKey] = (skippedByCity[cityKey] || 0) + 1;
          continue;
        }

        if (!dateStart) continue;

        const address = buildAddress(venue);
        const imageUrl = pickBestImage(event.images);

        const newEvent = {
          name: event.name,
          description: event.info || event.description || event.name,
          specialization,
          admin_id: adminId,
          country_id: resolvedCountryId?.toString ? resolvedCountryId.toString() : String(resolvedCountryId),
          city_id: resolvedCityId?.toString ? resolvedCityId.toString() : String(resolvedCityId),
          operationId,
          contacts: { website: event.url || '' },
          photos: imageUrl ? [{ full_url: imageUrl }] : [],
          holding_date: formatHoldingDate([dateStart]),
          date_start: dateStart,
          date_end: dateStart,
          source: EVENT_SOURCE.ticketmaster,
          address,
          ticketmaster_id: event.id,
        };

        if (venue.location?.latitude && venue.location?.longitude) {
          newEvent.lat = parseFloat(venue.location.latitude);
          newEvent.lon = parseFloat(venue.location.longitude);
          newEvent.is_special_point_on_map = false;
        } else if (fallbackCoords?.lat && fallbackCoords?.lon) {
          newEvent.lat = fallbackCoords.lat;
          newEvent.lon = fallbackCoords.lon;
          newEvent.is_special_point_on_map = fallbackCoords.is_special_point_on_map;
        }

        const priceRanges = event.priceRanges || [];
        if (priceRanges.length) {
          const mins = priceRanges.map((p) => p.min).filter((v) => typeof v === 'number');
          const maxs = priceRanges.map((p) => p.max).filter((v) => typeof v === 'number');
          if (mins.length) newEvent.min_price = Math.min(...mins);
          if (maxs.length) newEvent.max_price = Math.max(...maxs);
        }

        events.push(newEvent);
      }

      pagesFetched += 1;
      page += 1;
      if (page < totalPages && page <= maxPageIndex) {
        // eslint-disable-next-line no-await-in-loop
        await sleep(REQUEST_DELAY_MS);
      }
    }

    if (!hasMoreWindows) break;

    if (hitPageLimit && page < totalPages && lastBatchLastDate) {
      cursorStart = formatStartDateTime(new Date(lastBatchLastDate.getTime() + 1000));
      // eslint-disable-next-line no-await-in-loop
      await sleep(REQUEST_DELAY_MS);
      continue;
    }

    hasMoreWindows = false;
  }

  return { events, skippedNoVenue, skippedNoCity };
};

moment.locale('ru');

async function parseTicketmaster({ meta, operationId }) {
  const events = [];
  const errorTexts = [];
  const skippedByCity = {};

  const {
    maxPages,
    pageSize = PAGE_SIZE,
    startDateTime,
  } = meta || {};

  const apiKey = process.env.TICKETMASTER_API_KEY;
  if (!apiKey) {
    const errMsg = 'TICKETMASTER_API_KEY is not set in environment';
    errorTexts.push(errMsg);
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: errMsg,
      finish_time: new Date(),
    });
    return;
  }

  let skippedNoVenue = 0;
  let skippedNoCity = 0;
  let countriesProcessed = 0;
  let countryCodes = [];
  const parsedByCountry = {};

  try {
    const [citiesAll, countries] = await Promise.all([loadCities(), loadCountries()]);
    countryCodes = resolveCountryCodes(meta, countries);
    const startFrom = startDateTime || new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');

    for (const countryCode of countryCodes) {
      const { cities } = filterCitiesForCountry(citiesAll, countries, countryCode, meta);

      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await parseEventsForCountry({
          apiKey,
          countryCode,
          cities,
          countries,
          meta,
          operationId,
          startFrom,
          pageSize,
          maxPages,
          skippedByCity,
        });

        events.push(...result.events);
        skippedNoVenue += result.skippedNoVenue;
        skippedNoCity += result.skippedNoCity;
        parsedByCountry[countryCode] = result.events.length;
        countriesProcessed += 1;
      } catch (countryError) {
        const msg = `${countryCode}: ${countryError?.message || countryError}`;
        errorTexts.push(msg);
        logger.error(msg);
        parsedByCountry[countryCode] = 0;
      }

      // eslint-disable-next-line no-await-in-loop
      await sleep(REQUEST_DELAY_MS);
    }
  } catch (e) {
    const errMsg = e?.message || 'Unknown error while parsing Ticketmaster';
    errorTexts.push(errMsg);
    logger.error(`FATAL ERROR: ${errMsg}`, e);
  }

  const skippedCitiesOver5 = buildSkippedCitiesSummary(skippedByCity);
  const statistics = {
    total: events.length,
    batches: 0,
    errors: errorTexts.length,
    countryCodes,
    countriesProcessed,
    parsedByCountry,
    skippedNoVenue,
    skippedNoCity,
    skippedCitiesOver5,
  };

  const BATCH_SIZE = 10;
  try {
    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = events.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

      // eslint-disable-next-line no-await-in-loop
      await ParsedEventsSchema.insertMany(
        batch.map((event) => ({
          operation: operationId,
          event_data: event,
          batch_number: batchNumber,
        })),
      );
    }

    statistics.batches = Math.ceil(events.length / BATCH_SIZE) || 0;

    const summary = `parsed=${events.length}, countries=${countriesProcessed}, skippedNoCity=${skippedNoCity}`;

    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'success',
      finish_time: new Date(),
      statistics: JSON.stringify(statistics),
      errorText: errorTexts.join('\n'),
      infoText: summary,
    });
  } catch (error) {
    logger.error(`Error saving events to database: ${error.message || error}`, error);
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: error.message || 'Unknown error while saving events',
      finish_time: new Date(),
      statistics: JSON.stringify(statistics),
    });
  }
}

export default parseTicketmaster;
