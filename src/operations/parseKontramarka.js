import moment from 'moment';
import puppeteer from 'puppeteer';
import CitiesSchema from '../schemas/CitiesSchema';
import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSE_KONTRAMARKA');

const citiesCache = {
  gr: null,
};

const normalize = (str = '') => str
  .toString()
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

const cityTokens = (name = '') => name.split('|').map((s) => normalize(s)).filter(Boolean);

const findCity = (cities, targetName = '') => {
  const target = normalize(targetName);
  if (!target) return null;
  return cities.find((c) => {
    const tokens = cityTokens(c.name);
    return tokens.some((tok) => target.includes(tok) || tok.includes(target));
  }) || null;
};

const parseCoordinatesField = (coord) => {
  if (!coord) return null;
  if (typeof coord === 'object' && coord.lat && coord.lon) {
    return {
      lat: parseFloat(coord.lat),
      lon: parseFloat(coord.lon),
      is_special_point_on_map: false,
    };
  }
  if (typeof coord === 'string') {
    const match = coord.match(/lat\s*=\s*([0-9.,\-]+)[^\d\-]+lon\s*=\s*([0-9.,\-]+)/i);
    if (match) {
      return {
        lat: parseFloat(match[1].replace(',', '.')),
        lon: parseFloat(match[2].replace(',', '.')),
        is_special_point_on_map: false,
      };
    }
  }
  return null;
};

const buildCitySlug = (name = '') => {
  const parts = name.split('|').map((s) => s.trim()).filter(Boolean);
  const prefer = parts[1] || parts[0] || name;
  return encodeURIComponent(prefer.toLowerCase().replace(/\s+/g, '-'));
};

/** Форматирует массив дат в текстовое поле: "12–19 февраля 2025", "12, 16, 22 февраля" или "12 декабря 2024, 15 января 2025" */
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
  for (const k of [...byMonth.keys()].sort()) {
    const arr = byMonth.get(k);
    const m = moment(arr[0]);
    const withYear = multiYear ? ' YYYY' : '';
    if (arr.length === 1) {
      parts.push(m.format('D MMMM' + withYear));
    } else if (arr.length === 2) {
      parts.push(`${moment(arr[0]).format('D')}–${moment(arr[1]).format('D')} ${m.format('MMMM' + withYear)}`);
    } else {
      parts.push(arr.map((d) => moment(d).format('D')).join(', ') + ' ' + m.format('MMMM' + withYear));
    }
  }
  const result = parts.join(', ');
  if (!multiYear && years[0] != null) {
    return `${result} ${years[0]}`;
  }
  return result;
};

/** Объединяет дубликаты по (name, address, city_id): один объект на ключ, даты и цены мержатся. */
const mergeDuplicateEvents = (events) => {
  if (!events || events.length === 0) return [];
  const key = (e) => `${String(e.name).trim()}\n${String(e.address).trim()}\n${(e.city_id || '').toString()}`;
  const byKey = new Map();
  for (const e of events) {
    const k = key(e);
    if (!byKey.has(k)) byKey.set(k, { events: [], dates: [], prices: [] });
    const g = byKey.get(k);
    g.events.push(e);
    const dates = e._mergeDates || (e.date_start ? [e.date_start] : []);
    g.dates.push(...dates);
    if (e.min_price != null) g.prices.push(e.min_price);
    if (e.max_price != null) g.prices.push(e.max_price);
  }
  const result = [];
  for (const g of byKey.values()) {
    const first = g.events[0];
    const toTime = (d) => (d && d.getTime ? d.getTime() : (d ? new Date(d).getTime() : null));
    const validDates = g.dates.map((d) => (d instanceof Date ? d : new Date(d))).filter((d) => !Number.isNaN(d.getTime()));
    const dateStart = validDates.length ? new Date(Math.min(...validDates.map(toTime))) : null;
    const dateEnd = validDates.length ? new Date(Math.max(...validDates.map(toTime))) : null;
    const holdingDateStr = formatHoldingDate(validDates);
    const ev = {
      ...first,
      date_start: dateStart,
      date_end: dateEnd,
      holding_date: holdingDateStr,
      min_price: g.prices.length ? Math.min(...g.prices) : first.min_price,
      max_price: g.prices.length ? Math.max(...g.prices) : first.max_price,
    };
    delete ev._mergeDates;
    result.push(ev);
  }
  return result;
};

const poolAll = async (items, limit, worker) => {
  const results = [];
  const queue = [...items];
  const run = async () => {
    while (queue.length) {
      const item = queue.shift();
      // eslint-disable-next-line no-await-in-loop
      results.push(await worker(item));
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return results.flat().filter(Boolean);
};

const loadCities = async () => {
  if (citiesCache.gr) return citiesCache.gr;
  citiesCache.gr = await CitiesSchema.find({}).lean();
  return citiesCache.gr;
};

const logProgress = async (operationId, message) => {
  if (operationId) {
    try {
      const operation = await OperationsSchema.findById(operationId);
      if (operation) {
        const timestamp = new Date().toISOString();
        const newLog = `[${timestamp}] ${message}`;
        operation.infoText = operation.infoText ? `${operation.infoText}\n${newLog}` : newLog;
        await operation.save();
      }
    } catch (e) {
      logger.error(`Error logging progress: ${e.message || e}`);
    }
  }
};

moment.locale('ru');

async function parseKontramarka({ meta, operationId }) {
  const events = [];
  const errorTexts = [];
  const infoTexts = [];
  let allEvents = [];
  let mergedEvents = [];

  try {
    const {
      adminId, countryId, cityId, specialization = 'Event', maxCities, cityName,
    } = meta || {};
    
    const citiesAll = await loadCities();
    let cities = citiesAll;
    if (cityName) {
      const normalized = cityName.toLowerCase();
      cities = citiesAll.filter((c) => c.name.toLowerCase().includes(normalized));
    } else if (typeof maxCities === 'number' && maxCities > 0) {
      cities = citiesAll.slice(0, maxCities);
    }

    await logProgress(operationId, `Starting Kontramarka parsing. Cities to process: ${cities.length}`);

    let browser;
    try {
      await logProgress(operationId, 'Launching browser...');
      browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      });
      await logProgress(operationId, 'Browser launched successfully');
    } catch (launchError) {
      const errorMsg = `Failed to launch browser: ${launchError?.message || launchError}`;
      errorTexts.push(errorMsg);
      await logProgress(operationId, `FATAL ERROR: ${errorMsg}`);
      throw new Error(errorMsg);
    }

    const processCity = async (cityItem) => {
      const slug = buildCitySlug(cityItem.name);
      const url = `https://www.kontramarka.de/city/${slug}/`;
      const page = await browser.newPage();
      const cityEvents = [];
      let scraped = 0;
      let skippedMissingIds = 0;
      try {
        await logProgress(operationId, `Processing city: ${cityItem.name} (${url})`);
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 45000 });
        const cards = await page.$$eval('.events__item', (items) => items.map((el) => {
          const title = el.querySelector('.block-title__text')?.textContent?.trim() || '';
          const infoItems = el.querySelectorAll('.long-event__info-item');
          const venue = infoItems[1]?.textContent?.trim() || '';
          const img = el.querySelector('.cover-img-wrapper img');
          const photo = img?.getAttribute('data-lazy-src') || img?.getAttribute('src') || '';
          const link = el.querySelector('a[href*="/tour/"]')?.getAttribute('href') || '';
          return {
            title, venue, photo, link,
          };
        }));

        for (const card of cards) {
          scraped += 1;
          const photoUrl = card.photo
            ? (card.photo.startsWith('http')
              ? card.photo
              : `https://www.kontramarka.de/${card.photo.replace(/^\.?\/+/, '')}`)
            : null;
          const tourUrl = card.link?.startsWith('http')
            ? card.link
            : card.link
              ? `https://www.kontramarka.de${card.link}`
              : '';

          if (!tourUrl) continue;

          const detail = await browser.newPage();
          try {
            await detail.goto(tourUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
            const slots = await detail.$$eval('#scheduleType_list .schedule-row', (rows) => rows.map((row) => {
              const availability = row.querySelector('[itemprop="availability"]')?.getAttribute('content')?.toLowerCase() || '';
              const actionText = row.querySelector('.schedule-col-action')?.textContent?.toLowerCase() || '';
              const sold = availability.includes('soldout') || actionText.includes('распродан');
              if (sold) return null;
              const startIso = row.querySelector('[itemprop="startDate"]')?.getAttribute('content') || '';
              const endIso = row.querySelector('[itemprop="endDate"]')?.getAttribute('content') || startIso;
              const cityName = row.querySelector('.schedule-col-main .city')?.textContent?.trim() || '';
              const place = row.querySelector('.schedule-col-main .place')?.textContent?.trim() || '';
              const address = row.querySelector('[itemprop="address"]')?.getAttribute('content')
                || place
                || '';
              const priceStr = row.querySelector('[itemprop="price"]')?.getAttribute('content');
              const price = priceStr ? parseFloat(priceStr.replace(',', '.')) : null;
              const image = row.querySelector('[itemprop="image"]')?.getAttribute('content') || '';
              const description = row.querySelector('meta[itemprop="description"]')?.getAttribute('content') || '';
              return {
                startIso,
                endIso,
                cityName,
                place,
                address,
                price,
                image,
                description,
              };
            }).filter(Boolean));

            const groupKey = (name, address) => `${String(name).trim()}\n${String(address).trim()}`;
            const groups = new Map();

            for (const slot of slots) {
              const matchedCity = findCity(cities, slot.cityName || cityItem.name);
              const fallbackCoords = parseCoordinatesField(matchedCity?.coordinates);
              const resolvedCityId = cityId || matchedCity?._id || null;
              const resolvedCountryId = countryId || matchedCity?.country_id || null;

              if (!resolvedCityId || !resolvedCountryId) {
                skippedMissingIds += 1;
                const skipMsg = `Skip event "${card.title}" – city/country id is missing; provide meta.cityId/meta.countryId or add IDs to DB. [DEBUG targetCity="${slot.cityName || cityItem.name}" matched="${matchedCity?.name || 'null'}" matchedCityId="${matchedCity?._id || '-'}" matchedCountryId="${matchedCity?.country_id || '-'}" providedCityId="${cityId || '-'}" providedCountryId="${countryId || '-'}"]`;
                infoTexts.push(skipMsg);
                await logProgress(operationId, `INFO: ${skipMsg}`);
                continue;
              }

              const dateStart = slot.startIso ? new Date(slot.startIso) : null;
              const dateEnd = slot.endIso ? new Date(slot.endIso) : dateStart;
              const address = [slot.place || card.venue, slot.address || cityItem.name.split('|')[0]].filter(Boolean).join(', ');
              const key = groupKey(card.title, address);

              if (!groups.has(key)) {
                groups.set(key, {
                  name: card.title,
                  address,
                  resolvedCityId,
                  resolvedCountryId,
                  fallbackCoords,
                  dates: [],
                  prices: [],
                  description: slot.description || card.title,
                  photoUrl: slot.image || photoUrl,
                  tourUrl,
                });
              }
              const g = groups.get(key);
              if (dateStart) g.dates.push(dateStart);
              if (typeof slot.price === 'number') g.prices.push(slot.price);
            }

            for (const g of groups.values()) {
              const dateStart = g.dates.length ? new Date(Math.min(...g.dates.map((d) => d.getTime()))) : null;
              const dateEnd = g.dates.length ? new Date(Math.max(...g.dates.map((d) => d.getTime()))) : null;
              const holdingDateStr = formatHoldingDate(g.dates);

              const newEvent = {
                name: g.name,
                description: g.description,
                specialization,
                admin_id: adminId,
                country_id: g.resolvedCountryId,
                city_id: g.resolvedCityId,
                operationId: operationId,
                contacts: { website: g.tourUrl },
                photos: g.photoUrl ? [{ full_url: g.photoUrl }] : [],
                holding_date: holdingDateStr,
                date_start: dateStart,
                date_end: dateEnd,
                source: EVENT_SOURCE.kontramarka,
                address: g.address,
              };

              if (g.fallbackCoords?.lat && g.fallbackCoords?.lon) {
                newEvent.lat = g.fallbackCoords.lat;
                newEvent.lon = g.fallbackCoords.lon;
                newEvent.is_special_point_on_map = g.fallbackCoords.is_special_point_on_map;
              }

              if (g.prices.length) {
                newEvent.min_price = Math.min(...g.prices);
                newEvent.max_price = Math.max(...g.prices);
              }
              newEvent._mergeDates = g.dates;

              cityEvents.push(newEvent);
            }
          } catch (detailErr) {
            const errMsg = `Error opening tour ${tourUrl}: ${detailErr?.message || detailErr}`;
            infoTexts.push(errMsg);
            await logProgress(operationId, `WARNING: ${errMsg}`);
          } finally {
            await detail.close();
          }
        }

        if (!cards.length) {
          const noEventsMsg = `No events found on page for city ${cityItem.name} (${url})`;
          infoTexts.push(noEventsMsg);
          await logProgress(operationId, `INFO: ${noEventsMsg}`);
        } else {
          const cityStats = `City ${cityItem.name}: scraped ${scraped}, skippedMissingIds ${skippedMissingIds}, added ${cityEvents.length}`;
          infoTexts.push(cityStats);
          await logProgress(operationId, cityStats);
        }
      } catch (e) {
        const errMsg = `Error for city ${cityItem.name}: ${e?.message || e}`;
        infoTexts.push(errMsg);
        await logProgress(operationId, `WARNING: ${errMsg}`);
      } finally {
        await page.close();
      }
      return cityEvents;
    };

    allEvents = await poolAll(cities, 3, processCity);

    await browser.close();
    await logProgress(operationId, 'Browser closed');

    mergedEvents = mergeDuplicateEvents(allEvents || []);
    await logProgress(operationId, `Parsing completed. Total: ${mergedEvents.length} events (after merging duplicates)`);
  } catch (e) {
    const errMsg = e?.message || 'Unknown error while parsing Kontramarka';
    errorTexts.push(errMsg);
    await logProgress(operationId, `FATAL ERROR: ${errMsg}`);
  }

  const BATCH_SIZE = 10;
  const eventsToSave = mergedEvents.length ? mergedEvents : mergeDuplicateEvents(allEvents || []);
  try {
    for (let i = 0; i < eventsToSave.length; i += BATCH_SIZE) {
      const batch = eventsToSave.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      
      await ParsedEventsSchema.insertMany(
        batch.map(event => ({
          operation: operationId,
          event_data: event,
          batch_number: batchNumber,
        }))
      );
      
      const operation = await OperationsSchema.findById(operationId);
      await OperationsSchema.findByIdAndUpdate(operationId, {
        infoText: `${operation?.infoText || ''}\nОбработано ${i + batch.length} из ${eventsToSave.length} событий. Батч ${batchNumber} из ${Math.ceil(eventsToSave.length / BATCH_SIZE)}`,
      });
    }
    
    const operation = await OperationsSchema.findById(operationId);
    const finalInfoText = operation?.infoText || '';
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';
    
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'success',
      finish_time: new Date(),
      statistics: JSON.stringify({
        total: eventsToSave.length,
        batches: Math.ceil(eventsToSave.length / BATCH_SIZE),
        errors: errorTexts.length,
      }),
      errorText: errorTexts.join('\n'),
      infoText: finalInfoText + additionalInfo,
    });
  } catch (error) {
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: error.message || 'Unknown error while saving events',
      finish_time: new Date(),
    });
  }
}

export default parseKontramarka;

