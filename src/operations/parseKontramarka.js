import moment from 'moment';
import puppeteer from 'puppeteer';
import CitiesSchema from '../schemas/CitiesSchema';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import { EVENT_SOURCE } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';
import saveProcessedEvents from '../helpers/saveProcessedEvents';
import logParseRun from '../helpers/logParseRun';
import createCitySuggestionCollector from '../helpers/createCitySuggestionCollector';
import findCityInDb from '../helpers/cityMatching';

const logger = createLoggerWithSource('PARSE_KONTRAMARKA');

const citiesCache = {
  gr: null,
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

/** Форматирует последовательность чисел дат: 3+ подряд — через тире */
const formatDateRange = (dateNumbers) => {
  if (!dateNumbers || dateNumbers.length === 0) return '';
  if (dateNumbers.length === 1) return dateNumbers[0];
  const numbers = dateNumbers.map(n => parseInt(n, 10)).filter(n => !isNaN(n));
  if (numbers.length === 0) return dateNumbers.join(', ');
  const result = [];
  let start = numbers[0];
  let end = numbers[0];
  for (let i = 1; i < numbers.length; i++) {
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
      const formattedDates = formatDateRange(arr.map((d) => moment(d).format('D')));
      parts.push(formattedDates + ' ' + m.format('MMMM' + withYear));
    }
  }
  const result = parts.join(', ');
  if (!multiYear && years[0] != null) {
    return `${result} ${years[0]}`;
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

const logProgress = async (runId, message) => {
  await logParseRun(runId, `[${new Date().toISOString()}] ${message}`);
};

moment.locale('ru');

async function parseKontramarka({ meta = {}, runId }) {
  const parseRunId = runId;
  const events = [];
  const errorTexts = [];
  const infoTexts = [];
  let allEvents = [];
  const citySuggestions = createCitySuggestionCollector(EVENT_SOURCE.kontramarka);

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

    await logProgress(parseRunId, `Starting Kontramarka parsing. Cities to process: ${cities.length}`);

    let browser;
    try {
      await logProgress(parseRunId, 'Launching browser...');
      browser = await puppeteer.launch({
        headless: 'new',
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      });
      await logProgress(parseRunId, 'Browser launched successfully');
    } catch (launchError) {
      const errorMsg = `Failed to launch browser: ${launchError?.message || launchError}`;
      errorTexts.push(errorMsg);
      await logProgress(parseRunId, `FATAL ERROR: ${errorMsg}`);
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
        await logProgress(parseRunId, `Processing city: ${cityItem.name} (${url})`);
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

            const groupKey = (name, city) => `${String(name).trim()}\n${String(city || '')}`;
            const groups = new Map();

            for (const slot of slots) {
              const matchedCity = findCityInDb(cities, slot.cityName || cityItem.name);
              const fallbackCoords = parseCoordinatesField(matchedCity?.coordinates);
              const resolvedCityId = cityId || matchedCity?._id || null;
              const resolvedCountryId = countryId || matchedCity?.country_id || null;

              if (!resolvedCityId || !resolvedCountryId) {
                skippedMissingIds += 1;
                const targetCityName = slot.cityName || cityItem.name;
                citySuggestions.note(targetCityName, {
                  slug: buildCitySlug(targetCityName),
                  source_url: tourUrl || url,
                });
                const skipMsg = `Skip event "${card.title}" – city/country id is missing; provide meta.cityId/meta.countryId or add IDs to DB. [DEBUG targetCity="${targetCityName}" matched="${matchedCity?.name || 'null'}" matchedCityId="${matchedCity?._id || '-'}" matchedCountryId="${matchedCity?.country_id || '-'}" providedCityId="${cityId || '-'}" providedCountryId="${countryId || '-'}"]`;
                infoTexts.push(skipMsg);
                await logProgress(parseRunId, `INFO: ${skipMsg}`);
                continue;
              }

              const dateStart = slot.startIso ? new Date(slot.startIso) : null;
              const dateEnd = slot.endIso ? new Date(slot.endIso) : dateStart;
              const address = [slot.place || card.venue, slot.address || cityItem.name.split('|')[0]].filter(Boolean).join(', ');
              const key = groupKey(card.title, resolvedCityId);

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
            await logProgress(parseRunId, `WARNING: ${errMsg}`);
          } finally {
            await detail.close();
          }
        }

        if (!cards.length) {
          const noEventsMsg = `No events found on page for city ${cityItem.name} (${url})`;
          infoTexts.push(noEventsMsg);
          await logProgress(parseRunId, `INFO: ${noEventsMsg}`);
        } else {
          const cityStats = `City ${cityItem.name}: scraped ${scraped}, skippedMissingIds ${skippedMissingIds}, added ${cityEvents.length}`;
          infoTexts.push(cityStats);
          await logProgress(parseRunId, cityStats);
        }
      } catch (e) {
        if (e?.cancelled) throw e;
        const errMsg = `Error for city ${cityItem.name}: ${e?.message || e}`;
        infoTexts.push(errMsg);
        await logProgress(parseRunId, `WARNING: ${errMsg}`);
      } finally {
        await page.close();
      }
      return cityEvents;
    };

    allEvents = await poolAll(cities, 3, processCity);

    await browser.close();
    await logProgress(parseRunId, 'Browser closed');
    await logProgress(parseRunId, `Parsing completed. Total: ${(allEvents || []).length} events`);
  } catch (e) {
    if (e?.cancelled) throw e;
    const errMsg = e?.message || 'Unknown error while parsing Kontramarka';
    errorTexts.push(errMsg);
    await logProgress(parseRunId, `FATAL ERROR: ${errMsg}`);
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
      events: allEvents || [],
      source: EVENT_SOURCE.kontramarka,
      infoTexts,
      errorTexts,
      extraStatistics: { citySuggestions: citySuggestionStats },
    });
  } catch (error) {
    if (error?.cancelled) throw error;
    await ParseRunsSchema.findByIdAndUpdate(parseRunId, {
      status: 'error',
      errorText: error.message || 'Unknown error while saving events',
      finishedAt: new Date(),
    });
  }
}

export default parseKontramarka;

