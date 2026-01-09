import moment from 'moment';
import puppeteer from 'puppeteer';
import { load as cheerioLoad } from 'cheerio';
import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE } from '../helpers/constants';

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

async function extractEventFromPageHtml(html, cityName) {
  const $ = cheerioLoad(html);

  const pickEventJson = () => {
    const scripts = $('script[type="application/ld+json"]')
      .map((_, el) => $(el).text())
      .get();
    for (const txt of scripts) {
      try {
        const parsed = JSON.parse(txt);
        if (Array.isArray(parsed)) {
          const evt = parsed.find((it) => it['@type'] === 'Event');
          if (evt) return evt;
        }
        if (parsed && parsed['@type'] === 'Event') return parsed;
      } catch (err) { /* ignore */ }
    }
    return null;
  };

  const evtJson = pickEventJson();
  if (!evtJson) {
    return [];
  }

  const offers = evtJson?.offers || [];
  const prices = offers.map((o) => parseFloat(String(o.price).replace(',', '.'))).filter((n) => !Number.isNaN(n));
  const minPrice = prices.length ? Math.min(...prices) : null;
  const maxPrice = prices.length ? Math.max(...prices) : null;

  const locationAddress = evtJson?.location?.address;
  const addressParts = [];
  if (locationAddress?.streetAddress) addressParts.push(locationAddress.streetAddress);
  if (locationAddress?.addressLocality) addressParts.push(locationAddress.addressLocality);
  const addressFromJson = addressParts.join(', ');
  
  const locationName = evtJson?.location?.name || '';
  const locationFromHtml = $('.location').first().text().trim() || '';
  const baseAddress = addressFromJson || locationName || locationFromHtml;

  const baseEvent = {
    name: evtJson?.name || $('meta[property="og:title"]').attr('content') || '',
    description: evtJson?.description || evtJson?.name || '',
    image: Array.isArray(evtJson?.image) ? evtJson.image[0] : evtJson?.image,
    url: evtJson?.url || '',
    address: baseAddress,
    minPrice,
    maxPrice,
  };

  const seriesItems = $('.series-item').toArray();
  
  if (seriesItems.length === 0) {
    return [{
      ...baseEvent,
      startDate: evtJson?.startDate,
      endDate: evtJson?.endDate || evtJson?.startDate,
      holdingDate: evtJson?.startDate || '',
    }];
  }

  const events = [];
  for (const seriesItem of seriesItems) {
    const seriesText = $(seriesItem).text().trim();
    const seriesLines = seriesText.split('\n').map((t) => t.trim()).filter(Boolean);
    
    let dateText = seriesLines[0] || '';
    const timeText = seriesLines[1] || '';
    
    let startDate = evtJson?.startDate;
    let endDate = evtJson?.endDate || evtJson?.startDate;
    let holdingDate = evtJson?.startDate || '';

    if (dateText) {
      try {
        const cleanDateText = dateText.trim();
        
        const hasYear = /\d{4}/.test(cleanDateText);
        
        let parsedDate = null;
        
        if (hasYear) {
          parsedDate = moment(cleanDateText, ['ddd, DD MMM, YYYY', 'ddd, DD MMM YYYY', 'DD MMM, YYYY', 'DD MMM YYYY', 'YYYY-MM-DD'], true);
        } else {
          const currentYear = moment().year();
          const dateWithCurrentYear = `${cleanDateText} ${currentYear}`;
          parsedDate = moment(dateWithCurrentYear, ['ddd, DD MMM YYYY', 'DD MMM YYYY'], true);
          
          if (parsedDate.isValid() && parsedDate.isBefore(moment(), 'day')) {
            const dateWithNextYear = `${cleanDateText} ${currentYear + 1}`;
            parsedDate = moment(dateWithNextYear, ['ddd, DD MMM YYYY', 'DD MMM YYYY'], true);
          }
        }
        
        if (parsedDate && parsedDate.isValid()) {
          if (timeText) {
            const timeMatch = timeText.match(/(\d{1,2}):(\d{2})/);
            if (timeMatch) {
              const hours = parseInt(timeMatch[1], 10);
              const minutes = parseInt(timeMatch[2], 10);
              if (!Number.isNaN(hours) && !Number.isNaN(minutes)) {
                parsedDate.hours(hours).minutes(minutes).seconds(0).milliseconds(0);
              }
            }
          }
          startDate = parsedDate.toISOString();
          endDate = parsedDate.toISOString();
          holdingDate = parsedDate.toISOString();
        }
      } catch (e) {
      }
    }

    events.push({
      ...baseEvent,
      startDate,
      endDate,
      holdingDate,
    });
  }

  return events;
}

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
      console.error('Error logging progress:', e);
    }
  }
};

async function parseFienta({ meta, operationId }) {
  const events = [];
  const errorTexts = [];

  const {
    adminId, countryId, cityId, cityName, specialization = 'Event', maxCities,
  } = meta || {};
  
  // Используем города из meta.cities (переданные с основного сервера)
  const citiesAll = meta.cities || [];
  
  let cities = citiesAll;
  if (cityName || cityId) {
    if (cityId) {
      cities = citiesAll.filter((c) => c._id.toString() === cityId.toString());
    } else if (cityName) {
      const normalizedCityName = normalize(cityName);
      cities = citiesAll.filter((c) => {
        const tokens = cityTokens(c.name);
        return tokens.some((tok) => normalizedCityName.includes(tok) || tok.includes(normalizedCityName));
      });
    }
    
    if (cities.length === 0) {
      errorTexts.push(`City not found: ${cityName || cityId}`);
      await logProgress(operationId, `City not found: ${cityName || cityId}`);
      await OperationsSchema.findByIdAndUpdate(operationId, {
        status: 'error',
        errorText: errorTexts.join('\n'),
        finish_time: new Date(),
      });
      return;
    }
    
    await logProgress(operationId, `Filtered to ${cities.length} city(ies) based on meta.cityName/cityId`);
  }
  
  if (typeof maxCities === 'number' && maxCities > 0) {
    cities = cities.slice(0, maxCities);
  }

  await logProgress(operationId, `Starting Fienta parsing. Cities to process: ${cities.length}`);

  let browser;

  try {
    await logProgress(operationId, 'Launching browser...');
    try {
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
    
    for (const city of cities) {
      const cityToken = cityTokens(city.name)[1] || cityTokens(city.name)[0] || cityName || '';
      const searchUrl = `https://fienta.com/?country=&city=${encodeURIComponent(cityToken)}`;
      await logProgress(operationId, `Processing city: ${city.name} (${cityToken})`);
      const page = await browser.newPage();
      let scraped = 0;
      let skippedMissingIds = 0;
      let added = 0;
      try {
        await logProgress(operationId, `Loading search page for ${city.name}...`);
        await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
        const eventLinks = await page.$$eval('#events a[href*="fienta.com"]', (anchors) => {
          const urls = anchors.map((a) => a.href.split('#')[0]);
          return Array.from(new Set(urls));
        });
        await page.close();
        await logProgress(operationId, `Found ${eventLinks.length} events for ${city.name}`);

        const totalFound = eventLinks.length;
        if (eventLinks.length > 5) {
          eventLinks.splice(5);
          errorTexts.push(`Limited to 5 events for testing (total found: ${totalFound})`);
          await logProgress(operationId, `Limited to 5 events for testing (total found: ${totalFound})`);
        }

        for (const link of eventLinks) {
          await logProgress(operationId, `Processing event: ${link}`);
          const p = await browser.newPage();
          let html = null;
          try {
            await logProgress(operationId, `Loading event page...`);
            await p.goto(link, { waitUntil: 'domcontentloaded', timeout: 20000 });
            html = await p.content();
            if (p && !p.isClosed()) {
              await p.close();
            }
            await logProgress(operationId, 'Parsing event HTML...');
            const parsedArray = await extractEventFromPageHtml(html, cityToken);
            if (!parsedArray || parsedArray.length === 0 || !parsedArray[0]?.name) {
              await logProgress(operationId, 'No valid event data found, skipping');
              continue;
            }

            await logProgress(operationId, `Found ${parsedArray.length} date(s), processing all...`);
            for (const parsed of parsedArray) {
              const dateStart = parsed.startDate ? new Date(parsed.startDate) : null;
              const dateEnd = parsed.endDate ? new Date(parsed.endDate) : dateStart;
              const holdingDate = dateStart ? dateStart.toISOString() : parsed.holdingDate || parsed.startDate || '';

              const targetCity = cityToken || parsed.address || parsed.name || '';
              const matchedCity = findCity(citiesAll, targetCity || parsed.address || parsed.name || '');
              const fallbackCoords = parseCoordinatesField(matchedCity?.coordinates);
              const resolvedCityId = cityId || matchedCity?._id || null;
              const resolvedCountryId = countryId || matchedCity?.country_id || null;

              if (!resolvedCityId || !resolvedCountryId) {
                errorTexts.push(`Skip event "${parsed.name}" – city/country id missing; pass meta.cityId/meta.countryId or ensure city exists in DB. [DEBUG targetCity="${targetCity}" matched="${matchedCity?.name || 'null'}" matchedCityId="${matchedCity?._id || '-'}" matchedCountryId="${matchedCity?.country_id || '-'}" providedCityId="${cityId || '-'}" providedCountryId="${countryId || '-'}"]`);
                skippedMissingIds += 1;
                continue;
              }

              const event = {
                name: parsed.name,
                description: parsed.description || parsed.name,
                specialization,
                admin_id: adminId,
                country_id: resolvedCountryId,
                city_id: resolvedCityId,
                contacts: { website: parsed.url || link },
                photos: parsed.image ? [{ full_url: parsed.image }] : [],
                holding_date: holdingDate,
                date_start: dateStart,
                date_end: dateEnd,
                source: EVENT_SOURCE.fienta,
                address: parsed.address || cityToken || '',
              };

              if (fallbackCoords?.lat && fallbackCoords?.lon) {
                event.lat = fallbackCoords.lat;
                event.lon = fallbackCoords.lon;
                event.is_special_point_on_map = fallbackCoords.is_special_point_on_map;
              }

              if (parsed.minPrice || parsed.maxPrice) {
                event.min_price = parsed.minPrice;
                event.max_price = parsed.maxPrice;
              }

              events.push(event);
              added += 1;
            }
            scraped += 1;
            await logProgress(operationId, `Event processed successfully: ${parsedArray[0]?.name || 'Unknown'}`);
          } catch (err) {
            const errMsg = `Error loading event ${link}: ${err?.message || err}`;
            errorTexts.push(errMsg);
            await logProgress(operationId, `ERROR: ${errMsg}`);
            try {
              if (p && !p.isClosed()) {
                await p.close();
              }
            } catch (closeErr) {
            }
          }
        }
        await logProgress(operationId, `City ${city.name} completed: scraped ${scraped}, skipped ${skippedMissingIds}, added ${added}`);
      } catch (e) {
        const errMsg = `Error for city ${city.name}: ${e?.message || e}`;
        errorTexts.push(errMsg);
        await logProgress(operationId, `ERROR: ${errMsg}`);
      } finally {
        errorTexts.push(`City ${city.name}: scraped ${scraped}, skippedMissingIds ${skippedMissingIds}, added ${added}`);
      }
    }
    await logProgress(operationId, 'All cities processed. Closing browser...');
  } catch (e) {
    const errMsg = e?.message || 'Unknown error while parsing Fienta';
    errorTexts.push(errMsg);
    await logProgress(operationId, `FATAL ERROR: ${errMsg}`);
  } finally {
    if (browser) {
      await browser.close();
      await logProgress(operationId, 'Browser closed');
    }
    await logProgress(operationId, `Parsing completed. Total: ${events.length} events parsed`);
  }

  // Ограничение на уровне ссылок ограничивает количество парсируемых страниц,
  // но одно событие может иметь несколько дат, поэтому может быть > 5 событий
  // Ограничиваем финальный результат до 5 событий
  if (events.length > 5) {
    events.splice(5);
    errorTexts.push(`Limited to 5 events for testing (total parsed: ${events.length})`);
  }

  // Сохранение событий частями (по 10)
  const BATCH_SIZE = 10;
  try {
    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = events.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      
      await ParsedEventsSchema.insertMany(
        batch.map(event => ({
          operation: operationId,
          event_data: event,
          batch_number: batchNumber,
        }))
      );
      
      // Обновление прогресса в infoText
      const operation = await OperationsSchema.findById(operationId);
      await OperationsSchema.findByIdAndUpdate(operationId, {
        infoText: `${operation?.infoText || ''}\nОбработано ${i + batch.length} из ${events.length} событий. Батч ${batchNumber} из ${Math.ceil(events.length / BATCH_SIZE)}`,
      });
    }
    
    // Финальное обновление операции
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'success',
      finish_time: new Date(),
      statistics: JSON.stringify({
        total: events.length,
        batches: Math.ceil(events.length / BATCH_SIZE),
        errors: errorTexts.length,
      }),
      errorText: errorTexts.join('\n'),
    });
  } catch (error) {
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: error.message || 'Unknown error while saving events',
      finish_time: new Date(),
    });
  }
}

export default parseFienta;

