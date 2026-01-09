import fs from 'fs';
import path from 'path';
import moment from 'moment';
import https from 'https';
import http from 'http';
import { pipeline } from 'stream/promises';
import { createGunzip } from 'zlib';
import { exec } from 'child_process';
import { promisify } from 'util';
import { URL } from 'url';
import CitiesSchema from '../schemas/CitiesSchema';
import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSE_EVENTIM');

const execPromise = promisify(exec);

const citiesCache = {
  list: null,
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

const loadCities = async () => {
  if (citiesCache.list) return citiesCache.list;
  citiesCache.list = await CitiesSchema.find({}).lean();
  return citiesCache.list;
};

const downloadFile = async (url, destPath, password = null, username = null) => {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    let urlObj;
    
    if (password && !username) {
      try {
        urlObj = new URL(url);
        urlObj.username = password;
        urlObj.password = password;
        url = urlObj.toString();
      } catch (e) {
      }
    }
    
    urlObj = new URL(url);
    const isHttps = urlObj.protocol === 'https:';
    const httpModule = isHttps ? https : http;

    const options = {
      hostname: urlObj.hostname,
      port: urlObj.port || (isHttps ? 443 : 80),
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
    };

    if (password) {
      const authUser = username !== null ? username : '';
      const auth = Buffer.from(`${authUser}:${password}`).toString('base64');
      options.headers.Authorization = `Basic ${auth}`;
    }

    const req = httpModule.request(options, (response) => {
      if (response.statusCode === 301 || response.statusCode === 302) {
        file.close();
        fs.unlinkSync(destPath);
        return downloadFile(response.headers.location, destPath, password, username).then(resolve).catch(reject);
      }
      if (response.statusCode !== 200) {
        file.close();
        fs.unlinkSync(destPath);
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      response.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve();
      });
    });

    req.on('error', (err) => {
      file.close();
      if (fs.existsSync(destPath)) {
        fs.unlinkSync(destPath);
      }
      reject(err);
    });

    req.end();
  });
};

const extractGz = async (gzPath, extractPath) => {
  return new Promise((resolve, reject) => {
    try {
      const gzFileName = path.basename(gzPath);
      let fileName = path.basename(gzPath, '.gz');
      
      if (fileName.endsWith('.json')) {
        fileName = fileName;
      } else if (gzFileName.includes('.json.gz')) {
        fileName = gzFileName.replace('.gz', '');
      } else {
        fileName = `${fileName}.json`;
      }
      
      const outputPath = path.join(extractPath, fileName);
      
      const readStream = fs.createReadStream(gzPath);
      const writeStream = fs.createWriteStream(outputPath);
      const gunzip = createGunzip();
      
      readStream
        .pipe(gunzip)
        .pipe(writeStream)
        .on('finish', () => {
          resolve(outputPath);
        })
        .on('error', (err) => {
          reject(new Error(`Failed to extract gz: ${err.message}`));
        });
    } catch (e) {
      reject(new Error(`Failed to extract gz: ${e.message}`));
    }
  });
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

async function parseEventim({ meta, operationId }) {
  const events = [];
  const errorTexts = [];

  try {
    const {
      adminId, countryId, cityId, cityName: metaCityName,
      eventimUrl,
    } = meta || {};
    
    const cities = await loadCities();

    await logProgress(operationId, 'Starting Eventim parsing...');

    let raw;
    let extractedPath = null;
    const rawPath = path.join(__dirname, 'eventim.json');
    const extractPath = __dirname;
    const eventimUrlDefault = process.env.EVENTIM_URL;
    const eventimPassword = process.env.EVENTIM_PASSWORD || '';
    const eventimUsername = process.env.EVENTIM_USERNAME || '';
    const urlToUse = eventimUrl || eventimUrlDefault;

    try {
      await logProgress(operationId, `Downloading Eventim file from ${urlToUse}...`);
      const urlObj = new URL(urlToUse);
      const urlFileName = path.basename(urlObj.pathname) || 'eventim.json.gz';
      const gzPath = path.join(__dirname, urlFileName);
      
      await downloadFile(urlToUse, gzPath, eventimPassword, eventimUsername);
      await logProgress(operationId, 'Extracting archive...');
      extractedPath = await extractGz(gzPath, extractPath);
      if (fs.existsSync(gzPath)) {
        fs.unlinkSync(gzPath);
      }

      const extractedFileName = path.basename(extractedPath);

      if (extractedFileName.endsWith('.nml')) {
        const nmlContent = fs.readFileSync(extractedPath, 'utf8');
        try {
          const parsed = JSON.parse(nmlContent);
          raw = JSON.stringify(parsed);
        } catch (e) {
          errorTexts.push(`NML file is not valid JSON, trying to parse as XML: ${e.message}`);
          throw new Error('NML file parsing not implemented yet');
        }
      } else if (extractedFileName.endsWith('.json')) {
        raw = fs.readFileSync(extractedPath, 'utf8');
      } else if (fs.existsSync(rawPath)) {
        raw = fs.readFileSync(rawPath, 'utf8');
        errorTexts.push('Using cached eventim.json file');
      } else {
        throw new Error('No NML or JSON file found in extracted archive');
      }
      await logProgress(operationId, 'File downloaded and extracted successfully');
    } catch (downloadErr) {
      errorTexts.push(`Failed to download/extract Eventim file: ${downloadErr.message}`);
      if (fs.existsSync(rawPath)) {
        raw = fs.readFileSync(rawPath, 'utf8');
        errorTexts.push('Using cached eventim.json file as fallback');
        await logProgress(operationId, 'Using cached file as fallback');
      } else {
        throw downloadErr;
      }
    }

    await logProgress(operationId, 'Parsing Eventim data...');
    const { eventserie = [] } = JSON.parse(raw);

    for (const series of eventserie) {
      const photoUrl = series.esPictureBig || series.esPicture || series.esPictureSmall || null;
      for (const event of series.events || []) {
        const dateStart = event.eventDateIso8601 ? new Date(event.eventDateIso8601) : null;
        const holdingDate = dateStart ? dateStart.toISOString() : '';
        const addressParts = [
          event.eventVenue,
          event.eventStreet,
          event.eventZip,
          event.eventCity,
        ].filter(Boolean);
        const address = addressParts.join(', ');

        const targetCity = event.eventCity || metaCityName || '';
        const matchedCity = findCity(cities, targetCity);
        const fallbackCoords = parseCoordinatesField(matchedCity?.coordinates);
        const resolvedCityId = cityId || matchedCity?._id || null;
        const resolvedCountryId = countryId || matchedCity?.country_id || null;

        if (!resolvedCityId || !resolvedCountryId) {
          errorTexts.push(`Skip event "${event.eventName || series.esName}" – city/country id missing; pass meta.cityId/meta.countryId or ensure city exists in DB. [DEBUG targetCity="${targetCity}" matched="${matchedCity?.name || 'null'}" matchedCityId="${matchedCity?._id || '-'}" matchedCountryId="${matchedCity?.country_id || '-'}" providedCityId="${cityId || '-'}" providedCountryId="${countryId || '-'}"]`);
          continue;
        }

        const newEvent = {
          name: event.eventName || series.esName,
          description: series.esText || event.eventName || '',
          specialization: 'Event',
          admin_id: adminId,
          country_id: resolvedCountryId,
          city_id: resolvedCityId,
          contacts: { website: event.eventLink || series.esLink || '' },
          photos: photoUrl ? [{ full_url: photoUrl }] : [],
          holding_date: holdingDate,
          date_start: dateStart,
          date_end: dateStart,
          source: EVENT_SOURCE.eventim,
          address,
        };

        if (typeof event.venueLatitude === 'number' && typeof event.venueLongitude === 'number') {
          newEvent.lat = event.venueLatitude;
          newEvent.lon = event.venueLongitude;
          newEvent.is_special_point_on_map = false;
        } else if (fallbackCoords?.lat && fallbackCoords?.lon) {
          newEvent.lat = fallbackCoords.lat;
          newEvent.lon = fallbackCoords.lon;
          newEvent.is_special_point_on_map = fallbackCoords.is_special_point_on_map;
        }

        if (typeof event.minPrice === 'number') newEvent.min_price = event.minPrice;
        if (typeof event.maxPrice === 'number') newEvent.max_price = event.maxPrice;

        events.push(newEvent);
      }
    }

    if (events.length > 5) {
      events.splice(5);
      errorTexts.push(`Limited to 5 events for testing (total parsed: ${events.length})`);
    }

    if (extractedPath && fs.existsSync(extractedPath)) {
      try {
        fs.unlinkSync(extractedPath);
      } catch (unlinkErr) {
        errorTexts.push(`Failed to delete extracted file: ${unlinkErr.message}`);
      }
    }

    await logProgress(operationId, `Parsing completed. Total: ${events.length} events parsed`);
  } catch (e) {
    const errMsg = e?.message || 'Unknown error while parsing Eventim';
    errorTexts.push(errMsg);
    await logProgress(operationId, `FATAL ERROR: ${errMsg}`);
  }

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
      
      const operation = await OperationsSchema.findById(operationId);
      await OperationsSchema.findByIdAndUpdate(operationId, {
        infoText: `${operation?.infoText || ''}\nОбработано ${i + batch.length} из ${events.length} событий. Батч ${batchNumber} из ${Math.ceil(events.length / BATCH_SIZE)}`,
      });
    }
    
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

export default parseEventim;

