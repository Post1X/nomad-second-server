import https from 'https';
import http from 'http';
import { URL } from 'url';
import CountriesSchema from '../schemas/CountriesSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { ENV } from '../helpers/constants';
import { rebuildAiPromptIfNeeded } from './AiCategoryServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('DICT_SYNC');

const requestJson = (urlString, headers = {}) => new Promise((resolve, reject) => {
  const url = new URL(urlString);
  const isHttps = url.protocol === 'https:';
  const mod = isHttps ? https : http;
  const req = mod.request({
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      ...headers,
    },
  }, (res) => {
    let data = '';
    res.on('data', (chunk) => { data += chunk; });
    res.on('end', () => {
      try {
        if (res.statusCode !== 200) {
          reject(new Error(`Main server HTTP ${res.statusCode}: ${data.slice(0, 400)}`));
          return;
        }
        resolve(JSON.parse(data));
      } catch (e) {
        reject(e);
      }
    });
  });
  req.on('error', reject);
  req.end();
});

const upsertById = async (Model, items, mapFn) => {
  let upserted = 0;
  for (const item of items) {
    if (!item?._id) continue;
    const doc = mapFn(item);
    await Model.findByIdAndUpdate(item._id, { $set: doc }, { upsert: true, setDefaultsOnInsert: true });
    upserted += 1;
  }
  return upserted;
};

const deleteMissingById = async (Model, remoteItems) => {
  const remoteIds = new Set(
    (remoteItems || []).map((item) => String(item?._id)).filter(Boolean),
  );
  if (remoteIds.size === 0) {
    return 0;
  }
  const local = await Model.find({}).select('_id').lean();
  const toDelete = local.filter((doc) => !remoteIds.has(String(doc._id))).map((doc) => doc._id);
  if (!toDelete.length) {
    return 0;
  }
  const result = await Model.deleteMany({ _id: { $in: toDelete } });
  return result.deletedCount || 0;
};

class DictSyncServices {
  static async pullFromMainServer() {
    const mainUrl = ENV.MAIN_SERVER_URL;
    const apiKey = ENV.MAIN_SERVER_API_KEY || ENV.PARSING_SERVER_API_KEY;

    if (!mainUrl) {
      throw new Error('MAIN_SERVER_URL is not set');
    }
    if (!apiKey) {
      throw new Error('MAIN_SERVER_API_KEY (or PARSING_SERVER_API_KEY) is not set');
    }

    const base = mainUrl.replace(/\/$/, '');
    const data = await requestJson(`${base}/api/parsing-dict/sync`, {
      'X-Api-Key': apiKey,
    });

    if (data.status !== 'ok') {
      throw new Error(`Main server returned status: ${data.status}`);
    }

    const countries = data.countries || [];
    const cities = data.cities || [];
    const eventCategories = data.eventCategories || data.categories || [];

    const countriesUpserted = await upsertById(CountriesSchema, countries, (c) => ({
      name: c.name,
      flag_url: c.flag_url || '',
      keywords: Array.isArray(c.keywords) ? c.keywords : [],
    }));

    const citiesUpserted = await upsertById(CitiesSchema, cities, (c) => ({
      country_id: c.country_id,
      name: c.name,
      sort: c.sort ?? 999,
      coordinates: c.coordinates || { lat: '0', lon: '0' },
      keywords: Array.isArray(c.keywords) ? c.keywords : [],
    }));

    const categoriesUpserted = await upsertById(EventsCategoriesSchema, eventCategories, (c) => ({
      name: c.name,
      sort: c.sort ?? 999,
      keywords: Array.isArray(c.keywords) ? c.keywords : [],
    }));

    const countriesDeleted = await deleteMissingById(CountriesSchema, countries);
    const citiesDeleted = await deleteMissingById(CitiesSchema, cities);
    const categoriesDeleted = await deleteMissingById(EventsCategoriesSchema, eventCategories);

    const promptResult = await rebuildAiPromptIfNeeded();

    const stats = {
      countriesUpserted,
      citiesUpserted,
      categoriesUpserted,
      countriesDeleted,
      citiesDeleted,
      categoriesDeleted,
      aiPromptUpdated: promptResult.updated,
    };
    logger.info(`Dict sync done: ${JSON.stringify(stats)}`);
    return stats;
  }
}

export default DictSyncServices;
