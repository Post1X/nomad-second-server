import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import CitySuggestions from '../schemas/CitySuggestionsSchema';
import { ENV, EVENT_SOURCE } from '../helpers/constants';
import {
  findExactCityMatch,
  findPossibleDuplicate,
  isGarbageCityName,
  normalizeCityKey,
} from '../helpers/cityDiscoveryNormalize';
import { createLoggerWithSource } from '../helpers/logger';
import { requestJson } from './cityDiscovery/http';
import discoverKontramarkaCities from './cityDiscovery/discoverKontramarka';
import discoverEventimCities from './cityDiscovery/discoverEventim';
import discoverIsraelinfoCities from './cityDiscovery/discoverIsraelinfo';
import discoverTicketmasterCities from './cityDiscovery/discoverTicketmaster';

const logger = createLoggerWithSource('CITY_DISCOVERY');

const SUPPORTED = {
  [EVENT_SOURCE.kontramarka]: discoverKontramarkaCities,
  [EVENT_SOURCE.eventim]: discoverEventimCities,
  [EVENT_SOURCE.israelinfo]: discoverIsraelinfoCities,
  [EVENT_SOURCE.ticketmaster]: discoverTicketmasterCities,
};

class CityDiscoveryServices {
  static supportedSources() {
    return Object.keys(SUPPORTED);
  }

  static async upsertCandidates(source, candidates = []) {
    const cities = await CitiesSchema.find({}).lean();
    const now = new Date();
    let created = 0;
    let updated = 0;
    let alreadyInDb = 0;
    let garbageSkipped = 0;
    let revived = 0;

    for (const c of candidates) {
      const raw_name = String(c.raw_name || '').trim();
      if (isGarbageCityName(raw_name)) {
        garbageSkipped += 1;
        // eslint-disable-next-line no-continue
        continue;
      }

      if (findExactCityMatch(cities, raw_name)) {
        alreadyInDb += 1;
        // eslint-disable-next-line no-continue
        continue;
      }

      const normalized_key = normalizeCityKey(raw_name);
      const dup = findPossibleDuplicate(cities, raw_name);
      const hitBump = Number(c.hit_count) > 0 ? Number(c.hit_count) : 1;

      // eslint-disable-next-line no-await-in-loop
      const existing = await CitySuggestions.findOne({ source, normalized_key });
      if (existing) {
        existing.raw_name = raw_name;
        existing.slug = c.slug || existing.slug || '';
        existing.source_url = c.source_url || existing.source_url || '';
        existing.last_seen_at = now;
        existing.hit_count = (existing.hit_count || 0) + hitBump;
        existing.possible_duplicate_of = dup?._id || undefined;
        existing.possible_duplicate_name = dup?.name || '';
        if (existing.status === 'rejected') {
          existing.status = 'pending';
          existing.reject_reason = '';
          revived += 1;
        }
        // eslint-disable-next-line no-await-in-loop
        await existing.save();
        updated += 1;
      } else {
        // eslint-disable-next-line no-await-in-loop
        await CitySuggestions.create({
          source,
          raw_name,
          normalized_key,
          slug: c.slug || '',
          source_url: c.source_url || '',
          status: 'pending',
          hit_count: hitBump,
          first_seen_at: now,
          last_seen_at: now,
          possible_duplicate_of: dup?._id,
          possible_duplicate_name: dup?.name || '',
        });
        created += 1;
      }
    }

    return {
      created,
      updated,
      alreadyInDb,
      garbageSkipped,
      revived,
      candidatesSeen: candidates.length,
    };
  }

  static async discover(source, options = {}) {
    const fn = SUPPORTED[source];
    if (!fn) {
      if (source === EVENT_SOURCE.fienta) {
        return {
          status: 'unsupported',
          source,
          message: 'Fienta has no city index — use parse run: unknown locations are collected into CitySuggestions automatically.',
          upsert: {
            created: 0, updated: 0, alreadyInDb: 0, garbageSkipped: 0, revived: 0, candidatesSeen: 0,
          },
          meta: { method: 'none' },
        };
      }
      throw new Error(`Unsupported discovery source: ${source}`);
    }

    logger.info(`Discover start: ${source}`);
    const { candidates, meta } = await fn(options);
    const upsert = await CityDiscoveryServices.upsertCandidates(source, candidates);
    logger.info(`Discover done: ${source} ${JSON.stringify({ ...upsert, meta })}`);
    return {
      status: 'ok',
      source,
      upsert,
      meta,
    };
  }

  static async listSuggestions({
    source, status = 'pending', page = 1, per_page = 50, q = '',
  } = {}) {
    const filter = {};
    if (status) filter.status = status;
    if (source) filter.source = source;
    if (q) {
      filter.raw_name = { $regex: String(q).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' };
    }

    const skip = (Math.max(1, page) - 1) * per_page;
    const [items, total] = await Promise.all([
      CitySuggestions.find(filter)
        .sort({ hit_count: -1, last_seen_at: -1 })
        .skip(skip)
        .limit(per_page)
        .lean(),
      CitySuggestions.countDocuments(filter),
    ]);

    return { items, total, page, per_page };
  }

  static async metrics() {
    const [byStatus, bySource, withDupHint, citiesCount, countriesCount] = await Promise.all([
      CitySuggestions.aggregate([
        { $group: { _id: '$status', count: { $sum: 1 } } },
      ]),
      CitySuggestions.aggregate([
        { $match: { status: 'pending' } },
        { $group: { _id: '$source', count: { $sum: 1 }, hits: { $sum: '$hit_count' } } },
        { $sort: { count: -1 } },
      ]),
      CitySuggestions.countDocuments({
        status: 'pending',
        possible_duplicate_of: { $ne: null },
      }),
      CitiesSchema.countDocuments({}),
      CountriesSchema.countDocuments({}),
    ]);

    const statusMap = Object.fromEntries(byStatus.map((r) => [r._id, r.count]));
    return {
      pending: statusMap.pending || 0,
      rejected: statusMap.rejected || 0,
      pendingWithDuplicateHint: withDupHint,
      bySource: bySource.map((r) => ({
        source: r._id,
        pending: r.count,
        hits: r.hits,
      })),
      dict: {
        cities: citiesCount,
        countries: countriesCount,
      },
      supportedSources: CityDiscoveryServices.supportedSources(),
    };
  }

  static async reject(id, reason = '') {
    const doc = await CitySuggestions.findById(id);
    if (!doc) {
      const err = new Error('Suggestion not found');
      err.status = 404;
      throw err;
    }
    doc.status = 'rejected';
    doc.reject_reason = String(reason || '').slice(0, 500);
    await doc.save();
    return doc.toObject();
  }

  static async approve(id, {
    name, country_id, lat, lon,
  }) {
    const suggestion = await CitySuggestions.findById(id);
    if (!suggestion) {
      const err = new Error('Suggestion not found');
      err.status = 404;
      throw err;
    }

    const cityName = String(name || suggestion.raw_name || '').trim();
    if (!cityName) {
      const err = new Error('name is required');
      err.status = 400;
      throw err;
    }
    if (!country_id) {
      const err = new Error('country_id is required');
      err.status = 400;
      throw err;
    }
    if (lat === undefined || lat === null || lat === ''
      || lon === undefined || lon === null || lon === '') {
      const err = new Error('lat and lon are required');
      err.status = 400;
      throw err;
    }

    const country = await CountriesSchema.findById(country_id).lean();
    if (!country) {
      const err = new Error('country_id not found in local Countries');
      err.status = 400;
      throw err;
    }

    const exact = findExactCityMatch(await CitiesSchema.find({}).lean(), cityName);
    if (exact) {
      const err = new Error(`City already exists exactly: ${exact.name} (${exact._id})`);
      err.status = 409;
      throw err;
    }

    const mainUrl = (ENV.MAIN_SERVER_URL || '').replace(/\/$/, '');
    const apiKey = ENV.MAIN_SERVER_API_KEY || ENV.PARSING_SERVER_API_KEY;
    if (!mainUrl || !apiKey) {
      const err = new Error('MAIN_SERVER_URL / MAIN_SERVER_API_KEY not configured');
      err.status = 500;
      throw err;
    }

    const { statusCode, data } = await requestJson(`${mainUrl}/api/parsing-dict/cities`, {
      method: 'POST',
      headers: { 'X-Api-Key': apiKey },
      body: JSON.stringify({
        name: cityName,
        country_id: String(country_id),
        lat: String(lat),
        lon: String(lon),
      }),
    });

    if (statusCode !== 200 || data?.status !== 'ok' || !data?.city?._id) {
      const err = new Error(
        `Main create city failed HTTP ${statusCode}: ${JSON.stringify(data).slice(0, 400)}`,
      );
      err.status = 502;
      throw err;
    }

    const remote = data.city;
    await CitiesSchema.findByIdAndUpdate(
      remote._id,
      {
        $set: {
          country_id: remote.country_id || country_id,
          name: remote.name || cityName,
          sort: remote.sort ?? 999,
          coordinates: remote.coordinates || { lat: String(lat), lon: String(lon) },
        },
      },
      { upsert: true, setDefaultsOnInsert: true },
    );

    await CitySuggestions.deleteOne({ _id: suggestion._id });

    const local = await CitiesSchema.findById(remote._id).lean();
    logger.info(`Approved city ${cityName} → main+local ${remote._id}`);
    return {
      city: local,
      suggestion_id: String(suggestion._id),
      source: suggestion.source,
      raw_name: suggestion.raw_name,
    };
  }
}

export default CityDiscoveryServices;
