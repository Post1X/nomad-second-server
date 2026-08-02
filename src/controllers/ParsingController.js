import path from 'path';
import ParseRunsSchema from '../schemas/ParseRunsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import CountriesSchema from '../schemas/CountriesSchema';
import FientaPagesSchema from '../schemas/FientaPagesSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import CleanupServices from '../services/CleanupServices';
import StatsServices from '../services/StatsServices';
import BackfillStatsServices from '../services/BackfillStatsServices';
import CityDiscoveryServices from '../services/CityDiscoveryServices';
import CategorySuggestionServices from '../services/CategorySuggestionServices';
import CategorySuggestionsBackfillServices from '../services/CategorySuggestionsBackfillServices';
import CategorySuggestionsConsolidateServices from '../services/CategorySuggestionsConsolidateServices';
import { categorizeBatch } from '../services/CategorizeBatchServices';
import { lookupParsedEvents } from '../services/LookupParsedEventsServices';
import { enrichFromTicketmaster } from '../services/EnrichTicketmasterServices';
import { rebuildAiPromptIfNeeded } from '../services/AiCategoryServices';
import {
  EVENT_SOURCE,
  EXPIRED_EVENTS_CLEANUP_MONTHS,
  OPERATION_TYPES,
  SOURCE_BY_OPERATION_TYPE,
} from '../helpers/constants';
import startParseRun from '../helpers/startParseRun';
import {
  getCronStatus,
  runCronJob,
  setCronJobEnabled,
  setParsingCronEnabled,
} from '../helpers/cron';
import { requestParseRunStop } from '../helpers/logParseRun';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSING_CONTROLLER');

const resolveSource = (typeOrSource) => {
  if (!typeOrSource) return null;
  if (SOURCE_BY_OPERATION_TYPE[typeOrSource]) return SOURCE_BY_OPERATION_TYPE[typeOrSource];
  if (Object.values(EVENT_SOURCE).includes(typeOrSource) && typeOrSource !== EVENT_SOURCE.nomad) {
    return typeOrSource;
  }
  return null;
};

class ParsingController {
  static create = async (req, res, next) => {
    try {
      const { type, source, meta } = req.body;
      const typeOrSource = type || source;

      if (!typeOrSource) {
        return res.status(400).json({
          status: 'error',
          message: 'type or source is required',
        });
      }

      const validTypes = Object.values(OPERATION_TYPES);
      const validSources = Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);
      if (!validTypes.includes(typeOrSource) && !validSources.includes(typeOrSource)) {
        return res.status(400).json({
          status: 'error',
          message: `Invalid type/source. Must be one of: ${[...validTypes, ...validSources].join(', ')}`,
        });
      }

      const runId = await startParseRun(typeOrSource, meta || {});

      res.json({
        status: 'ok',
        runId: runId.toString(),
        message: 'Parse run created and started',
      });
    } catch (error) {
      if (error?.status) {
        return res.status(error.status).json({
          status: 'error',
          message: error.message,
          activeRunId: error.activeRunId ? String(error.activeRunId) : undefined,
        });
      }
      next(error);
    }
  };

  static getCron = async (req, res, next) => {
    try {
      const cronStatus = await getCronStatus();
      res.json({ status: 'ok', cron: cronStatus });
    } catch (error) {
      next(error);
    }
  };

  static stopCron = async (req, res, next) => {
    try {
      await setParsingCronEnabled(false);
      res.json({ status: 'ok', cron: await getCronStatus() });
    } catch (error) {
      next(error);
    }
  };

  static startCron = async (req, res, next) => {
    try {
      await setParsingCronEnabled(true);
      res.json({ status: 'ok', cron: await getCronStatus() });
    } catch (error) {
      next(error);
    }
  };

  static setCronJob = async (req, res, next) => {
    try {
      const { jobId } = req.params;
      const enabled = req.body?.enabled;
      if (typeof enabled !== 'boolean') {
        return res.status(400).json({ status: 'error', message: 'body.enabled boolean required' });
      }
      await setCronJobEnabled(jobId, enabled);
      res.json({ status: 'ok', cron: await getCronStatus() });
    } catch (error) {
      if (error?.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static runCronJobNow = async (req, res, next) => {
    try {
      const { jobId } = req.params;
      const result = await runCronJob(jobId, {
        force: true,
        ignoreEnabled: true,
      });
      res.json({ status: 'ok', result, cron: await getCronStatus() });
    } catch (error) {
      if (error?.status) {
        return res.status(error.status).json({
          status: 'error',
          message: error.message,
          activeRunId: error.activeRunId ? String(error.activeRunId) : undefined,
        });
      }
      next(error);
    }
  };

  static stopParseRun = async (req, res, next) => {
    try {
      const { runId } = req.params;
      const run = await requestParseRunStop(runId);
      res.json({ status: 'ok', run });
    } catch (error) {
      if (error?.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static getEvents = async (req, res, next) => {
    try {
      const {
        source: sourceParam,
        type,
        page: pageParam,
        per_page: perPageParam,
        updatedSince,
        onlyPending,
      } = req.query;

      const source = resolveSource(sourceParam || type);
      if (!source) {
        return res.status(400).json({
          status: 'error',
          message: 'Parameter "source" (or legacy "type") is required',
        });
      }

      const page = Math.max(1, parseInt(String(pageParam || 1), 10) || 1);
      const per_page = Math.max(1, Math.min(100, parseInt(String(perPageParam || 20), 10) || 20));

      const filter = { source };
      if (updatedSince) {
        const since = new Date(updatedSince);
        if (!Number.isNaN(since.getTime())) {
          filter.updatedAt = { $gte: since };
        }
      } else if (String(onlyPending || 'true') !== 'false') {
        filter.$or = [
          { exported_at: null },
          { $expr: { $gt: ['$updatedAt', '$exported_at'] } },
        ];
      }

      const totalEvents = await ParsedEventsSchema.countDocuments(filter);
      const totalPages = Math.max(1, Math.ceil(totalEvents / per_page) || 1);
      const skip = (page - 1) * per_page;

      const docs = await ParsedEventsSchema.find(filter)
        .sort({ updatedAt: 1 })
        .skip(skip)
        .limit(per_page)
        .lean();

      const events = docs.map((pe) => ({
        ...(pe.event_data || {}),
        _parsed_event_id: pe._id.toString(),
        source: pe.source,
        updatedAt: pe.updatedAt,
        fingerprint: pe.fingerprint,
        parser_unique_id: pe.parser_unique_id || pe.event_data?.parser_unique_id || null,
      }));

      res.json({
        status: 'ok',
        source,
        events,
        totalEvents,
        totalPages,
        page,
        per_page,
        ids: docs.map((d) => d._id.toString()),
      });
    } catch (error) {
      next(error);
    }
  };

  /**
   * Browse all ParsedEvents for admin UI (not pull pipeline).
   * source optional; no onlyPending filter.
   */
  static browseEvents = async (req, res, next) => {
    try {
      const {
        source: sourceParam,
        type,
        page: pageParam,
        per_page: perPageParam,
        q,
      } = req.query;

      const page = Math.max(1, parseInt(String(pageParam || 1), 10) || 1);
      const per_page = Math.max(1, Math.min(100, parseInt(String(perPageParam || 24), 10) || 24));

      const filter = {};
      if (sourceParam || type) {
        const source = resolveSource(sourceParam || type);
        if (!source) {
          return res.status(400).json({
            status: 'error',
            message: 'Invalid source. Use eventim|ticketmaster|israelinfo|kontramarka|fienta or omit for all',
          });
        }
        filter.source = source;
      }

      const query = String(q || '').trim();
      if (query) {
        filter['event_data.name'] = { $regex: query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' };
      }

      const [totalEvents, docs, countsAgg] = await Promise.all([
        ParsedEventsSchema.countDocuments(filter),
        ParsedEventsSchema.find(filter)
          .sort({ 'event_data.date_start': -1, updatedAt: -1 })
          .skip((page - 1) * per_page)
          .limit(per_page)
          .lean(),
        ParsedEventsSchema.aggregate([
          { $group: { _id: '$source', count: { $sum: 1 } } },
        ]),
      ]);

      const totalPages = Math.max(1, Math.ceil(totalEvents / per_page) || 1);
      const countsBySource = {};
      for (const row of countsAgg) {
        if (row._id) countsBySource[row._id] = row.count;
      }

      const events = docs.map((pe) => ({
        ...(pe.event_data || {}),
        _parsed_event_id: pe._id.toString(),
        source: pe.source,
        updatedAt: pe.updatedAt,
        createdAt: pe.createdAt,
        fingerprint: pe.fingerprint,
        exported_at: pe.exported_at || null,
        parser_unique_id: pe.parser_unique_id || pe.event_data?.parser_unique_id || null,
      }));

      res.json({
        status: 'ok',
        source: filter.source || null,
        events,
        totalEvents,
        totalPages,
        page,
        per_page,
        countsBySource,
      });
    } catch (error) {
      next(error);
    }
  };

  static ackEvents = async (req, res, next) => {
    try {
      const { source: sourceParam, type, ids = [], exportedUntil } = req.body || {};
      const source = resolveSource(sourceParam || type);
      if (!source) {
        return res.status(400).json({
          status: 'error',
          message: 'source (or type) is required',
        });
      }

      const now = new Date();
      let result;

      if (Array.isArray(ids) && ids.length) {
        result = await ParsedEventsSchema.updateMany(
          { source, _id: { $in: ids } },
          { $set: { exported_at: now } },
        );
      } else if (exportedUntil) {
        const until = new Date(exportedUntil);
        result = await ParsedEventsSchema.updateMany(
          { source, updatedAt: { $lte: until } },
          { $set: { exported_at: now } },
        );
      } else {
        return res.status(400).json({
          status: 'error',
          message: 'ids[] or exportedUntil is required',
        });
      }

      res.json({
        status: 'ok',
        modified: result.modifiedCount,
        source,
      });
    } catch (error) {
      next(error);
    }
  };

  static getRuns = async (req, res, next) => {
    try {
      const {
        source: sourceParam,
        type,
        page: pageParam,
        per_page: perPageParam,
        from,
        to,
        status,
      } = req.query;

      const page = Math.max(1, parseInt(String(pageParam || 1), 10) || 1);
      const per_page = Math.max(1, Math.min(100, parseInt(String(perPageParam || 20), 10) || 20));
      const filter = {};

      const sourceRaw = sourceParam || type;
      if (sourceRaw) {
        const list = String(sourceRaw).split(',').map((s) => s.trim()).filter(Boolean)
          .map(resolveSource)
          .filter(Boolean);
        if (list.length === 1) filter.source = list[0];
        else if (list.length > 1) filter.source = { $in: list };
      }

      if (status) filter.status = status;
      if (from || to) {
        filter.createdAt = {};
        if (from) filter.createdAt.$gte = new Date(from);
        if (to) filter.createdAt.$lt = new Date(to);
      }

      const total = await ParseRunsSchema.countDocuments(filter);
      const runs = await ParseRunsSchema.find(filter)
        .sort({ createdAt: -1 })
        .skip((page - 1) * per_page)
        .limit(per_page)
        .lean();

      res.json({
        status: 'ok',
        runs,
        total,
        page,
        per_page,
      });
    } catch (error) {
      next(error);
    }
  };

  static getResults = async (req, res, next) => {
    try {
      const { runId } = req.params;
      const run = await ParseRunsSchema.findById(runId).lean();
      if (!run) {
        return res.status(404).json({
          status: 'error',
          message: 'Parse run not found',
        });
      }

      const parsedEvents = await ParsedEventsSchema.find({ parse_run: runId }).lean();
      res.json({
        status: 'ok',
        run,
        events: parsedEvents.map((pe) => pe.event_data),
        totalEvents: parsedEvents.length,
      });
    } catch (error) {
      next(error);
    }
  };

  static cleanup = async (req, res, next) => {
    try {
      const months = Number(req.body?.months) > 0
        ? Number(req.body.months)
        : EXPIRED_EVENTS_CLEANUP_MONTHS;

      const result = await CleanupServices.cleanupExpiredEvents(months);

      res.json({
        status: 'ok',
        deletedCount: result.deletedEvents,
        message: 'Expired events cleanup completed',
        months,
      });
    } catch (error) {
      next(error);
    }
  };

  static categorizeBatch = async (req, res, next) => {
    try {
      const {
        events,
        source = 'backfill',
        purpose,
        persist,
        meta,
      } = req.body || {};
      if (!Array.isArray(events) || !events.length) {
        return res.status(400).json({
          status: 'error',
          message: 'events array is required',
        });
      }
      if (events.length > 500) {
        return res.status(400).json({
          status: 'error',
          message: 'At most 500 events per request',
        });
      }

      const out = await categorizeBatch(events, source, {
        purpose: purpose || (source === 'backfill' ? 'backfill' : undefined),
        persist,
        meta,
      });
      res.json({
        status: 'ok',
        results: out.results,
        openaiUsage: out.openaiUsage,
        statistics: out.statistics,
        runId: out.runId,
      });
    } catch (error) {
      next(error);
    }
  };

  static lookupEvents = async (req, res, next) => {
    try {
      const { items, events } = req.body || {};
      const list = Array.isArray(items) ? items : (Array.isArray(events) ? events : null);
      if (!list?.length) {
        return res.status(400).json({
          status: 'error',
          message: 'items (or events) array is required',
        });
      }
      if (list.length > 500) {
        return res.status(400).json({
          status: 'error',
          message: 'At most 500 items per request',
        });
      }
      const results = await lookupParsedEvents(list);
      res.json({ status: 'ok', results });
    } catch (error) {
      next(error);
    }
  };

  static enrichTicketmaster = async (req, res, next) => {
    try {
      const { items, events } = req.body || {};
      const list = Array.isArray(items) ? items : (Array.isArray(events) ? events : null);
      if (!list?.length) {
        return res.status(400).json({
          status: 'error',
          message: 'items (or events) array is required',
        });
      }
      if (list.length > 100) {
        return res.status(400).json({
          status: 'error',
          message: 'At most 100 items per request',
        });
      }
      const results = await enrichFromTicketmaster(list);
      res.json({ status: 'ok', results });
    } catch (error) {
      next(error);
    }
  };

  static getBackfillStats = async (req, res, next) => {
    try {
      const { source, sources, from, to } = req.query;
      const sourceList = sources
        ? String(sources).split(',').map((s) => s.trim()).filter(Boolean)
        : undefined;
      const stats = await BackfillStatsServices.getBackfillStats({
        source,
        sources: sourceList,
        from,
        to,
      });
      res.json({
        status: 'ok',
        ...stats,
      });
    } catch (error) {
      next(error);
    }
  };

  static syncCitiesAndCountries = async (req, res, next) => {
    try {
      const {
        countries = [],
        cities = [],
        eventCategories = [],
        replaceAll = false,
      } = req.body;

      if (!Array.isArray(countries) || !Array.isArray(cities)) {
        return res.status(400).json({
          status: 'error',
          message: 'countries and cities must be arrays',
        });
      }
      if (eventCategories != null && !Array.isArray(eventCategories)) {
        return res.status(400).json({
          status: 'error',
          message: 'eventCategories must be an array when provided',
        });
      }

      let countriesCreated = 0;
      let citiesCreated = 0;
      let countriesDeleted = 0;
      let citiesDeleted = 0;
      let categoriesUpserted = 0;

      if (replaceAll) {
        const deleteCountriesResult = await CountriesSchema.deleteMany({});
        countriesDeleted = deleteCountriesResult.deletedCount;
        if (countries.length > 0) {
          await CountriesSchema.insertMany(
            countries.map((country) => ({
              _id: country._id,
              name: country.name,
              flag_url: country.flag_url || '',
            })),
            { ordered: false },
          ).catch((err) => {
            if (err.code !== 11000) throw err;
          });
          countriesCreated = countries.length;
        }
      } else if (countries.length > 0) {
        const existingCountryIds = await CountriesSchema.find({}).select('_id').lean();
        const existingIdsSet = new Set(existingCountryIds.map((c) => c._id.toString()));
        const newCountries = countries.filter((c) => !existingIdsSet.has(c._id.toString()));
        if (newCountries.length > 0) {
          await CountriesSchema.insertMany(
            newCountries.map((country) => ({
              _id: country._id,
              name: country.name,
              flag_url: country.flag_url || '',
            })),
            { ordered: false },
          ).catch((err) => {
            if (err.code !== 11000) throw err;
          });
          countriesCreated = newCountries.length;
        }
      }

      if (replaceAll) {
        const deleteCitiesResult = await CitiesSchema.deleteMany({});
        citiesDeleted = deleteCitiesResult.deletedCount;
        if (cities.length > 0) {
          await CitiesSchema.insertMany(
            cities.map((city) => ({
              _id: city._id,
              country_id: city.country_id,
              name: city.name,
              sort: city.sort || 999,
              coordinates: city.coordinates || { lat: '0', lon: '0' },
            })),
            { ordered: false },
          ).catch((err) => {
            if (err.code !== 11000) throw err;
          });
          citiesCreated = cities.length;
        }
      } else if (cities.length > 0) {
        const existingCityIds = await CitiesSchema.find({}).select('_id').lean();
        const existingIdsSet = new Set(existingCityIds.map((c) => c._id.toString()));
        const newCities = cities.filter((c) => !existingIdsSet.has(c._id.toString()));
        if (newCities.length > 0) {
          await CitiesSchema.insertMany(
            newCities.map((city) => ({
              _id: city._id,
              country_id: city.country_id,
              name: city.name,
              sort: city.sort || 999,
              coordinates: city.coordinates || { lat: '0', lon: '0' },
            })),
            { ordered: false },
          ).catch((err) => {
            if (err.code !== 11000) throw err;
          });
          citiesCreated = newCities.length;
        }
      }

      if (Array.isArray(eventCategories) && eventCategories.length > 0) {
        for (const cat of eventCategories) {
          if (!cat?._id) continue;
          // eslint-disable-next-line no-await-in-loop
          await EventsCategoriesSchema.findByIdAndUpdate(
            cat._id,
            {
              $set: {
                name: cat.name,
                sort: cat.sort ?? 999,
                keywords: Array.isArray(cat.keywords) ? cat.keywords : [],
              },
            },
            { upsert: true, setDefaultsOnInsert: true },
          );
          categoriesUpserted += 1;
        }
        const remoteIds = new Set(eventCategories.map((c) => String(c._id)));
        const local = await EventsCategoriesSchema.find({}).select('_id').lean();
        const toDelete = local.filter((c) => !remoteIds.has(String(c._id))).map((c) => c._id);
        if (toDelete.length) {
          await EventsCategoriesSchema.deleteMany({ _id: { $in: toDelete } });
        }
        await rebuildAiPromptIfNeeded();
      }

      res.json({
        status: 'ok',
        message: 'Sync completed',
        statistics: {
          countries: { created: countriesCreated, deleted: countriesDeleted },
          cities: { created: citiesCreated, deleted: citiesDeleted },
          eventCategories: { upserted: categoriesUpserted },
        },
      });
    } catch (error) {
      logger.error(`Error syncing cities and countries: ${error.message || error}`);
      next(error);
    }
  };

  static submitFientaHtml = async (req, res, next) => {
    try {
      const { html: data } = req.body;

      if (!data || typeof data !== 'string') {
        return res.status(400).json({
          status: 'error',
          message: 'Data content is required and must be a string',
        });
      }

      try {
        JSON.parse(data);
      } catch (e) {
        return res.status(400).json({
          status: 'error',
          message: 'Data must be a valid JSON string',
        });
      }

      const page = new FientaPagesSchema({
        data,
        is_processed: false,
      });
      await page.save();

      res.json({
        status: 'ok',
        message: 'Page saved successfully',
        pageId: page._id.toString(),
      });
    } catch (error) {
      next(error);
    }
  };

  static getWeeklyStats = async (req, res, next) => {
    try {
      const { source, sources, from, to } = req.query;
      const sourceList = sources
        ? String(sources).split(',').map((s) => s.trim()).filter(Boolean)
        : undefined;
      const stats = await StatsServices.getWeeklyStats({
        source,
        sources: sourceList,
        from,
        to,
      });
      res.json({
        status: 'ok',
        ...stats,
      });
    } catch (error) {
      next(error);
    }
  };

  static statsPage = async (req, res) => {
    const filePath = path.join(__dirname, '../../public/parsing-stats.html');
    res.sendFile(filePath);
  };

  static eventsPage = async (req, res) => {
    const filePath = path.join(__dirname, '../../public/parsed-events.html');
    res.sendFile(filePath);
  };

  static citiesPage = async (req, res) => {
    res.redirect('/parsing/stats-ui#cities');
  };

  static getCountries = async (req, res, next) => {
    try {
      const countries = await CountriesSchema.find({})
        .select('_id name flag_url')
        .sort({ name: 1 })
        .lean();
      res.json({ status: 'ok', countries });
    } catch (error) {
      next(error);
    }
  };

  static discoverCities = async (req, res, next) => {
    try {
      const { source, countryCodes } = req.body || {};
      if (!source) {
        return res.status(400).json({ status: 'error', message: 'source is required' });
      }
      const result = await CityDiscoveryServices.discover(source, { countryCodes });
      res.json(result);
    } catch (error) {
      next(error);
    }
  };

  static listCitySuggestions = async (req, res, next) => {
    try {
      const page = Math.max(1, parseInt(String(req.query.page || 1), 10) || 1);
      const per_page = Math.max(1, Math.min(200, parseInt(String(req.query.per_page || 50), 10) || 50));
      const result = await CityDiscoveryServices.listSuggestions({
        source: req.query.source || undefined,
        status: req.query.status || 'pending',
        page,
        per_page,
        q: req.query.q || '',
      });
      res.json({ status: 'ok', ...result });
    } catch (error) {
      next(error);
    }
  };

  static citySuggestionsMetrics = async (req, res, next) => {
    try {
      const metrics = await CityDiscoveryServices.metrics();
      res.json({ status: 'ok', metrics });
    } catch (error) {
      next(error);
    }
  };

  static approveCitySuggestion = async (req, res, next) => {
    try {
      const result = await CityDiscoveryServices.approve(req.params.id, req.body || {});
      res.json({ status: 'ok', ...result });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static rejectCitySuggestion = async (req, res, next) => {
    try {
      const doc = await CityDiscoveryServices.reject(req.params.id, req.body?.reason || '');
      res.json({ status: 'ok', suggestion: doc });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static listCategorySuggestions = async (req, res, next) => {
    try {
      const page = Math.max(1, parseInt(String(req.query.page || 1), 10) || 1);
      const per_page = Math.max(1, Math.min(200, parseInt(String(req.query.per_page || 50), 10) || 50));
      const result = await CategorySuggestionServices.listCategorySuggestions({
        status: req.query.status || 'pending',
        page,
        per_page,
        q: req.query.q || '',
      });
      res.json({ status: 'ok', ...result });
    } catch (error) {
      next(error);
    }
  };

  static categorySuggestionsMetrics = async (req, res, next) => {
    try {
      const metrics = await CategorySuggestionServices.categorySuggestionsMetrics();
      res.json({ status: 'ok', metrics });
    } catch (error) {
      next(error);
    }
  };

  static approveCategorySuggestion = async (req, res, next) => {
    try {
      const result = await CategorySuggestionServices.approveCategorySuggestion(
        req.params.id,
        req.body || {},
      );
      res.json({ status: 'ok', ...result });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static rejectCategorySuggestion = async (req, res, next) => {
    try {
      const result = await CategorySuggestionServices.rejectCategorySuggestion(
        req.params.id,
        req.body?.reason || '',
      );
      res.json({ status: 'ok', ...result });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static startCategorySuggestionsBackfill = async (req, res, next) => {
    try {
      const body = req.body || {};
      const job = await CategorySuggestionsBackfillServices.runCategorySuggestionsBackfill({
        limit: body.limit != null && body.limit !== '' ? body.limit : null,
        applyCategory: Boolean(body.applyCategory),
        chunk: body.chunk,
        dryRun: Boolean(body.dryRun),
      });
      res.json({ status: 'ok', job });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static getCategorySuggestionsBackfill = async (req, res, next) => {
    try {
      const job = CategorySuggestionsBackfillServices.getCategorySuggestionsBackfillJob();
      res.json({ status: 'ok', job });
    } catch (error) {
      next(error);
    }
  };

  static stopCategorySuggestionsBackfill = async (req, res, next) => {
    try {
      const job = CategorySuggestionsBackfillServices.stopCategorySuggestionsBackfill();
      res.json({ status: 'ok', job });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static startCategorySuggestionsConsolidate = async (req, res, next) => {
    try {
      const body = req.body || {};
      const job = await CategorySuggestionsConsolidateServices.runCategorySuggestionsConsolidate({
        maxCategories: body.maxCategories != null ? body.maxCategories : 20,
      });
      res.json({ status: 'ok', job });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };

  static getCategorySuggestionsConsolidate = async (req, res, next) => {
    try {
      const job = CategorySuggestionsConsolidateServices.getCategorySuggestionsConsolidateJob();
      res.json({ status: 'ok', job });
    } catch (error) {
      next(error);
    }
  };

  static stopCategorySuggestionsConsolidate = async (req, res, next) => {
    try {
      const job = CategorySuggestionsConsolidateServices.stopCategorySuggestionsConsolidate();
      res.json({ status: 'ok', job });
    } catch (error) {
      if (error.status) {
        return res.status(error.status).json({ status: 'error', message: error.message });
      }
      next(error);
    }
  };
}

export default ParsingController;

