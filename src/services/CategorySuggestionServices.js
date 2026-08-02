import CategorySuggestions from '../schemas/CategorySuggestionsSchema';
import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import SettingsSchema from '../schemas/SettingsSchema';
import { ENV, SETTINGS_KEYS } from '../helpers/constants';
import { buildCategoryKeywords } from '../helpers/buildCategoryKeywords';
import { requestJson } from './cityDiscovery/http';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CATEGORY_SUGGESTIONS');

export const normalizeCategoryKey = (name = '') => String(name || '')
  .trim()
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^\p{L}\p{N}\s&/+-]/gu, ' ')
  .replace(/\s+/g, ' ')
  .trim();

const isTooSpecific = (name = '') => {
  const s = String(name || '').trim();
  if (!s) return true;
  if (s.length < 3 || s.length > 32) return true;
  if ((s.match(/\s+/g) || []).length > 2) return true;
  if (/\d{4}/.test(s)) return true;
  // truncated / incomplete words often end mid-stem
  if (/[а-яa-z]$/i.test(s) && s.length <= 5 && !/ы|и|а|я|е|о|у$/i.test(s)) {
    // keep short valid roots like "Спорт" — only flag odd truncations elsewhere
  }
  if (/туриз$/i.test(s) || /мероприят$/i.test(s)) return true;
  return false;
};

/** Category names in Nomad dict are Russian — reject Latin-only AI noise. */
const isNonRussianName = (name = '') => {
  const s = String(name || '').trim();
  if (!s) return true;
  const cyr = (s.match(/\p{Script=Cyrillic}/gu) || []).length;
  const lat = (s.match(/[A-Za-z]/g) || []).length;
  return cyr === 0 || lat > cyr;
};

const GARBAGE_KEYS = new Set([
  'cancelled', 'canceled', 'отменено', 'отмена',
  'premium', 'elite', 'vip', 'friends', 'fans', 'fan',
  'friends club', 'премиум', 'элит', 'элита',
  'локация', 'место', 'адрес', 'добро',
  'пакеты', 'эксклюзивные пакеты', 'пакет',
  'друзья', 'фанаты', 'фанат',
  'гриль', 'круиз', 'мода', 'кумбия',
  'children show', 'childrens show', "children's show",
  'детские мероприятия', 'детские шоу',
]);

const GARBAGE_RE = [
  /cancel/i,
  /premium|elite|\bvip\b/i,
  /пакет/i,
  /fan|friend/i,
  /локац/i,
  /^(добро|место)$/i,
];

const isGarbageSuggestion = (name = '') => {
  const key = normalizeCategoryKey(name);
  if (!key) return true;
  if (GARBAGE_KEYS.has(key)) return true;
  if (GARBAGE_RE.some((re) => re.test(name) || re.test(key))) return true;
  return false;
};

export const isInvalidSuggestionName = (name = '') => (
  isTooSpecific(name) || isNonRussianName(name) || isGarbageSuggestion(name)
);

/**
 * Upsert AI-suggested category names (already missing from EventsCategories).
 * @param {Array<{ name: string, source?: string, exampleEvent?: string, tokens?: number }>} items
 */
export async function upsertCategorySuggestions(items = []) {
  const stats = {
    created: 0,
    updated: 0,
    skippedExisting: 0,
    skippedInvalid: 0,
  };
  if (!items.length) return stats;

  const existing = await EventsCategoriesSchema.find({}).select('name').lean();
  const existingKeys = new Set(existing.map((c) => normalizeCategoryKey(c.name)).filter(Boolean));

  const byKey = new Map();
  for (const item of items) {
    const raw = String(item.name || '').trim();
    const key = normalizeCategoryKey(raw);
    if (!key || isInvalidSuggestionName(raw) || existingKeys.has(key)) {
      if (existingKeys.has(key)) stats.skippedExisting += 1;
      else stats.skippedInvalid += 1;
      continue;
    }
    if (!byKey.has(key)) {
      byKey.set(key, {
        raw_name: raw,
        normalized_key: key,
        sources: new Set(),
        examples: [],
        tokens: 0,
        hits: 0,
      });
    }
    const row = byKey.get(key);
    row.hits += 1;
    row.tokens += Number(item.tokens) || 0;
    if (item.source) row.sources.add(item.source);
    if (item.exampleEvent && row.examples.length < 8) {
      const ex = String(item.exampleEvent).slice(0, 160);
      if (!row.examples.includes(ex)) row.examples.push(ex);
    }
  }

  for (const row of byKey.values()) {
    // eslint-disable-next-line no-await-in-loop
    const existingDoc = await CategorySuggestions.findOne({ normalized_key: row.normalized_key });
    if (!existingDoc) {
      const keywords = buildCategoryKeywords(row.raw_name, row.examples);
      // eslint-disable-next-line no-await-in-loop
      await CategorySuggestions.create({
        raw_name: row.raw_name,
        normalized_key: row.normalized_key,
        status: 'pending',
        hit_count: row.hits,
        tokens_total: row.tokens,
        example_events: row.examples,
        keywords,
        sources: [...row.sources],
        first_seen_at: new Date(),
        last_seen_at: new Date(),
      });
      stats.created += 1;
    } else {
      const sources = new Set([...(existingDoc.sources || []), ...row.sources]);
      const examples = [...(existingDoc.example_events || [])];
      for (const ex of row.examples) {
        if (examples.length >= 12) break;
        if (!examples.includes(ex)) examples.push(ex);
      }
      const keywords = buildCategoryKeywords(row.raw_name, examples);
      // eslint-disable-next-line no-await-in-loop
      await CategorySuggestions.updateOne(
        { _id: existingDoc._id },
        {
          $set: {
            raw_name: row.raw_name,
            last_seen_at: new Date(),
            example_events: examples,
            keywords,
            sources: [...sources],
            ...(existingDoc.status === 'rejected' ? { status: 'pending', reject_reason: '' } : {}),
          },
          $inc: {
            hit_count: row.hits,
            tokens_total: row.tokens,
          },
        },
      );
      stats.updated += 1;
    }
  }

  logger.info(`Category suggestions upsert: ${JSON.stringify(stats)}`);
  return stats;
}

export async function listCategorySuggestions({
  status = 'pending',
  page = 1,
  per_page = 50,
  q = '',
} = {}) {
  const filter = {};
  if (status) filter.status = status;
  const query = String(q || '').trim();
  if (query) {
    filter.raw_name = { $regex: query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), $options: 'i' };
  }

  const skip = (Math.max(1, page) - 1) * per_page;
  const [total, items] = await Promise.all([
    CategorySuggestions.countDocuments(filter),
    CategorySuggestions.find(filter)
      .sort({ hit_count: -1, last_seen_at: -1 })
      .skip(skip)
      .limit(per_page)
      .lean(),
  ]);

  // Legacy pending rows (created before keywords-on-upsert): fill now.
  const hydrated = [];
  for (const item of items) {
    if (Array.isArray(item.keywords) && item.keywords.length) {
      hydrated.push(item);
      continue;
    }
    const keywords = buildCategoryKeywords(item.raw_name, item.example_events || []);
    // eslint-disable-next-line no-await-in-loop
    await CategorySuggestions.updateOne({ _id: item._id }, { $set: { keywords } });
    hydrated.push({ ...item, keywords });
  }

  return {
    items: hydrated,
    total,
    page: Math.max(1, page),
    per_page,
    totalPages: Math.max(1, Math.ceil(total / per_page) || 1),
  };
}

export async function categorySuggestionsMetrics() {
  const [pending, rejected, top] = await Promise.all([
    CategorySuggestions.countDocuments({ status: 'pending' }),
    CategorySuggestions.countDocuments({ status: 'rejected' }),
    CategorySuggestions.find({ status: 'pending' })
      .sort({ hit_count: -1 })
      .limit(10)
      .select('raw_name hit_count tokens_total')
      .lean(),
  ]);
  return {
    pending,
    rejected,
    top,
  };
}

export async function rejectCategorySuggestion(id, reason = '') {
  const doc = await CategorySuggestions.findById(id);
  if (!doc) {
    const err = new Error('Category suggestion not found');
    err.status = 404;
    throw err;
  }
  doc.status = 'rejected';
  doc.reject_reason = String(reason || 'manual').slice(0, 300);
  await doc.save();
  return { suggestion_id: String(doc._id), status: doc.status };
}

/**
 * Create EventsCategory on main + second (same _id), remove suggestion.
 */
export async function approveCategorySuggestion(id, { name } = {}) {
  const suggestion = await CategorySuggestions.findById(id);
  if (!suggestion) {
    const err = new Error('Category suggestion not found');
    err.status = 404;
    throw err;
  }
  if (suggestion.status === 'rejected') {
    const err = new Error('Suggestion is rejected');
    err.status = 400;
    throw err;
  }

  const categoryName = String(name || suggestion.raw_name || '').trim();
  if (!categoryName) {
    const err = new Error('name is required');
    err.status = 400;
    throw err;
  }

  const key = normalizeCategoryKey(categoryName);
  const localExisting = await EventsCategoriesSchema.findOne({
    name: { $regex: `^${categoryName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`, $options: 'i' },
  }).lean();
  if (localExisting) {
    await CategorySuggestions.deleteOne({ _id: suggestion._id });
    const err = new Error(`Category already exists: ${localExisting.name} (${localExisting._id})`);
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

  const maxSort = await EventsCategoriesSchema.findOne({}).sort({ sort: -1 }).select('sort').lean();
  const sort = (maxSort?.sort ?? 0) + 10;
  // Merge stored keywords + name stems + EN topic synonyms (movie/cinema for Фильмы…).
  const keywords = buildCategoryKeywords(
    categoryName,
    suggestion.example_events || [],
    Array.isArray(suggestion.keywords) ? suggestion.keywords : [],
  );

  const { statusCode, data } = await requestJson(`${mainUrl}/api/parsing-dict/events-categories`, {
    method: 'POST',
    headers: { 'X-Api-Key': apiKey },
    body: JSON.stringify({ name: categoryName, sort, keywords }),
  });

  if (statusCode !== 200 || data?.status !== 'ok' || !data?.category?._id) {
    const err = new Error(
      `Main create category failed HTTP ${statusCode}: ${JSON.stringify(data).slice(0, 400)}`,
    );
    err.status = 502;
    throw err;
  }

  const remote = data.category;
  await EventsCategoriesSchema.findByIdAndUpdate(
    remote._id,
    {
      $set: {
        name: remote.name || categoryName,
        sort: remote.sort ?? sort,
        keywords: Array.isArray(remote.keywords) && remote.keywords.length
          ? remote.keywords
          : keywords,
      },
    },
    { upsert: true, setDefaultsOnInsert: true },
  );

  await CategorySuggestions.deleteOne({ _id: suggestion._id });
  // Also drop any pending duplicates with same normalized key
  await CategorySuggestions.deleteMany({ normalized_key: key });

  // Force AI prompt rebuild on next categorize (category list changed)
  await SettingsSchema.deleteOne({ key: SETTINGS_KEYS.categoriesHash });

  const local = await EventsCategoriesSchema.findById(remote._id).lean();
  logger.info(`Approved category ${categoryName} → main+local ${remote._id}`);
  return {
    category: local,
    suggestion_id: String(suggestion._id),
    raw_name: suggestion.raw_name,
  };
}

export default {
  normalizeCategoryKey,
  isInvalidSuggestionName,
  upsertCategorySuggestions,
  listCategorySuggestions,
  categorySuggestionsMetrics,
  rejectCategorySuggestion,
  approveCategorySuggestion,
};
