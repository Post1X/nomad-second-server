import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import getCategoryConfigForSource from '../config/categoryKeywords';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CATEGORY_KEYWORDS');

const WEIGHTS = {
  name: 3,
  specialization: 2,
  description: 1,
};

const escapeRegExp = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * Merge file-config keywords with per-category keywords stored in DB
 * (new categories approved from UI get DB keywords).
 */
const mergeKeywordMaps = (configMap, categories) => {
  const merged = { ...(configMap || {}) };
  for (const cat of categories || []) {
    const name = cat?.name;
    if (!name || name === 'Другое') continue;
    const dbKw = Array.isArray(cat.keywords) ? cat.keywords : [];
    if (!dbKw.length) continue;
    const existing = merged[name] ? [...merged[name]] : [];
    const seen = new Set(existing.map((k) => String(k.word || '').toLowerCase()));
    for (const k of dbKw) {
      const word = String(k?.word || '').trim();
      if (!word) continue;
      const key = word.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      existing.push({ word, value: Number(k.value) || 1 });
    }
    merged[name] = existing;
  }
  return merged;
};

export async function detectCategoryByKeywords(event, source) {
  const config = getCategoryConfigForSource(source);
  const threshold = config.threshold ?? 2;

  const categories = await EventsCategoriesSchema.find({}).lean();
  if (!categories.length) {
    logger.warn('No events categories in local DB — skip keyword detection');
    return { categoryId: null, score: 0, categoryName: null };
  }

  const keywordMap = mergeKeywordMaps(config.keywords || {}, categories);

  const nameById = new Map(categories.map((c) => [String(c._id), c.name]));
  const idByName = new Map(categories.map((c) => [c.name, String(c._id)]));

  const scores = {};
  for (const cat of categories) {
    scores[String(cat._id)] = 0;
  }

  const fields = [
    { text: event.name || '', weight: WEIGHTS.name },
    { text: event.specialization || '', weight: WEIGHTS.specialization },
    { text: event.description || '', weight: WEIGHTS.description },
  ];

  for (const [categoryName, keywords] of Object.entries(keywordMap)) {
    const categoryId = idByName.get(categoryName);
    if (!categoryId || categoryName === 'Другое') continue;

    for (const { word, value } of keywords || []) {
      if (!word) continue;
      const re = new RegExp(escapeRegExp(word), 'iu');
      for (const field of fields) {
        if (!field.text) continue;
        if (re.test(field.text)) {
          scores[categoryId] += (Number(value) || 1) * field.weight;
        }
      }
    }
  }

  let bestId = null;
  let bestScore = 0;
  for (const [id, score] of Object.entries(scores)) {
    if (score > bestScore) {
      bestScore = score;
      bestId = id;
    }
  }

  if (bestScore < threshold) {
    return { categoryId: null, score: bestScore, categoryName: null };
  }

  return {
    categoryId: bestId,
    score: bestScore,
    categoryName: nameById.get(bestId) || null,
  };
}

export default detectCategoryByKeywords;
