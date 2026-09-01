import EventsCategoriesSchema from '../schemas/EventsCategoriesSchema';
import { createLoggerWithSource } from '../helpers/logger';
import {
  keywordMatchesText,
  stripHtmlForKeywords,
} from '../helpers/keywordMatch';

const logger = createLoggerWithSource('CATEGORY_KEYWORDS');

/** Same field weights / threshold as main EventServices.detectEventCategory */
const WEIGHTS = {
  name: 3,
  specialization: 2,
  description: 1,
};
/** Same threshold for every source (keywords from DB only). */
const THRESHOLD = 5;

/**
 * Detect category from EventsCategories.keywords in DB only (synced from main).
 * Matching is whole-word/phrase (Unicode); short/trap tokens are ignored.
 * @param {object} event
 * @param {string} [_source] unused — kept for call-site compat
 */
export async function detectCategoryByKeywords(event, _source) {
  const categories = await EventsCategoriesSchema.find({}).lean();
  if (!categories.length) {
    logger.warn('No events categories in local DB — skip keyword detection');
    return { categoryId: null, score: 0, categoryName: null };
  }

  const nameText = event.name ? String(event.name).toLowerCase() : '';
  const specText = event.specialization ? String(event.specialization).toLowerCase() : '';
  const descText = event.description
    ? stripHtmlForKeywords(event.description).toLowerCase()
    : '';

  let bestCategory = null;
  let bestScore = -Infinity;

  for (const category of categories) {
    if (!category?.name || category.name === 'Другое') continue;

    let categoryScore = 0;

    for (const keyword of category.keywords || []) {
      const keywordWord = keyword.word ? String(keyword.word).toLowerCase() : '';
      const keywordValue = typeof keyword.value === 'number' ? keyword.value : 1;
      if (!keywordWord) continue;

      let keywordWeight = 0;
      const applyWeight = (weight) => {
        if (keywordValue >= 0) {
          keywordWeight = Math.max(keywordWeight, weight);
        } else {
          keywordWeight = Math.min(keywordWeight, weight);
        }
      };

      if (nameText && keywordMatchesText(nameText, keywordWord)) {
        applyWeight(keywordValue * WEIGHTS.name);
      }
      if (specText && keywordMatchesText(specText, keywordWord)) {
        applyWeight(keywordValue * WEIGHTS.specialization);
      }
      if (descText && keywordMatchesText(descText, keywordWord)) {
        applyWeight(keywordValue * WEIGHTS.description);
      }

      if (keywordWeight !== 0) {
        categoryScore += keywordWeight;
      }
    }

    if (categoryScore > bestScore) {
      bestScore = categoryScore;
      bestCategory = category;
    }
  }

  if (!bestCategory || bestScore < THRESHOLD) {
    return { categoryId: null, score: bestScore > 0 ? bestScore : 0, categoryName: null };
  }

  return {
    categoryId: String(bestCategory._id),
    score: bestScore,
    categoryName: bestCategory.name || null,
  };
}

export default detectCategoryByKeywords;
