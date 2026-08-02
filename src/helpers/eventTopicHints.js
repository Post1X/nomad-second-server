/**
 * Minimal helpers — topic correctness comes from proposeCategoriesFromEvents
 * (one AI pass with verified exampleIds), not per-category regex lists.
 */

export const exampleFitsCategory = (_categoryName, exampleTitle) => Boolean(
  String(exampleTitle || '').trim(),
);

export const filterExamplesForCategory = (categoryName, examples = []) => (
  (examples || []).filter((ex) => exampleFitsCategory(categoryName, ex))
);

export default {
  exampleFitsCategory,
  filterExamplesForCategory,
};
