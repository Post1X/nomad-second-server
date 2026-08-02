/**
 * Drop obviously mismatched example titles from category suggestions
 * (e.g. concert upgrades attached to «Фильмы»).
 */

const CONCERT_RE = new RegExp(
  [
    '\\bconcert\\b',
    'концерт',
    '\\btour\\b',
    '\\blive\\b',
    'golden\\s*circle',
    'circle\\s*upgrade',
    '\\bupgrade\\b',
    '\\bga\\b',
    'standing\\s*ticket',
    '\\bdj\\b',
    'orchestra',
    'оркестр',
    '\\bfestival\\b',
    'фестиваль',
    'imagine\\s*dragons',
  ].join('|'),
  'i',
);

const FILM_HINT_RE = /фильм|кинотеатр|\bmovies?\b|\bcinema\b|\bscreening\b|watch\s*together|сеанс|(?:^|[\s—\-|:·])film(?:s)?(?:$|[\s—\-|:·])/i;

/**
 * @param {string} categoryName
 * @param {string} exampleTitle
 */
export const exampleFitsCategory = (categoryName, exampleTitle) => {
  const cat = String(categoryName || '')
    .trim()
    .toLowerCase()
    .replace(/ё/g, 'е');
  const ex = String(exampleTitle || '').trim();
  if (!ex) return false;

  if (/фильм|кино/.test(cat)) {
    const looksConcert = CONCERT_RE.test(ex);
    const looksFilm = FILM_HINT_RE.test(ex);
    if (looksConcert && !looksFilm) return false;
  }

  return true;
};

/**
 * @param {string} categoryName
 * @param {string[]} examples
 */
export const filterExamplesForCategory = (categoryName, examples = []) => (
  (examples || []).filter((ex) => exampleFitsCategory(categoryName, ex))
);

export default exampleFitsCategory;
