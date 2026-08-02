/**
 * Drop obviously mismatched example titles from category suggestions
 * (e.g. concert upgrades attached to «Фильмы»).
 */

const CONCERT_RE = /\bconcert\b|концерт|\btour\b|\blive\b|golden\s*circle|circle\s*upgrade|\bga\b|standing\s*ticket|\bdj\b|orchestra|оркестр|\bfestival\b|фестиваль/i;

const FILM_HINT_RE = /фильм|кино|\bmovie\b|\bcinema\b|\bfilm\b|screening|watch\s*together|сеанс|watch\s*party/i;

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
    // Concert ticket tiers / tours must not pollute film candidates
    if (CONCERT_RE.test(ex) && !FILM_HINT_RE.test(ex)) return false;
  }

  return true;
};

export default exampleFitsCategory;
