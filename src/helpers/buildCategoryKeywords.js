/**
 * Build a starter keyword list for a new EventsCategory.
 * From category name (+ light stems) and known EN topic synonyms.
 * Does NOT pull tokens from event titles (cities/artists noise).
 */

import { isUnsafeKeyword } from './keywordMatch';

const STOP = new Set([
  'и', 'в', 'на', 'для', 'с', 'по', 'the', 'and', 'of', 'a', 'an',
  'event', 'events', 'мероприятия', 'мероприятие', 'шоу', ' fest',
]);

/** Extra EN/RU stems keyed by normalized RU category name. */
const TOPIC_SYNONYMS = {
  // avoid "watch party" alone — too loose; concerts must not match via vague EN stems
  фильмы: ['фильм', 'кино', 'movie', 'movies', 'cinema', 'film', 'films', 'screening'],
  экскурсии: ['экскурсия', 'tour', 'tours', 'sightseeing', 'guided tour'],
  спорт: ['sport', 'sports', 'match', 'матч'],
  фестивали: ['фестиваль', 'festival', 'festivals'],
  выставки: ['выставка', 'exhibition', 'museum'],
  лекциисеминары: ['лекция', 'семинар', 'lecture', 'talk', 'workshop'],
  семейное: ['семья', 'дети', 'family', 'kids', 'children'],
  музыка: ['концерт', 'concert', 'music', 'dj'],
  театр: ['спектакль', 'theatre', 'theater', 'drama'],
  танцы: ['танец', 'ballet', 'dance'],
  шоумюзиклы: ['мюзикл', 'musical', 'circus', 'show'],
  юмор: ['стендап', 'comedy', 'standup'],
};

const stemVariants = (word) => {
  const w = String(word || '').trim().toLowerCase().replace(/ё/g, 'е');
  if (w.length < 3) return [];
  const out = new Set([w]);
  if (w.endsWith('ы') || w.endsWith('и') || w.endsWith('а') || w.endsWith('я')) {
    out.add(w.slice(0, -1));
  }
  if (w.endsWith('ии') || w.endsWith('ия') || w.endsWith('ов') || w.endsWith('ей')) {
    out.add(w.slice(0, -2));
  }
  if (w.endsWith('ция') || w.endsWith('сия')) out.add(w.slice(0, -3));
  return [...out].filter((x) => x.length >= 3 && !STOP.has(x));
};

const topicKey = (name = '') => String(name || '')
  .trim()
  .toLowerCase()
  .replace(/ё/g, 'е')
  .replace(/[^\p{L}\p{N}]+/gu, '');

/**
 * @param {string} categoryName
 * @param {string[]} [_exampleEvents] ignored (kept for call-site compat)
 * @param {{ word: string, value: number }[]} [extraKeywords] from consolidate AI
 * @returns {{ word: string, value: number }[]}
 */
export const buildCategoryKeywords = (categoryName, _exampleEvents = [], extraKeywords = []) => {
  const byWord = new Map();

  const add = (word, value, { allowLatin = false } = {}) => {
    const w = String(word || '').trim().toLowerCase().replace(/ё/g, 'е');
    if (!w || w.length < 3 || STOP.has(w)) return;
    // Drop substring traps / too-short tokens (AI extras often add "lan", "art", "it")
    if (isUnsafeKeyword(w)) return;
    const hasCyr = /\p{Script=Cyrillic}/u.test(w);
    const hasLat = /[a-z]/i.test(w);
    if (!hasCyr && hasLat && !allowLatin) return;
    const prev = byWord.get(w) || 0;
    if (value > prev) byWord.set(w, value);
  };

  const name = String(categoryName || '').trim();
  add(name, 3);
  for (const part of name.split(/[\s/|,+.]+/).filter(Boolean)) {
    add(part, 2);
    for (const st of stemVariants(part)) add(st, 2);
  }

  const syns = TOPIC_SYNONYMS[topicKey(name)] || [];
  for (const s of syns) {
    add(s, 2, { allowLatin: true });
  }

  for (const k of extraKeywords || []) {
    const word = String(k?.word || '').trim();
    const value = Number(k?.value) || 1;
    if (!word) continue;
    add(word, value, { allowLatin: true });
  }

  return [...byWord.entries()]
    .map(([word, value]) => ({ word, value }))
    .sort((a, b) => b.value - a.value || a.word.localeCompare(b.word))
    .slice(0, 16);
};

export default buildCategoryKeywords;
