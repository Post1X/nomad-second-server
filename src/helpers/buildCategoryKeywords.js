/**
 * Build a starter keyword list for a new EventsCategory.
 * Used on approve so keyword detection works without editing config files.
 */

const STOP = new Set([
  'и', 'в', 'на', 'для', 'с', 'по', 'the', 'and', 'of', 'a', 'an', 'для',
  ' fest', 'event', 'мероприятия', 'мероприятие',
]);

const stemVariants = (word) => {
  const w = String(word || '').trim().toLowerCase().replace(/ё/g, 'е');
  if (w.length < 3) return [];
  const out = new Set([w]);
  // light Russian / Latin stems
  if (w.endsWith('ы') || w.endsWith('и') || w.endsWith('а') || w.endsWith('я')) {
    out.add(w.slice(0, -1));
  }
  if (w.endsWith('ии') || w.endsWith('ия')) out.add(w.slice(0, -2));
  if (w.endsWith('tion') || w.endsWith('sion')) out.add(w.slice(0, -2));
  if (w.length >= 5) out.add(w.slice(0, Math.max(4, w.length - 1)));
  return [...out].filter((x) => x.length >= 3);
};

/**
 * @param {string} categoryName
 * @param {string[]} [exampleEvents]
 * @returns {{ word: string, value: number }[]}
 */
export const buildCategoryKeywords = (categoryName, exampleEvents = []) => {
  const byWord = new Map();

  const add = (word, value) => {
    const w = String(word || '').trim().toLowerCase().replace(/ё/g, 'е');
    if (!w || w.length < 3 || STOP.has(w)) return;
    const prev = byWord.get(w) || 0;
    if (value > prev) byWord.set(w, value);
  };

  const name = String(categoryName || '').trim();
  add(name, 3);
  for (const part of name.split(/[\s/|,+.]+/).filter(Boolean)) {
    add(part, 2);
    for (const st of stemVariants(part)) add(st, 2);
  }

  // Pull distinctive tokens from a few example event titles
  for (const ex of (exampleEvents || []).slice(0, 5)) {
    const tokens = String(ex || '')
      .toLowerCase()
      .replace(/ё/g, 'е')
      .split(/[^\p{L}\p{N}]+/u)
      .filter((t) => t.length >= 4 && !STOP.has(t));
    for (const t of tokens.slice(0, 4)) {
      add(t, 1);
    }
  }

  return [...byWord.entries()]
    .map(([word, value]) => ({ word, value }))
    .sort((a, b) => b.value - a.value || a.word.localeCompare(b.word))
    .slice(0, 16);
};

export default buildCategoryKeywords;
