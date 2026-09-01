/**
 * Whole-word / whole-phrase keyword matching for event categorization.
 * Avoids substring traps: lan⊂poland, art⊂party, it⊂ticket, хор⊂хореография.
 */

/** Brands / codes allowed below MIN_KEYWORD_LETTERS. */
export const SHORT_KEYWORD_ALLOWLIST = new Set([
  'cs2', 'dj', 'ufc', 'nba', 'nhl', 'mlb', 'mls', 'mma', 'lol', 'dnd', 'd&d',
  'nft', 'ios', 'gp', 'wrc', '4dx', 'квн', 'егэ', 'ege',
]);

/**
 * Bare tokens that are too ambiguous even as whole words (or legacy traps).
 * Prefer multi-word phrases in DB instead (e.g. "lan party", "live music").
 */
export const KEYWORD_TRAPS = new Set([
  'lan', 'it', 'art', 'eat', 'tour', 'live', 'spa', 'lab', 'mode',
  'fest', 'show', 'pop', 'рок', 'поп', 'арт', 'шоу', 'хор',
  'auto', 'tech', 'film', 'play', 'band', 'race', 'camp', 'fair', 'code', 'cine',
  'gra', 'pc', 'pro', 'war', 'gr',
]);

export const MIN_KEYWORD_LETTERS = 4;

export const escapeRegExp = (s = '') => String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Letters/digits only (for length checks). */
export const keywordLetterCount = (word = '') => (
  String(word).replace(/[^\p{L}\p{N}]+/gu, '').length
);

/**
 * Drop HTML so tags like <br> cannot match short tokens as "words".
 * @param {string} text
 */
export const stripHtmlForKeywords = (text = '') => String(text || '')
  .replace(/<[^>]*>/g, ' ')
  .replace(/&[a-z]+;/gi, ' ')
  .replace(/\s+/g, ' ')
  .trim();

/**
 * @param {string} word
 * @returns {boolean} true if keyword should be ignored by matcher / builder
 */
export const isUnsafeKeyword = (word = '') => {
  const w = String(word || '').trim().toLowerCase().replace(/ё/g, 'е');
  if (!w) return true;
  if (KEYWORD_TRAPS.has(w)) return true;
  const letters = keywordLetterCount(w);
  if (letters < MIN_KEYWORD_LETTERS && !SHORT_KEYWORD_ALLOWLIST.has(w)) return true;
  return false;
};

/**
 * True if `keyword` appears in `text` as a whole word/phrase (Unicode-aware).
 * Multi-word keywords allow flexible whitespace between tokens.
 * @param {string} text already lowercased preferred
 * @param {string} keyword already lowercased preferred
 */
export const keywordMatchesText = (text, keyword) => {
  if (!text || !keyword) return false;
  const kw = String(keyword).trim().toLowerCase().replace(/ё/g, 'е');
  if (!kw || isUnsafeKeyword(kw)) return false;

  const hay = String(text).toLowerCase().replace(/ё/g, 'е');
  const parts = kw.split(/\s+/).filter(Boolean).map(escapeRegExp);
  if (!parts.length) return false;

  const body = parts.join('\\s+');
  // Boundaries: start/end or non-letter/non-number (works for Cyrillic; \\b does not).
  const re = new RegExp(`(^|[^\\p{L}\\p{N}])${body}([^\\p{L}\\p{N}]|$)`, 'u');
  return re.test(hay);
};

export default keywordMatchesText;
