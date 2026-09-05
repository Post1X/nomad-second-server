/** Strict normalize for discovery equality (diacritics off, collapse spaces). */
export const normalizeCityKey = (str = '') => String(str || '')
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[''`]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

/** Drop obvious garbage labels from source lists. */
export const isGarbageCityName = (name = '') => {
  const raw = String(name || '').trim();
  if (!raw) return true;
  const key = normalizeCityKey(raw);
  if (key.length < 2) return true;
  if (/^\d+$/.test(key)) return true;
  const junk = [
    'all cities',
    'alle städte',
    'alle stadte',
    'все города',
    'удаленно',
    'remote',
    'online',
    'n/a',
    'unknown',
    'tba',
  ];
  return junk.includes(key);
};

/**
 * Exact existence check: Frankfurt ≠ Frankfurt am Main.
 * Match only if normalize(raw) equals any pipe-token of Cities.name exactly.
 */
export const findExactCityMatch = (cities, rawName = '') => {
  const key = normalizeCityKey(rawName);
  if (!key) return null;
  return cities.find((c) => {
    const tokens = String(c.name || '').split('|').map((s) => normalizeCityKey(s)).filter(Boolean);
    return tokens.includes(key);
  }) || null;
};

const levenshtein = (a, b) => {
  if (a === b) return 0;
  const m = a.length;
  const n = b.length;
  if (!m) return n;
  if (!n) return m;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 0; i <= m; i += 1) dp[i][0] = i;
  for (let j = 0; j <= n; j += 1) dp[0][j] = j;
  for (let i = 1; i <= m; i += 1) {
    for (let j = 1; j <= n; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
    }
  }
  return dp[m][n];
};

/** Soft hint for reviewers only — does not block creating a new city. */
export const findPossibleDuplicate = (cities, rawName = '') => {
  const key = normalizeCityKey(rawName);
  if (!key || key.length < 5) return null;

  let best = null;
  let bestDist = Infinity;

  for (const c of cities) {
    const tokens = String(c.name || '').split('|').map((s) => normalizeCityKey(s)).filter(Boolean);
    for (const tok of tokens) {
      if (!tok || tok === key) continue;
      const lenDiff = Math.abs(tok.length - key.length);
      if (lenDiff > 2) continue;

      // Prefix near-match: "bad vilbel" vs "bad vilbel frankfurt..."
      const shorter = key.length <= tok.length ? key : tok;
      const longer = key.length <= tok.length ? tok : key;
      if (longer.startsWith(`${shorter} `) || longer.startsWith(`${shorter}-`)) {
        return c;
      }

      // Typo hint only when same first letter (avoids Aurich ↔ Zürich).
      if (key[0] !== tok[0]) continue;
      const dist = levenshtein(key, tok);
      const threshold = key.length >= 9 ? 2 : 1;
      if (dist > 0 && dist <= threshold && dist < bestDist) {
        bestDist = dist;
        best = c;
      }
    }
  }
  return best;
};

export default {
  normalizeCityKey,
  isGarbageCityName,
  findExactCityMatch,
  findPossibleDuplicate,
};
