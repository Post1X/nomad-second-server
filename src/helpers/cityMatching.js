import { CITY_ALIASES, getCitySearchVariants } from './cityAliases';

export { CITY_ALIASES, getCitySearchVariants };

export const normalize = (str = '') => str
  .toString()
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

export const cityTokens = (name = '') => name.split('|').map((s) => normalize(s)).filter(Boolean);

/** Границы — явные разделители, не \\b (кириллица). */
export const containsWholeWord = (text, token) => {
  if (!text || !token) return false;
  const escaped = token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[\\s,.\\-/•;|])${escaped}([\\s,.\\-/•;|]|$)`, 'i').test(text);
};

const tokensMatch = (target, tok) => {
  if (!target || !tok) return false;
  if (target === tok) return true;
  return containsWholeWord(target, tok) || containsWholeWord(tok, target);
};

/** "Gdańsk/Sopot", "City A, City B" → отдельные части для поиска */
export const splitCityNameParts = (cityName = '') => String(cityName || '')
  .split(/[/|,]|(?:\s+&\s+)|(?:\s+and\s+)/i)
  .map((s) => s.trim())
  .filter(Boolean);

const findCityBySingleTerm = (cities, targetName = '') => {
  const target = normalize(targetName);
  if (!target) return null;
  return cities.find((c) => {
    const tokens = cityTokens(c.name);
    if (tokens.some((tok) => tokensMatch(target, tok))) return true;
    return normalize(c.name) === target;
  }) || null;
};

/**
 * Ищет город в DB по внешнему названию (TM, Eventim, адрес и т.д.).
 * Учитывает CITY_ALIASES и формат "Русский | Local | English" в Cities.name.
 */
export const findCityInDb = (cities, targetName = '') => {
  const parts = splitCityNameParts(targetName);
  const namesToTry = parts.length > 1 ? parts : [targetName];

  for (const name of namesToTry) {
    const variants = getCitySearchVariants(name);
    for (const term of variants) {
      const match = findCityBySingleTerm(cities, term);
      if (match) return match;
    }
  }
  return null;
};

export default findCityInDb;
