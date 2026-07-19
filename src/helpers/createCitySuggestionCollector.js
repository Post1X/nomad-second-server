import CityDiscoveryServices from '../services/CityDiscoveryServices';
import { isGarbageCityName, normalizeCityKey } from './cityDiscoveryNormalize';

/**
 * Buffer unknown city names during a parse run, then upsert into CitySuggestions.
 * Same collection/UI as standalone discover — parse-time hits just bump hit_count.
 */
export function createCitySuggestionCollector(source) {
  const map = new Map();

  const note = (rawName, meta = {}) => {
    const raw_name = String(rawName || '').trim();
    if (isGarbageCityName(raw_name)) return false;

    const key = normalizeCityKey(raw_name);
    if (!key) return false;

    const bump = Number(meta.hit_count) > 0 ? Number(meta.hit_count) : 1;
    const prev = map.get(key);
    if (prev) {
      prev.hit_count += bump;
      if (!prev.slug && meta.slug) prev.slug = meta.slug;
      if (!prev.source_url && meta.source_url) prev.source_url = meta.source_url;
      return true;
    }

    map.set(key, {
      raw_name,
      slug: meta.slug || '',
      source_url: meta.source_url || '',
      hit_count: bump,
    });
    return true;
  };

  /** Prefer last comma-segments of a full address (Fienta-style locations). */
  const noteFromLocation = (location, meta = {}) => {
    const cleaned = String(location || '').replace(/\s+/g, ' ').trim();
    if (!cleaned) return 0;
    const parts = cleaned.split(/[,•]/).map((p) => p.trim()).filter(Boolean);
    const candidates = parts.length ? parts.slice(-3) : [cleaned];
    let n = 0;
    for (const part of candidates) {
      if (note(part, meta)) n += 1;
    }
    return n;
  };

  const flush = async () => {
    if (!map.size) {
      return {
        created: 0,
        updated: 0,
        alreadyInDb: 0,
        garbageSkipped: 0,
        revived: 0,
        candidatesSeen: 0,
      };
    }
    return CityDiscoveryServices.upsertCandidates(source, [...map.values()]);
  };

  return {
    note,
    noteFromLocation,
    size: () => map.size,
    flush,
  };
}

export default createCitySuggestionCollector;
