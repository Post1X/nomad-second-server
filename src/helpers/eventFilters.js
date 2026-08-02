/**
 * Shared ingest filters for parsed events (past / no city).
 */

export const startOfTodayUtc = (now = new Date()) => {
  const d = new Date(now);
  d.setUTCHours(0, 0, 0, 0);
  return d;
};

/** Monday 00:00 UTC of the current week. */
export const startOfWeekMondayUtc = (now = new Date()) => {
  const d = new Date(now);
  const day = d.getUTCDay(); // 0=Sun
  const diff = day === 0 ? 6 : day - 1;
  d.setUTCDate(d.getUTCDate() - diff);
  d.setUTCHours(0, 0, 0, 0);
  return d;
};

export const isEventInPast = (event, now = new Date()) => {
  const cutoff = startOfTodayUtc(now).getTime();
  const end = event?.date_end || event?.date_start;
  if (!end) return false;
  const t = end instanceof Date ? end.getTime() : new Date(end).getTime();
  if (Number.isNaN(t)) return false;
  return t < cutoff;
};

export const filterIngestEvents = (events = [], now = new Date()) => {
  let skippedPast = 0;
  let skippedNoCity = 0;
  const kept = [];
  for (const ev of events) {
    if (isEventInPast(ev, now)) {
      skippedPast += 1;
      continue;
    }
    if (!ev?.city_id) {
      skippedNoCity += 1;
      continue;
    }
    kept.push(ev);
  }
  return { events: kept, skippedPast, skippedNoCity };
};

export default {
  startOfTodayUtc,
  startOfWeekMondayUtc,
  isEventInPast,
  filterIngestEvents,
};
