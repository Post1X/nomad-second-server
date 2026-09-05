import moment from 'moment';

moment.locale('ru');

/**
 * Collapse consecutive day numbers: [1,2,3,5] → "1–3, 5"
 */
const formatDayRanges = (dayNumbers) => {
  if (!dayNumbers?.length) return '';
  if (dayNumbers.length === 1) return String(dayNumbers[0]);
  const numbers = dayNumbers.map((n) => parseInt(n, 10)).filter((n) => !Number.isNaN(n));
  if (!numbers.length) return dayNumbers.join(', ');
  const result = [];
  let start = numbers[0];
  let end = numbers[0];
  const flush = () => {
    const count = end - start + 1;
    if (count === 1) result.push(String(start));
    else result.push(`${start}–${end}`); // consecutive days → dash (incl. 2-day ranges)
  };
  for (let i = 1; i < numbers.length; i += 1) {
    if (numbers[i] === end + 1) {
      end = numbers[i];
    } else {
      flush();
      start = numbers[i];
      end = numbers[i];
    }
  }
  flush();
  return result.join(', ');
};

const uniqueSortedDays = (dates = []) => {
  const valid = (dates || [])
    .map((d) => (d instanceof Date ? d : new Date(d)))
    .filter((d) => !Number.isNaN(d.getTime()))
    .sort((a, b) => a.getTime() - b.getTime());
  const unique = [];
  const seen = new Set();
  for (const d of valid) {
    const key = moment(d).format('YYYY-MM-DD');
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }
  return unique;
};

/**
 * Display string for the app. Consecutive days in the same month → dash range.
 * Example: 1,2,3 Sep 2026 → "1–3 сентября 2026"
 */
export const formatHoldingDate = (dates = []) => {
  const uniqueDays = uniqueSortedDays(dates);
  if (!uniqueDays.length) return '';
  if (uniqueDays.length === 1) {
    return moment(uniqueDays[0]).format('D MMMM YYYY');
  }

  const years = [...new Set(uniqueDays.map((d) => d.getFullYear()))];
  const multiYear = years.length > 1;
  const byMonth = new Map();
  for (const d of uniqueDays) {
    const k = `${d.getFullYear()}-${d.getMonth()}`;
    if (!byMonth.has(k)) byMonth.set(k, []);
    byMonth.get(k).push(d);
  }

  const parts = [];
  for (const [, arr] of byMonth) {
    arr.sort((a, b) => a.getTime() - b.getTime());
    const m = moment(arr[0]);
    const withYear = multiYear ? ' YYYY' : '';
    const consecutive = arr.length >= 2
      && arr.every((d, i) => i === 0 || d.getDate() === arr[i - 1].getDate() + 1);
    if (consecutive) {
      parts.push(
        `${moment(arr[0]).format('D')}–${moment(arr[arr.length - 1]).format('D')} `
        + `${m.format(`MMMM${withYear}`)}`,
      );
    } else {
      const formattedDates = formatDayRanges(arr.map((d) => moment(d).format('D')));
      parts.push(`${formattedDates} ${m.format(`MMMM${withYear}`)}`);
    }
  }

  const result = parts.join(', ');
  if (!multiYear && years[0] != null) return `${result} ${years[0]}`;
  return result;
};

/**
 * Compact numeric form (used when RU locale text is not required).
 * Consecutive calendar days → "DD.MM.YYYY–DD.MM.YYYY", else comma list.
 */
export const formatHoldingDateNumeric = (dates = []) => {
  const uniqueDays = uniqueSortedDays(dates);
  if (!uniqueDays.length) return '';
  if (uniqueDays.length === 1) return moment(uniqueDays[0]).format('DD.MM.YYYY');

  const parts = [];
  let rangeStart = uniqueDays[0];
  let rangeEnd = uniqueDays[0];
  const flush = () => {
    const startStr = moment(rangeStart).format('DD.MM.YYYY');
    const endStr = moment(rangeEnd).format('DD.MM.YYYY');
    const daySpan = Math.round((rangeEnd - rangeStart) / (24 * 60 * 60 * 1000)) + 1;
    if (daySpan === 1) parts.push(startStr);
    else parts.push(`${startStr}–${endStr}`);
  };

  for (let i = 1; i < uniqueDays.length; i += 1) {
    const prev = uniqueDays[i - 1];
    const cur = uniqueDays[i];
    const diffDays = Math.round((cur - prev) / (24 * 60 * 60 * 1000));
    if (diffDays === 1) {
      rangeEnd = cur;
    } else {
      flush();
      rangeStart = cur;
      rangeEnd = cur;
    }
  }
  flush();
  return parts.join(', ');
};

/**
 * Parse holding_date display/numeric string back into Date[] (local midnight).
 * Supports:
 *  - "DD.MM.YYYY"
 *  - "DD.MM.YYYY, DD.MM.YYYY"
 *  - "DD.MM.YYYY–DD.MM.YYYY" / "DD.MM.YYYY-DD.MM.YYYY"
 *  - "1–3 сентября 2026" / "1, 2 сентября 2026"
 */
export const parseHoldingDate = (holdingDate = '') => {
  const text = String(holdingDate || '').trim();
  if (!text) return [];

  const out = [];
  const pushDay = (y, m0, d) => {
    const dt = new Date(y, m0, d);
    if (!Number.isNaN(dt.getTime())) out.push(dt);
  };

  // Numeric ranges / lists: 01.09.2026–03.09.2026, 01.09.2026, 05.09.2026
  const numericChunkRe = /(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s*[–-]\s*(\d{1,2})\.(\d{1,2})\.(\d{4}))?/g;
  let m;
  let matchedNumeric = false;
  while ((m = numericChunkRe.exec(text)) !== null) {
    matchedNumeric = true;
    const d1 = parseInt(m[1], 10);
    const mo1 = parseInt(m[2], 10) - 1;
    const y1 = parseInt(m[3], 10);
    if (m[4]) {
      const d2 = parseInt(m[4], 10);
      const mo2 = parseInt(m[5], 10) - 1;
      const y2 = parseInt(m[6], 10);
      const start = new Date(y1, mo1, d1);
      const end = new Date(y2, mo2, d2);
      for (let t = start.getTime(); t <= end.getTime(); t += 24 * 60 * 60 * 1000) {
        out.push(new Date(t));
      }
    } else {
      pushDay(y1, mo1, d1);
    }
  }
  if (matchedNumeric) return uniqueSortedDays(out);

  // RU locale: "1–3 сентября 2026" or "12, 15 сентября 2026"
  // genitive (display) + nominative (moment MMMM) variants
  const months = {
    января: 0, январь: 0,
    февраля: 1, февраль: 1,
    марта: 2, март: 2,
    апреля: 3, апрель: 3,
    мая: 4, май: 4,
    июня: 5, июнь: 5,
    июля: 6, июль: 6,
    августа: 7, август: 7,
    сентября: 8, сентябрь: 8,
    октября: 9, октябрь: 9,
    ноября: 10, ноябрь: 10,
    декабря: 11, декабрь: 11,
  };
  const yearMatch = text.match(/\b(20\d{2})\b/);
  const year = yearMatch ? parseInt(yearMatch[1], 10) : new Date().getFullYear();
  const lower = text.toLowerCase();
  const monthName = Object.keys(months)
    .sort((a, b) => b.length - a.length)
    .find((name) => lower.includes(name));
  if (monthName != null) {
    const month = months[monthName];
    const beforeMonth = lower.split(monthName)[0] || '';
    const range = beforeMonth.match(/(\d{1,2})\s*[–-]\s*(\d{1,2})/);
    if (range) {
      const a = parseInt(range[1], 10);
      const b = parseInt(range[2], 10);
      for (let d = a; d <= b; d += 1) pushDay(year, month, d);
      return uniqueSortedDays(out);
    }
    for (const bit of beforeMonth.match(/\d{1,2}/g) || []) {
      pushDay(year, month, parseInt(bit, 10));
    }
  }

  return uniqueSortedDays(out);
};

export const mergeHoldingDates = (...holdingOrDateLists) => {
  const all = [];
  for (const item of holdingOrDateLists) {
    if (Array.isArray(item)) all.push(...item);
    else if (typeof item === 'string') all.push(...parseHoldingDate(item));
    else if (item instanceof Date) all.push(item);
  }
  const unique = uniqueSortedDays(all);
  return {
    dates: unique,
    holding_date: formatHoldingDate(unique),
    date_start: unique.length ? unique[0] : null,
    date_end: unique.length ? unique[unique.length - 1] : null,
  };
};

export default {
  formatHoldingDate,
  formatHoldingDateNumeric,
  parseHoldingDate,
  mergeHoldingDates,
};
