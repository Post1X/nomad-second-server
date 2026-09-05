import moment from 'moment';

/**
 * Parse "Даты: …" from israelinfo feed / description text.
 * Lists (comma) + ranges ("02/08/26 - 29/08/26") → Date[] (ranges expanded day-by-day).
 */
export const parseIsraelinfoDatesFromText = (text = '') => {
  const block = String(text || '').match(/Дат[аы]\s*:\s*([^\n]+?)(?:\s+Город[аы]?|$)/i);
  const src = block ? block[1] : String(text || '');
  const dates = [];

  const toDate = (d, m, yRaw) => {
    let year = Number(yRaw);
    if (year < 100) year += 2000;
    const dt = moment(
      `${year}-${String(m).padStart(2, '0')}-${String(d).padStart(2, '0')}`,
      'YYYY-MM-DD',
      true,
    );
    return dt.isValid() ? dt.toDate() : null;
  };

  const rangeRe = /(\d{1,2})[./](\d{1,2})[./](\d{2,4})\s*[–-]\s*(\d{1,2})[./](\d{1,2})[./](\d{2,4})/g;
  let rm;
  while ((rm = rangeRe.exec(src))) {
    const start = toDate(rm[1], rm[2], rm[3]);
    const end = toDate(rm[4], rm[5], rm[6]);
    if (!start || !end) continue;
    const from = start.getTime() <= end.getTime() ? start : end;
    const to = start.getTime() <= end.getTime() ? end : start;
    for (let t = from.getTime(); t <= to.getTime(); t += 24 * 60 * 60 * 1000) {
      dates.push(new Date(t));
    }
  }

  const singleRe = /(\d{1,2})[./](\d{1,2})[./](\d{2,4})/g;
  let sm;
  while ((sm = singleRe.exec(src))) {
    const d = toDate(sm[1], sm[2], sm[3]);
    if (d) dates.push(d);
  }

  const seen = new Set();
  const unique = [];
  for (const d of dates) {
    const key = moment(d).format('YYYY-MM-DD');
    if (seen.has(key)) continue;
    seen.add(key);
    unique.push(d);
  }
  return unique.sort((a, b) => a.getTime() - b.getTime());
};

export default parseIsraelinfoDatesFromText;
