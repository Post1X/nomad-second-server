/** Min length of Discovery body text (info / description / pleaseNote) to keep an event. */
export const TM_MIN_DESCRIPTION_LENGTH = 50;

/**
 * Longest non-empty Discovery body field. Name is intentionally excluded.
 * @param {object} event Discovery event (or any object with the same fields)
 * @returns {string}
 */
export function pickTicketmasterBodyText(event) {
  const candidates = [
    event?.description,
    event?.info,
    event?.pleaseNote,
  ]
    .map((s) => String(s || '').trim())
    .filter(Boolean);

  if (!candidates.length) return '';
  return candidates.reduce((best, cur) => (cur.length > best.length ? cur : best));
}

/**
 * @param {object} event Discovery event
 * @param {number} [min]
 * @returns {boolean}
 */
export function hasSufficientTicketmasterText(event, min = TM_MIN_DESCRIPTION_LENGTH) {
  return pickTicketmasterBodyText(event).length >= min;
}

/**
 * @param {string} text
 * @param {number} [min]
 * @returns {boolean}
 */
export function isSufficientDescription(text, min = TM_MIN_DESCRIPTION_LENGTH) {
  return String(text || '').trim().length >= min;
}
