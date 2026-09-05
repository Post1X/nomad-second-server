#!/usr/bin/env babel-node
/* eslint-disable no-console */
import {
  formatHoldingDate,
  parseHoldingDate,
  mergeHoldingDates,
} from '../src/helpers/holdingDate';

let passed = 0;
let failed = 0;
const ok = (n, d = '') => { passed += 1; console.log(`✓ ${n}${d ? ` — ${d}` : ''}`); };
const fail = (n, d = '') => { failed += 1; console.error(`✗ ${n}${d ? ` — ${d}` : ''}`); };

const d = (y, m, day) => new Date(y, m, day);

const formatted = formatHoldingDate([d(2026, 8, 1), d(2026, 8, 2), d(2026, 8, 3)]);
if (formatted.includes('–') && formatted.includes('2026')) ok('format consecutive', formatted);
else fail('format consecutive', formatted);

const parsed = parseHoldingDate(formatted);
if (parsed.length === 3) ok('parse RU consecutive', parsed.map((x) => x.toISOString().slice(0, 10)).join(','));
else fail('parse RU consecutive', String(parsed.length));

const numeric = parseHoldingDate('01.09.2026–03.09.2026');
if (numeric.length === 3) ok('parse numeric range', String(numeric.length));
else fail('parse numeric range', String(numeric.length));

const merged = mergeHoldingDates('01.09.2026', [d(2026, 8, 3)], '05.09.2026');
if (merged.dates.length === 3 && merged.holding_date) {
  ok('mergeHoldingDates', `${merged.dates.length} → ${merged.holding_date}`);
} else fail('mergeHoldingDates', JSON.stringify(merged));

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
