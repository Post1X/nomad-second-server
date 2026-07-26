/* eslint-disable no-console */
/**
 * Sample Ticketmaster Discovery events and report description/info coverage.
 *
 * Usage:
 *   babel-node -r dotenv/config scripts/sampleTicketmasterDiscoveryInfo.js
 *   babel-node -r dotenv/config scripts/sampleTicketmasterDiscoveryInfo.js --limit 1000
 *   babel-node -r dotenv/config scripts/sampleTicketmasterDiscoveryInfo.js --limit 1000 --country BE
 *   babel-node -r dotenv/config scripts/sampleTicketmasterDiscoveryInfo.js --limit 1000 --countries BE,NL,DE,FR,TR,GB
 */
import dotenv from 'dotenv';
import path from 'path';
import https from 'https';

dotenv.config({ path: path.resolve(__dirname, '../.env') });

const DISCOVERY_BASE = 'https://app.ticketmaster.com/discovery/v2';
const PAGE_SIZE = 200;
/** Ticketmaster: (page * size) must be < 1000 */
const MAX_PAGES_PER_QUERY = Math.floor(999 / PAGE_SIZE); // 4 → 1000 events max per country

const DEFAULT_COUNTRIES = ['BE', 'NL', 'DE', 'FR', 'TR', 'GB', 'US', 'PL', 'ES', 'IT'];

const parseArgs = () => {
  const args = process.argv.slice(2);
  let limit = 1000;
  let countries = [...DEFAULT_COUNTRIES];
  for (let i = 0; i < args.length; i += 1) {
    if (args[i] === '--limit' && args[i + 1]) {
      limit = Math.max(1, parseInt(args[i + 1], 10) || 1000);
      i += 1;
    } else if (args[i] === '--country' && args[i + 1]) {
      countries = [String(args[i + 1]).toUpperCase()];
      i += 1;
    } else if (args[i] === '--countries' && args[i + 1]) {
      countries = String(args[i + 1]).split(',').map((c) => c.trim().toUpperCase()).filter(Boolean);
      i += 1;
    }
  }
  return { limit, countries };
};

const fetchJson = (url) => new Promise((resolve, reject) => {
  https.get(url, (res) => {
    let raw = '';
    res.on('data', (c) => { raw += c; });
    res.on('end', () => {
      if (res.statusCode !== 200) {
        reject(new Error(`HTTP ${res.statusCode}: ${raw.slice(0, 200)}`));
        return;
      }
      try {
        resolve(JSON.parse(raw));
      } catch (e) {
        reject(e);
      }
    });
  }).on('error', reject);
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const classify = (event) => {
  const name = String(event?.name || '').trim();
  const description = String(event?.description || '').trim();
  const info = String(event?.info || '').trim();
  const pleaseNote = String(event?.pleaseNote || '').trim();
  const hasDescription = description.length > 0;
  const hasInfo = info.length > 0;
  const hasPleaseNote = pleaseNote.length > 0;
  const hasAnyText = hasDescription || hasInfo || hasPleaseNote;
  const longest = [description, info, pleaseNote].sort((a, b) => b.length - a.length)[0] || '';
  const thinLikeName = !hasAnyText || (longest === name) || (longest.length <= name.length + 20 && longest.includes(name));
  // "useful" = any field clearly richer than name
  const useful = hasAnyText && longest.length > name.length + 20;
  return {
    hasDescription,
    hasInfo,
    hasPleaseNote,
    hasAnyText,
    useful,
    thinLikeName: !useful,
    descLen: description.length,
    infoLen: info.length,
    pleaseNoteLen: pleaseNote.length,
  };
};

async function fetchCountryEvents(apiKey, countryCode, need) {
  const out = [];
  for (let page = 0; page <= MAX_PAGES_PER_QUERY && out.length < need; page += 1) {
    const params = new URLSearchParams({
      apikey: apiKey,
      countryCode,
      size: String(PAGE_SIZE),
      page: String(page),
      sort: 'date,asc',
    });
    const url = `${DISCOVERY_BASE}/events.json?${params}`;
    // eslint-disable-next-line no-await-in-loop
    const data = await fetchJson(url);
    const events = data?._embedded?.events || [];
    if (!events.length) break;
    out.push(...events);
    const totalPages = data?.page?.totalPages ?? 1;
    if (page + 1 >= totalPages) break;
    // eslint-disable-next-line no-await-in-loop
    await sleep(250);
  }
  return out.slice(0, need);
}

async function main() {
  const { limit, countries } = parseArgs();
  const apiKey = process.env.TICKETMASTER_API_KEY;
  if (!apiKey) {
    console.error('TICKETMASTER_API_KEY is not set');
    process.exit(1);
  }

  const perCountry = Math.max(1, Math.ceil(limit / countries.length));
  console.log(
    `Sampling up to ${limit} Discovery events `
    + `(~${perCountry}/country) from: ${countries.join(', ')}`,
  );

  const seen = new Set();
  const rows = [];
  const byCountry = {};

  for (const country of countries) {
    if (rows.length >= limit) break;
    const need = Math.min(perCountry, limit - rows.length, 1000);
    process.stdout.write(`  ${country}: fetching (need ${need})... `);
    let events;
    try {
      // eslint-disable-next-line no-await-in-loop
      events = await fetchCountryEvents(apiKey, country, need);
    } catch (e) {
      console.log(`FAIL ${e.message}`);
      continue;
    }
    let added = 0;
    byCountry[country] = {
      fetched: 0, useful: 0, anyText: 0, info: 0, description: 0, pleaseNote: 0, thin: 0,
    };
    for (const ev of events) {
      if (rows.length >= limit) break;
      if (added >= need) break;
      const id = ev.id;
      if (!id || seen.has(id)) continue;
      seen.add(id);
      const c = classify(ev);
      rows.push({ country, id, name: ev.name, ...c });
      byCountry[country].fetched += 1;
      if (c.useful) byCountry[country].useful += 1;
      if (c.hasAnyText) byCountry[country].anyText += 1;
      if (c.hasInfo) byCountry[country].info += 1;
      if (c.hasDescription) byCountry[country].description += 1;
      if (c.hasPleaseNote) byCountry[country].pleaseNote += 1;
      if (c.thinLikeName) byCountry[country].thin += 1;
      added += 1;
    }
    console.log(`${added} unique`);
    // eslint-disable-next-line no-await-in-loop
    await sleep(300);
  }

  const n = rows.length;
  const pct = (x) => (n ? `${((100 * x) / n).toFixed(1)}%` : '0%');
  const count = (fn) => rows.filter(fn).length;

  const withInfo = count((r) => r.hasInfo);
  const withDesc = count((r) => r.hasDescription);
  const withPlease = count((r) => r.hasPleaseNote);
  const withAny = count((r) => r.hasAnyText);
  const useful = count((r) => r.useful);
  const thin = count((r) => r.thinLikeName);

  console.log('\n=== TOTAL ===');
  console.log(`events sampled: ${n}`);
  console.log(`with info:            ${withInfo}  (${pct(withInfo)})`);
  console.log(`with description:     ${withDesc}  (${pct(withDesc)})`);
  console.log(`with pleaseNote:      ${withPlease}  (${pct(withPlease)})`);
  console.log(`any of the three:     ${withAny}  (${pct(withAny)})`);
  console.log(`useful (>name+20):    ${useful}  (${pct(useful)})`);
  console.log(`thin / name-only:     ${thin}  (${pct(thin)})`);

  console.log('\n=== BY COUNTRY ===');
  console.log(
    'CC   n     info%   desc%   please% any%    useful% thin%',
  );
  for (const [cc, s] of Object.entries(byCountry)) {
    if (!s.fetched) continue;
    const p = (x) => ((100 * x) / s.fetched).toFixed(0).padStart(5);
    console.log(
      `${cc.padEnd(4)} ${String(s.fetched).padStart(4)}  `
      + `${p(s.info)}% ${p(s.description)}% ${p(s.pleaseNote)}% `
      + `${p(s.anyText)}%  ${p(s.useful)}%  ${p(s.thin)}%`,
    );
  }

  // a few thin examples
  const thinSamples = rows.filter((r) => r.thinLikeName).slice(0, 5);
  const richSamples = rows.filter((r) => r.useful).slice(0, 3);
  if (thinSamples.length) {
    console.log('\n=== thin examples ===');
    thinSamples.forEach((r) => console.log(`  [${r.country}] ${r.name} (${r.id}) info=${r.infoLen} desc=${r.descLen}`));
  }
  if (richSamples.length) {
    console.log('\n=== rich examples ===');
    richSamples.forEach((r) => console.log(`  [${r.country}] ${r.name} (${r.id}) info=${r.infoLen} desc=${r.descLen}`));
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
