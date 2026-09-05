import fs from 'fs';
import path from 'path';
import https from 'https';
import http from 'http';
import { pipeline } from 'stream/promises';
import { createGunzip } from 'zlib';
import { exec } from 'child_process';
import { promisify } from 'util';
import { URL } from 'url';
import { ENV } from '../../helpers/constants';
import { isGarbageCityName } from '../../helpers/cityDiscoveryNormalize';

const execPromise = promisify(exec);

const downloadFile = (urlString, destPath, username = '', password = '') => new Promise((resolve, reject) => {
  const url = new URL(urlString);
  const isHttps = url.protocol === 'https:';
  const mod = isHttps ? https : http;
  const headers = {};
  if (username || password) {
    const token = Buffer.from(`${username}:${password}`).toString('base64');
    headers.Authorization = `Basic ${token}`;
  }
  const file = fs.createWriteStream(destPath);
  const req = mod.get({
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    headers,
  }, (res) => {
    if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
      file.close();
      fs.unlink(destPath, () => {});
      downloadFile(res.headers.location, destPath, username, password).then(resolve).catch(reject);
      return;
    }
    if (res.statusCode !== 200) {
      file.close();
      fs.unlink(destPath, () => {});
      reject(new Error(`Eventim download HTTP ${res.statusCode}`));
      return;
    }
    res.pipe(file);
    file.on('finish', () => file.close(() => resolve(destPath)));
  });
  req.on('error', (err) => {
    file.close();
    fs.unlink(destPath, () => {});
    reject(err);
  });
});

const extractGz = async (gzPath, extractDir) => {
  const base = path.basename(gzPath).replace(/\.gz$/i, '');
  const outPath = path.join(extractDir, base);
  try {
    await pipeline(fs.createReadStream(gzPath), createGunzip(), fs.createWriteStream(outPath));
    return outPath;
  } catch (e) {
    await execPromise(`gzip -dc "${gzPath}" > "${outPath}"`);
    return outPath;
  }
};

/**
 * Unique eventCity from Eventim feed — no ParsedEvents writes.
 */
export default async function discoverEventimCities() {
  const tmpDir = path.join(process.cwd(), 'tmp');
  if (!fs.existsSync(tmpDir)) fs.mkdirSync(tmpDir, { recursive: true });

  const eventimUrl = ENV.EVENTIM_URL;
  const username = ENV.EVENTIM_USERNAME || '';
  const password = ENV.EVENTIM_PASSWORD || '';
  const cachePath = path.join(tmpDir, 'eventim.json');

  let raw = null;
  let fromCache = false;

  if (eventimUrl) {
    try {
      const urlObj = new URL(eventimUrl);
      const gzPath = path.join(tmpDir, path.basename(urlObj.pathname) || 'eventim.json.gz');
      await downloadFile(eventimUrl, gzPath, username, password);
      const extracted = await extractGz(gzPath, tmpDir);
      if (fs.existsSync(gzPath)) fs.unlinkSync(gzPath);
      raw = fs.readFileSync(extracted, 'utf8');
      if (extracted !== cachePath) {
        fs.copyFileSync(extracted, cachePath);
      }
    } catch (e) {
      if (fs.existsSync(cachePath)) {
        raw = fs.readFileSync(cachePath, 'utf8');
        fromCache = true;
      } else {
        throw e;
      }
    }
  } else if (fs.existsSync(cachePath)) {
    raw = fs.readFileSync(cachePath, 'utf8');
    fromCache = true;
  } else {
    throw new Error('EVENTIM_URL is not set and no cached eventim.json');
  }

  const parsed = JSON.parse(raw);
  const series = parsed.eventserie || [];
  const counts = new Map();

  for (const s of series) {
    for (const event of s.events || []) {
      const city = String(event.eventCity || '').trim();
      if (isGarbageCityName(city)) continue;
      counts.set(city, (counts.get(city) || 0) + 1);
    }
  }

  const candidates = [...counts.entries()]
    .map(([raw_name, hit_count]) => ({
      raw_name,
      slug: '',
      source_url: eventimUrl || '',
      hit_count,
    }))
    .sort((a, b) => a.raw_name.localeCompare(b.raw_name, 'de'));

  return {
    candidates,
    meta: {
      method: 'eventim_feed_eventCity',
      fromCache,
      seriesCount: series.length,
      uniqueCities: candidates.length,
    },
  };
}
