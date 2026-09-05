import https from 'https';
import http from 'http';
import { URL } from 'url';

const USER_AGENT = 'Mozilla/5.0 (compatible; NomadCityDiscovery/1.0)';

export const requestText = (urlString, {
  method = 'GET',
  headers = {},
  body = null,
  timeoutMs = 60000,
} = {}) => new Promise((resolve, reject) => {
  const url = new URL(urlString);
  const isHttps = url.protocol === 'https:';
  const mod = isHttps ? https : http;
  const payload = body == null ? null : (Buffer.isBuffer(body) ? body : Buffer.from(String(body)));
  const reqHeaders = {
    'User-Agent': USER_AGENT,
    Accept: '*/*',
    ...headers,
  };
  if (payload) {
    reqHeaders['Content-Length'] = payload.length;
  }
  const req = mod.request({
    hostname: url.hostname,
    port: url.port || (isHttps ? 443 : 80),
    path: url.pathname + url.search,
    method,
    headers: reqHeaders,
  }, (res) => {
    const chunks = [];
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      const buffer = Buffer.concat(chunks);
      resolve({
        statusCode: res.statusCode || 0,
        headers: res.headers,
        text: buffer.toString('utf8'),
        buffer,
      });
    });
  });
  req.setTimeout(timeoutMs, () => {
    req.destroy(new Error(`Request timeout after ${timeoutMs}ms`));
  });
  req.on('error', reject);
  if (payload) req.write(payload);
  req.end();
});

export const requestJson = async (urlString, options = {}) => {
  const res = await requestText(urlString, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });
  let data = null;
  try {
    data = res.text ? JSON.parse(res.text) : null;
  } catch (e) {
    throw new Error(`Invalid JSON from ${urlString}: ${res.text.slice(0, 200)}`);
  }
  return { ...res, data };
};

export default { requestText, requestJson };
