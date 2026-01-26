import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import OperationsSchema from '../schemas/OperationsSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSE_FIENTA');

puppeteer.use(StealthPlugin());

/**
 * Тест первого этапа: обрабатывать только эти города.
 * Подставь сюда _id десяти городов — массив строк или одну строку через запятую, например:
 * ['507f1f77bcf86cd799439011', '507f1f77bcf86cd799439012']
 * или в .env/конфиге передать строку и парсить через .split(',').
 * Пустой массив = обрабатывать все отфильтрованные города.
 */
// const TEST_CITY_IDS = ['67bb15e8ffb030b6113c86ee', '6837fc6e8a72929899db9996', '67534bc22279503123f77dad', '68ce4a072afd2c36182dc413', '67c813e8c2142c6dfb6f75c6', '674869eb2279503123f6ccf6'];
const TEST_CITY_IDS = ['6837fc6e8a72929899db9996'];

const citiesCache = { list: null };

const loadCities = async () => {
  if (citiesCache.list) return citiesCache.list;
  citiesCache.list = await CitiesSchema.find({}).lean();
  return citiesCache.list;
};

const logProgress = async (operationId, message) => {
  if (operationId) {
    try {
      const operation = await OperationsSchema.findById(operationId);
      if (operation) {
        const timestamp = new Date().toISOString();
        const newLog = `[${timestamp}] ${message}`;
        operation.infoText = operation.infoText ? `${operation.infoText}\n${newLog}` : newLog;
        await operation.save();
      }
    } catch (e) {
      logger.error(`Error logging progress: ${e.message || e}`);
    }
  }
};

async function parseFienta({ meta, operationId }) {
  const errorTexts = [];
  const infoLines = [];

  const {
    adminId,
    countryId,
    cityId,
    cityName,
    specialization = 'Event',
    maxCities,
  } = meta || {};

  const citiesAll = await loadCities();

  const excludePatterns = ['удаленно', 'все города'];
  const hasExcludedText = (name) => excludePatterns.some((p) => String(name || '').toLowerCase().includes(p.toLowerCase()));
  const hasOriginalName = (name) => String(name || '').includes('|');

  const afterRemote = citiesAll.filter((c) => !hasExcludedText(c.name));
  const afterOriginal = afterRemote.filter((c) => hasOriginalName(c.name));
  const excludedRemote = citiesAll.length - afterRemote.length;
  const excludedNoOriginal = afterRemote.length - afterOriginal.length;

  infoLines.push(
    `Города: всего ${citiesAll.length}, исключено "удаленно/все города": ${excludedRemote}, без ориг. названия: ${excludedNoOriginal}, к обработке: ${afterOriginal.length}`
  );
  await logProgress(operationId, infoLines[infoLines.length - 1]);

  let cities = afterOriginal;
  const testIds = Array.isArray(TEST_CITY_IDS)
    ? TEST_CITY_IDS
    : (typeof TEST_CITY_IDS === 'string' ? TEST_CITY_IDS.split(',').map((s) => s.trim()).filter(Boolean) : []);
  if (testIds.length > 0) {
    const idSet = new Set(testIds.map((id) => String(id).trim()).filter(Boolean));
    cities = afterOriginal.filter((c) => idSet.has(c._id.toString()));
    infoLines.push(`Режим теста: только города с _id из TEST_CITY_IDS, их ${cities.length}`);
    await logProgress(operationId, infoLines[infoLines.length - 1]);
  }
  if (cityName || cityId) {
    if (cityId) {
      cities = cities.filter((c) => c._id.toString() === String(cityId));
    } else if (cityName) {
      const n = String(cityName).toLowerCase();
      cities = cities.filter((c) => String(c.name || '').toLowerCase().includes(n));
    }
  }
  if (typeof maxCities === 'number' && maxCities > 0) {
    cities = cities.slice(0, maxCities);
  }

  if (cities.length === 0) {
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: 'Нет городов для обработки после фильтров',
      finish_time: new Date(),
      infoText: infoLines.join('\n'),
    });
    return;
  }

  const cityToken = (city) => {
    const parts = String(city.name || '').split('|').map((s) => s.trim()).filter(Boolean);
    return parts[1] || parts[0] || '';
  };

  const result = {
    cities: cities.map((c) => ({
      city_name: c.name,
      city_id: c._id,
      single_date_event_urls: [],
      multiple_date_event_urls: [],
    })),
    multiple_cities_event_urls: [],
  };

  const seenMultipleCities = new Set();

  let browser;
  try {
    await logProgress(operationId, 'Launching browser (stealth)...');
    browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-blink-features=AutomationControlled',
        '--disable-features=IsolateOrigins,site-per-process',
      ],
    });
    await logProgress(operationId, 'Browser launched.');

    for (let ci = 0; ci < cities.length; ci += 1) {
      const city = cities[ci];
      const cityEntry = result.cities.find((e) => e.city_id.toString() === city._id.toString());
      const token = cityToken(city);
      const searchUrl = `https://fienta.com/?country=&city=${encodeURIComponent(token)}`;
      console.log({ searchUrl });
      // При антиботе сайт может отдавать другой/перемешанный контент. Используем puppeteer-extra-plugin-stealth.
      // Если карточки всё равно не по городу — рассмотреть: прокси из нужной страны или сбор без фильтра по city с последующей фильтрацией по venue на странице события.

      await logProgress(operationId, `City ${ci + 1}/${cities.length}: ${city.name}`);

      const page = await browser.newPage();
      await page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      );
      await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9,de;q=0.8' });
      await page.setViewport({ width: 1280, height: 800 });

      let multiCityCountThisCity = 0;
      try {
        await page.goto(searchUrl, { waitUntil: 'networkidle2', timeout: 45000 });
        await page.waitForSelector('.search-result, .event-card, #events', { timeout: 15000 }).catch(() => {});
        await new Promise((r) => setTimeout(r, 1500));
        const cards = await page.$$eval('.search-result', (rows) =>
          rows.map((el) => {
            const a = el.querySelector('article.event-card a[href*="fienta.com"]');
            if (!a) return null;
            const href = (a.getAttribute('href') || '').split('#')[0].trim();
            const body = el.querySelector('.event-card-body');
            const titleEl = body ? body.querySelector('.event-card-title h2') : null;
            const title = (titleEl && titleEl.textContent) ? titleEl.textContent.trim() : '';
            const smalls = body ? body.querySelectorAll('p.small') : [];
            const dateText = (smalls[0] && smalls[0].textContent) || '';
            const venueText = (smalls[1] && smalls[1].textContent) || '';
            return { href, title, dateText, venueText };
          })
        );

        const validCards = cards.filter(Boolean);
        const seenSingle = new Set();
        const seenMultiple = new Set();

        for (const card of validCards) {
          const venueLower = (card.venueText || '').toLowerCase();
          const dateLower = (card.dateText || '').toLowerCase();
          const name = (card.title || card.href || '').slice(0, 80);
          if (venueLower.includes('online')) {
            console.log(`[Fienta] "${name}" → пропуск (online)`);
            continue;
          }
          const isType3 = venueLower.includes('multiple venues');
          const isType2 = !isType3 && (dateLower.includes('and few more') || dateLower.includes('one more'));
          const isType1 = !isType3 && !isType2;

          if (isType1) {
            console.log(`[Fienta] "${name}" → тип 1 (single_date_event_urls)`);
            if (!seenSingle.has(card.href)) {
              seenSingle.add(card.href);
              cityEntry.single_date_event_urls.push(card.href);
            }
            continue;
          }
          if (isType2) {
            console.log(`[Fienta] "${name}" → тип 2 (multiple_date_event_urls)`);
          } else {
            console.log(`[Fienta] "${name}" → тип 3 (multiple_cities_event_urls)`);
          }

          const detailPage = await browser.newPage();
          try {
            await detailPage.goto(card.href, { waitUntil: 'domcontentloaded', timeout: 20000 });
            for (let round = 0; round < 5; round += 1) {
              const moreBtn = await detailPage.$('#btn-series-items-more');
              if (moreBtn) {
                const text = await moreBtn.evaluate((el) => el && el.textContent || '');
                if (/\bsee more\b|\bещё\b/i.test(text)) {
                  await moreBtn.click();
                  await new Promise((r) => setTimeout(r, 800));
                } else break;
              } else break;
            }

            const urls = await detailPage.$$eval('a.series-item[href*="fienta.com"]', (as) =>
              as
                .filter((a) => (a.getAttribute('id') || '') !== 'btn-series-items-more')
                .map((a) => (a.getAttribute('href') || '').split('#')[0].trim())
                .filter((u) => u && u.includes('fienta.com'))
            );
            await detailPage.close();

            const uniqueUrls = [...new Set(urls)];
            if (isType3) {
              for (const u of uniqueUrls) {
                if (!seenMultipleCities.has(u)) {
                  seenMultipleCities.add(u);
                  result.multiple_cities_event_urls.push(u);
                  multiCityCountThisCity += 1;
                }
              }
            } else {
              for (const u of uniqueUrls) {
                if (!seenMultiple.has(u)) {
                  seenMultiple.add(u);
                  cityEntry.multiple_date_event_urls.push(u);
                }
              }
            }
          } catch (e) {
            logger.error(`Fienta detail page ${card.href}: ${e.message}`);
            try { await detailPage.close(); } catch (_) {}
          }
        }

        const single = cityEntry.single_date_event_urls.length;
        const multi = cityEntry.multiple_date_event_urls.length;
        const cityLog = {
          city_name: city.name,
          city_id: city._id.toString(),
          single_date_events_count: single,
          multiple_date_events_count: multi,
          multiple_cities_events_count: multiCityCountThisCity,
        };
        infoLines.push(JSON.stringify(cityLog, null, 2));
        await logProgress(operationId, `  single: ${single}, multiple dates: ${multi}, multiple cities: ${multiCityCountThisCity}`);
      } finally {
        await page.close();
      }
    }

    await browser.close();
    await logProgress(operationId, 'Browser closed.');
  } catch (e) {
    errorTexts.push(e?.message || String(e));
    await logProgress(operationId, `FATAL: ${e?.message || e}`);
    if (browser) try { await browser.close(); } catch (_) {}
  }

  const totalSingle = result.cities.reduce((s, c) => s + c.single_date_event_urls.length, 0);
  const totalMultiple = result.cities.reduce((s, c) => s + c.multiple_date_event_urls.length, 0);
  const totalMultiCity = result.multiple_cities_event_urls.length;
  infoLines.push(`Итого: single_date=${totalSingle}, multiple_date=${totalMultiple}, multiple_cities=${totalMultiCity}`);

  const finalPayload = {
    cities: result.cities.map((c) => ({
      city_name: c.city_name,
      city_id: c.city_id.toString(),
      single_date_event_urls: c.single_date_event_urls,
      multiple_date_event_urls: c.multiple_date_event_urls,
    })),
    multiple_cities_event_urls: result.multiple_cities_event_urls,
  };

  console.log('Fienta phase 1 — итоговый список URL:', JSON.stringify(finalPayload, null, 2));

  await OperationsSchema.findByIdAndUpdate(operationId, {
    status: 'success',
    finish_time: new Date(),
    errorText: errorTexts.join('\n') || '',
    infoText: infoLines.join('\n'),
    statistics: JSON.stringify({
      total_single_date: totalSingle,
      total_multiple_date: totalMultiple,
      total_multiple_cities: totalMultiCity,
    }),
  });
}

export default parseFienta;
