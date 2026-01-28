import moment from 'moment';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import OperationsSchema from '../schemas/OperationsSchema';
import CitiesSchema from '../schemas/CitiesSchema';
import FientaPagesSchema from '../schemas/FientaPagesSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EVENT_SOURCE } from '../helpers/constants';
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

moment.locale('ru');

/** Форматирует последовательность чисел с группировкой через тире */
const formatDateRange = (dateNumbers) => {
  if (!dateNumbers || dateNumbers.length === 0) return '';
  if (dateNumbers.length === 1) return dateNumbers[0];
  
  const numbers = dateNumbers.map(n => parseInt(n, 10)).filter(n => !isNaN(n));
  if (numbers.length === 0) return dateNumbers.join(', ');
  
  const result = [];
  let start = numbers[0];
  let end = numbers[0];
  
  for (let i = 1; i < numbers.length; i++) {
    if (numbers[i] === end + 1) {
      // Продолжаем последовательность
      end = numbers[i];
    } else {
      // Завершаем текущую последовательность
      const count = end - start + 1;
      if (count === 1) {
        result.push(start.toString());
      } else if (count === 2) {
        // Две даты подряд - через запятую
        result.push(start.toString());
        result.push(end.toString());
      } else if (count === 3) {
        // Три даты подряд - через запятую
        result.push(start.toString());
        result.push((start + 1).toString());
        result.push(end.toString());
      } else {
        // Четыре и более дат подряд - через тире
        result.push(`${start}–${end}`);
      }
      start = numbers[i];
      end = numbers[i];
    }
  }
  
  // Добавляем последнюю последовательность
  const count = end - start + 1;
  if (count === 1) {
    result.push(start.toString());
  } else if (count === 2) {
    result.push(start.toString());
    result.push(end.toString());
  } else if (count === 3) {
    result.push(start.toString());
    result.push((start + 1).toString());
    result.push(end.toString());
  } else {
    result.push(`${start}–${end}`);
  }
  
  return result.join(', ');
};

/** Форматирует массив дат в текстовое поле: "12–19 февраля 2025", "12, 16, 22 февраля" или "12 декабря 2024, 15 января 2025" */
const formatHoldingDate = (dateArray) => {
  if (!dateArray || dateArray.length === 0) return '';
  const seen = new Set();
  const uniques = [];
  for (const d of dateArray) {
    if (!d || !(d instanceof Date)) continue;
    const key = `${d.getFullYear()}-${d.getMonth()}-${d.getDate()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    uniques.push(new Date(d.getFullYear(), d.getMonth(), d.getDate()));
  }
  uniques.sort((a, b) => a.getTime() - b.getTime());
  if (uniques.length === 1) {
    return moment(uniques[0]).format('D MMMM YYYY');
  }

  const years = [...new Set(uniques.map((d) => d.getFullYear()))];
  const multiYear = years.length > 1;
  const byMonth = new Map();
  for (const d of uniques) {
    const k = `${d.getFullYear()}-${d.getMonth()}`;
    if (!byMonth.has(k)) byMonth.set(k, []);
    byMonth.get(k).push(d);
  }
  const parts = [];
  for (const k of [...byMonth.keys()].sort()) {
    const arr = byMonth.get(k);
    const m = moment(arr[0]);
    const withYear = multiYear ? ' YYYY' : '';
    if (arr.length === 1) {
      parts.push(m.format('D MMMM' + withYear));
    } else if (arr.length === 2) {
      parts.push(`${moment(arr[0]).format('D')}–${moment(arr[1]).format('D')} ${m.format('MMMM' + withYear)}`);
    } else {
      // Форматируем даты с группировкой последовательных дат через тире
      const formattedDates = formatDateRange(arr.map((d) => moment(d).format('D')));
      parts.push(formattedDates + ' ' + m.format('MMMM' + withYear));
    }
  }
  const result = parts.join(', ');
  if (!multiYear && years[0] != null) {
    return `${result} ${years[0]}`;
  }
  return result;
};

const normalize = (str = '') => str
  .toString()
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

const cityTokens = (name = '') => name.split('|').map((s) => normalize(s)).filter(Boolean);

/** Очищает адрес от переносов строк и множественных пробелов */
const cleanAddress = (address) => {
  if (!address || typeof address !== 'string') return '';
  return address
    .replace(/\n/g, ' ') // Заменяем переносы строк на пробелы
    .replace(/\s+/g, ' ') // Заменяем множественные пробелы на один
    .trim();
};

const findCity = (cities, targetName = '') => {
  const target = normalize(targetName);
  if (!target) return null;
  
  // Разбиваем адрес по запятым и точкам (город обычно в последних частях)
  const parts = target.split(/[,•]/).map(p => p.trim()).filter(p => p.length > 0);
  
  // Проверяем только последние 3 части адреса (город обычно там)
  // Это предотвращает случайное определение города из названия заведения (например, "Bar" из "Tokyo Comedy Bar")
  const partsToCheck = parts.slice(-3);
  
  for (let partIdx = partsToCheck.length - 1; partIdx >= 0; partIdx--) {
    const part = partsToCheck[partIdx];
    const words = part.split(/\s+/).filter(w => w.length > 2);
    
    // Проверяем слова справа налево в этой части
    for (let wordIdx = words.length - 1; wordIdx >= 0; wordIdx--) {
      const word = words[wordIdx];
      
      // Пробуем найти точное совпадение слова с токенами города
      for (let i = 0; i < cities.length; i++) {
        const c = cities[i];
        const tokens = cityTokens(c.name);
        for (let j = 0; j < tokens.length; j++) {
          const tok = tokens[j];
          // Точное совпадение слова
          if (word === tok) {
            return c;
          }
          // Токен содержит слово или слово содержит токен (для составных названий)
          if (tok.includes(word) || word.includes(tok)) {
            // Проверяем, что это не случайное совпадение короткого слова
            if (word.length >= 4 || tok.length >= 4) {
              return c;
            }
          }
        }
      }
      
      // Пробуем совпадение с несколькими словами подряд (для составных названий типа "Old Tbilisi", "Shibuya City")
      if (wordIdx > 0) {
        const twoWords = `${words[wordIdx - 1]} ${words[wordIdx]}`;
        for (let i = 0; i < cities.length; i++) {
          const c = cities[i];
          const tokens = cityTokens(c.name);
          for (let j = 0; j < tokens.length; j++) {
            const tok = tokens[j];
            if (tok.includes(twoWords) || twoWords.includes(tok)) {
              return c;
            }
          }
        }
      }
    }
    
    // Также проверяем всю часть целиком (для случаев типа "Tokyo, Japan")
    for (let i = 0; i < cities.length; i++) {
      const c = cities[i];
      const tokens = cityTokens(c.name);
      for (let j = 0; j < tokens.length; j++) {
        const tok = tokens[j];
        if (part.includes(tok) || tok.includes(part)) {
          // Проверяем, что это не слишком короткое совпадение
          if (part.length >= 4 || tok.length >= 4) {
            return c;
          }
        }
      }
    }
  }
  
  return null;
};

/** Парсит дату и время из строки типа "Wednesday 28. January at 10:30 - 17:00" или "Wed, 28 Jan" */
const parseDateTime = (dateTimeStr, timeStr = null) => {
  if (!dateTimeStr || typeof dateTimeStr !== 'string') return null;
  
  try {
    // Парсим с английской локалью, так как Fienta использует английские названия месяцев
    const originalLocale = moment.locale();
    moment.locale('en');
    
    // Если есть отдельная строка времени, объединяем
    let fullDateTimeStr = dateTimeStr;
    if (timeStr && typeof timeStr === 'string' && timeStr.trim()) {
      fullDateTimeStr = `${dateTimeStr.trim()} ${timeStr.trim()}`;
    }
    
    // Обработка диапазонов дат типа "Wednesday 28. January at 09:00 - Friday 30. January at 18:00"
    const rangeMatch = fullDateTimeStr.match(/^(.+?)\s+-\s+(.+)$/);
    if (rangeMatch) {
      // Берем первую дату из диапазона и парсим её напрямую
      const firstPart = rangeMatch[1].trim();
      // Парсим первую часть без рекурсии
      for (const fmt of ['dddd D. MMMM [at] HH:mm', 'dddd D MMMM [at] HH:mm', 'D. MMMM [at] HH:mm', 'D MMMM [at] HH:mm']) {
        const parsed = moment(firstPart, fmt, true);
        if (parsed.isValid()) {
          const result = parsed.toDate();
          moment.locale(originalLocale);
          return result;
        }
      }
      // Если не получилось, пробуем без времени
      const dateOnly = moment(firstPart, ['dddd D. MMMM', 'dddd D MMMM', 'D. MMMM', 'D MMMM'], true);
      if (dateOnly.isValid()) {
        const result = dateOnly.toDate();
        moment.locale(originalLocale);
        return result;
      }
    }
    
    // Формат "Wed, 28 Jan" + "19:00"
    const shortDateMatch = fullDateTimeStr.match(/^([A-Za-z]{3}),?\s+(\d{1,2})\s+([A-Za-z]{3})(?:\s+(\d{1,2}):(\d{2}))?$/);
    if (shortDateMatch) {
      const [, dayName, day, month, hour, minute] = shortDateMatch;
      const currentYear = new Date().getFullYear();
      let dateStr = `${day} ${month} ${currentYear}`;
      if (hour && minute) {
        dateStr += ` ${hour}:${minute}`;
      }
      let parsed = moment(dateStr, ['D MMM YYYY HH:mm', 'D MMM YYYY'], true);
      
      // Если дата в прошлом (например, январь, а сейчас уже февраль), пробуем следующий год
      if (parsed.isValid() && parsed.isBefore(moment(), 'day')) {
        const nextYear = currentYear + 1;
        dateStr = `${day} ${month} ${nextYear}`;
        if (hour && minute) {
          dateStr += ` ${hour}:${minute}`;
        }
        parsed = moment(dateStr, ['D MMM YYYY HH:mm', 'D MMM YYYY'], true);
      }
      
      if (parsed.isValid()) {
        const result = parsed.toDate();
        moment.locale(originalLocale);
        return result;
      }
    }
    
    // Пробуем разные форматы
    const formats = [
      'dddd D. MMMM [at] HH:mm',
      'dddd D. MMMM [at] HH:mm - HH:mm',
      'dddd, D. MMMM [at] HH:mm',
      'dddd, D. MMMM [at] HH:mm - HH:mm',
      'D. MMMM [at] HH:mm',
      'D. MMMM [at] HH:mm - HH:mm',
      'dddd D MMMM [at] HH:mm',
      'dddd D MMMM [at] HH:mm - HH:mm',
      'MMMM D [at] HH:mm',
      'MMMM D [at] HH:mm - HH:mm',
      'D MMMM [at] HH:mm',
      'D MMMM [at] HH:mm - HH:mm',
      'ddd, D MMM HH:mm',
      'ddd, D MMM',
      'D MMM YYYY HH:mm',
      'D MMM YYYY',
    ];
    
    let parsedDate = null;
    for (const fmt of formats) {
      const parsed = moment(fullDateTimeStr, fmt, true);
      if (parsed.isValid()) {
        parsedDate = parsed.toDate();
        break;
      }
    }
    
    // Если не получилось, пробуем просто дату
    if (!parsedDate) {
      const dateOnly = moment(fullDateTimeStr, ['D. MMMM', 'D MMMM', 'MMMM D', 'D MMMM YYYY', 'MMMM D, YYYY', 'D MMM', 'D MMM YYYY'], true);
      if (dateOnly.isValid()) {
        parsedDate = dateOnly.toDate();
      }
    }
    
    // Восстанавливаем исходную локаль
    moment.locale(originalLocale);
    
    return parsedDate;
  } catch (e) {
    return null;
  }
};

/** Парсит массив дат из dates_times для типа 2 */
const parseDatesFromDatesTimes = (datesTimes) => {
  if (!Array.isArray(datesTimes) || datesTimes.length === 0) return [];
  
  const dates = [];
  for (const dt of datesTimes) {
    const dateStr = dt.date || '';
    const timeStr = dt.time || '';
    const parsedDate = parseDateTime(dateStr, timeStr);
    if (parsedDate) {
      dates.push(parsedDate);
    } else {
      logger.warn(`    → Не удалось распарсить дату: "${dateStr}" время: "${timeStr}"`);
    }
  }
  return dates;
};

/** Парсит страницу события и извлекает данные */
const parseEventPage = async (page, url) => {
  try {
    const eventData = await page.evaluate(() => {
      const data = {};
      
      // Название
      const titleEl = document.querySelector('#event-header h1');
      data.name = titleEl ? titleEl.textContent.trim() : '';
      
      // Дата и время - проверяем несколько источников
      let dateTime = '';
      
      // Основной источник: p.time в #event-header
      const timeEl = document.querySelector('#event-header p.time');
      if (timeEl) {
        dateTime = timeEl.textContent.trim();
      }
      
      // Если нет, ищем в button элементах (для типа 3)
      if (!dateTime) {
        const buttonEls = document.querySelectorAll('#event-header button, #event-header a.series-item button');
        for (let i = 0; i < buttonEls.length; i++) {
          const btn = buttonEls[i];
          const btnText = btn.textContent.trim();
          if (btnText && (btnText.match(/\d{1,2}/) || btnText.match(/Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec/i))) {
            dateTime = btnText;
            break;
          }
        }
      }
      
      // Если все еще нет, ищем в a.series-item (для типа 3)
      if (!dateTime) {
        const seriesItems = document.querySelectorAll('#event-header a.series-item');
        for (let i = 0; i < seriesItems.length; i++) {
          const item = seriesItems[i];
          const textElements = item.querySelectorAll('p.text-body');
          if (textElements.length > 0) {
            const dateText = textElements[0].textContent.trim();
            const timeText = textElements.length > 1 ? textElements[1].textContent.trim() : '';
            if (dateText) {
              dateTime = timeText ? dateText + ' ' + timeText : dateText;
              break;
            }
          }
        }
      }
      
      data.dateTime = dateTime;
      
      // Местоположение - проверяем оба варианта
      const locationEl = document.querySelector('#event-header p.location');
      const locationFromHeader = locationEl ? locationEl.textContent.trim() : '';
      
      // Дополнительный источник локации из #gmap
      const gmapEl = document.querySelector('#gmap .card-body p');
      const locationFromGmap = gmapEl ? gmapEl.textContent.trim() : '';
      
      // Объединяем оба значения, если они есть и различаются
      if (locationFromHeader && locationFromGmap) {
        // Если один содержит другой, используем более длинный
        if (locationFromGmap.includes(locationFromHeader)) {
          data.location = locationFromGmap;
        } else if (locationFromHeader.includes(locationFromGmap)) {
          data.location = locationFromHeader;
        } else {
          // Если они разные, объединяем через запятую
          data.location = `${locationFromHeader}, ${locationFromGmap}`;
        }
      } else {
        // Используем тот, который есть
        data.location = locationFromGmap || locationFromHeader;
      }
      
      // Описание - оставляем HTML как есть
      const descEl = document.querySelector('#desc');
      data.description = descEl ? descEl.innerHTML.trim() : '';
      
      // Изображение
      const imgEl = document.querySelector('#hero-image');
      data.imageUrl = imgEl ? imgEl.src : '';
      
      // Цены из билетов
      const prices = [];
      const ticketElements = document.querySelectorAll('.ticket .price, .ticket-price');
      for (let i = 0; i < ticketElements.length; i++) {
        const el = ticketElements[i];
        const priceText = el.textContent.trim();
        const match = priceText.match(/(\d+(?:[.,]\d+)?)/);
        if (match) {
          const price = parseFloat(match[1].replace(',', '.'));
          if (!isNaN(price) && price > 0) {
            prices.push(price);
          }
        }
      }
      data.prices = prices;
      
      return data;
    });
    
    return eventData;
  } catch (e) {
    logger.error(`Error parsing event page ${url}: ${e.message}`);
    return null;
  }
};

async function parseFienta({ meta, operationId }) {
  logger.info('\n========================================');
  logger.info('🚀 НАЧАЛО ПАРСИНГА FIENTA');
  logger.info('========================================');
  logger.info(`Operation ID: ${operationId}`);
  logger.info(`Meta: ${JSON.stringify(meta, null, 2)}`);
  logger.info('========================================\n');
  
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

  // Берем последнюю необработанную страницу из БД
  const page = await FientaPagesSchema.findOne({ is_processed: false })
    .sort({ createdAt: -1 })
    .lean();

  if (!page) {
    errorTexts.push('Нет необработанных страниц в базе данных');
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      finish_time: new Date(),
      errorText: errorTexts.join('\n') || '',
      infoText: infoLines.join('\n'),
    });
    return;
  }

  await logProgress(operationId, `Processing page ${page._id}...`);

  let totalCards = 0;
  let browser;
  let allEvents = [];
  try {
    // Парсим JSON данные
    let cards;
    try {
      cards = JSON.parse(page.data);
      if (!Array.isArray(cards)) {
        throw new Error('Data must be an array');
      }
    } catch (e) {
      throw new Error(`Invalid JSON data: ${e.message}`);
    }

    // Преобразуем формат данных
    const validCards = cards
      .map((card) => {
        if (!card.href || !card.title) return null;
        return {
          href: card.href.split('#')[0].trim(),
          title: card.title.trim(),
          dateText: (card.date || '').trim(),
          venueText: (card.venue || '').trim(),
        };
      })
      .filter(Boolean);

    totalCards = validCards.length;
    logger.info(`[Fienta] Found ${totalCards} event cards in data`);
    infoLines.push(`Найдено карточек: ${totalCards}`);

    // Классификация карточек по типам (обрабатываем все)
    const type1Cards = [];
    const type2Cards = [];
    const type3Cards = [];
    const skippedCards = [];

    const CLASSIFICATION_BATCH_SIZE = 100;
    const totalCards = validCards.length;
    
    logger.info(`\n=== КЛАССИФИКАЦИЯ ВСЕХ СОБЫТИЙ (${totalCards} карточек) ===`);
    await logProgress(operationId, `Starting classification of ${totalCards} cards...`);
    
    // Классифицируем все карточки батчами
    for (let batchStart = 0; batchStart < totalCards; batchStart += CLASSIFICATION_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + CLASSIFICATION_BATCH_SIZE, totalCards);
      const batch = validCards.slice(batchStart, batchEnd);
      
      logger.info(`Классификация батча ${Math.floor(batchStart / CLASSIFICATION_BATCH_SIZE) + 1}/${Math.ceil(totalCards / CLASSIFICATION_BATCH_SIZE)} (${batchStart + 1}-${batchEnd} из ${totalCards})`);
      await logProgress(operationId, `Classifying batch ${Math.floor(batchStart / CLASSIFICATION_BATCH_SIZE) + 1}/${Math.ceil(totalCards / CLASSIFICATION_BATCH_SIZE)}: ${batchStart + 1}-${batchEnd} of ${totalCards}`);
      
      for (let i = 0; i < batch.length; i += 1) {
        const card = batch[i];
        const venueLower = (card.venueText || '').toLowerCase();
        const dateLower = (card.dateText || '').toLowerCase();
        const name = (card.title || card.href || '').slice(0, 100);

        if (venueLower.includes('online')) {
          skippedCards.push({ ...card, reason: 'online' });
          continue;
        }

        // Проверка URL: если есть /s/ в URL, то это точно не тип 1
        const hasSeriesUrl = card.href.includes('/s/');
        
        const isType3 = venueLower.includes('multiple venues');
        const isType2 = !isType3 && (dateLower.includes('and few more') || dateLower.includes('and one more') || dateLower.includes('one more'));
        let isType1 = !isType3 && !isType2;
        
        // Если дошли до типа 1, но есть /s/ в URL, то это тип 3
        if (isType1 && hasSeriesUrl) {
          isType1 = false;
          isType3 = true;
        }

        const cardWithType = { ...card, type: isType1 ? 1 : (isType3 ? 3 : 2) };

        if (isType1) {
          type1Cards.push(cardWithType);
        } else if (isType2) {
          type2Cards.push(cardWithType);
        } else {
          type3Cards.push(cardWithType);
        }
      }
    }
    
    logger.info(`=== КОНЕЦ КЛАССИФИКАЦИИ ===\n`);
    logger.info(`Итого: тип 1 = ${type1Cards.length}, тип 2 = ${type2Cards.length}, тип 3 = ${type3Cards.length}, пропущено = ${skippedCards.length}`);

    infoLines.push(`Классификация всех ${totalCards} карточек: тип 1 = ${type1Cards.length}, тип 2 = ${type2Cards.length}, тип 3 = ${type3Cards.length}, пропущено = ${skippedCards.length}`);

    // Запускаем браузер для парсинга детальных страниц
    await logProgress(operationId, 'Launching browser for detail pages parsing...');
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

    // ЛОГИКА ОБРАБОТКИ ТИПОВ 2 И 3:
    // Для типов 2 и 3 нужно открыть страницу события и собрать ссылки на все серии
    
    const allCardsToProcess = [...type2Cards, ...type3Cards];
    logger.info(`\n=== ОБРАБОТКА ТИПОВ 2 И 3 (${allCardsToProcess.length} событий) ===`);
    await logProgress(operationId, `Processing types 2 and 3: ${allCardsToProcess.length} events...`);
    
    // Структура результатов
    const result = {
      links: [], // Тип 1 - простые ссылки
      grouped_links: [], // Типы 2 и 3 - сгруппированные
    };
    
    // Добавляем ссылки типа 1
    for (let i = 0; i < type1Cards.length; i++) {
      result.links.push(type1Cards[i].href);
    }
    
    const browserPage = await browser.newPage();
    await browserPage.setViewport({ width: 1920, height: 1080 });
    await browserPage.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
    await browserPage.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
    });

    // Обрабатываем типы 2 и 3 батчами
    const PROCESSING_BATCH_SIZE = 20;
    for (let batchStart = 0; batchStart < allCardsToProcess.length; batchStart += PROCESSING_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + PROCESSING_BATCH_SIZE, allCardsToProcess.length);
      const batch = allCardsToProcess.slice(batchStart, batchEnd);
      
      logger.info(`\nОбработка батча ${Math.floor(batchStart / PROCESSING_BATCH_SIZE) + 1}/${Math.ceil(allCardsToProcess.length / PROCESSING_BATCH_SIZE)} (${batchStart + 1}-${batchEnd} из ${allCardsToProcess.length})`);
      await logProgress(operationId, `Processing batch ${Math.floor(batchStart / PROCESSING_BATCH_SIZE) + 1}/${Math.ceil(allCardsToProcess.length / PROCESSING_BATCH_SIZE)}: ${batchStart + 1}-${batchEnd} of ${allCardsToProcess.length}`);

      for (let i = 0; i < batch.length; i += 1) {
        const card = batch[i];
        const globalIndex = batchStart + i + 1;
        logger.info(`\n[${globalIndex}/${allCardsToProcess.length}] Обработка события типа ${card.type}:`);
        logger.info(`  Название: ${card.title}`);
        logger.info(`  URL: ${card.href}`);
        
        try {
          // 1. Открываем страницу события
          logger.info(`  → Открываем страницу события...`);
          await browserPage.goto(card.href, { waitUntil: 'networkidle2', timeout: 30000 });
          await new Promise(resolve => setTimeout(resolve, 2000)); // Ждем загрузки
          
          // 2. Ищем и кликаем на кнопку "See more" (#btn-series-items-more)
          logger.info(`  → Ищем кнопку "See more" (#btn-series-items-more)...`);
          try {
            const seeMoreBtn = await browserPage.$('#btn-series-items-more');
            if (seeMoreBtn) {
              const isVisible = await seeMoreBtn.isIntersectingViewport();
              if (isVisible) {
                await seeMoreBtn.scrollIntoView();
                await new Promise(resolve => setTimeout(resolve, 500));
                await seeMoreBtn.click();
                await new Promise(resolve => setTimeout(resolve, 2000));
                logger.info(`  → Кнопка "See more" найдена и нажата`);
              } else {
                logger.info(`  → Кнопка "See more" найдена, но не видна`);
              }
            } else {
              logger.info(`  → Кнопка "See more" не найдена (возможно, все серии уже загружены)`);
            }
          } catch (btnError) {
            logger.info(`  → Ошибка при поиске кнопки: ${btnError.message}`);
          }
          
          // 3. Собираем все ссылки на отдельные события/серии
          logger.info(`  → Собираем ссылки на все серии события...`);
          const seriesData = await browserPage.evaluate(() => {
          const items = [];
          // Ищем все ссылки с классом series-item внутри #event-header
          const eventHeader = document.querySelector('#event-header');
          if (eventHeader) {
            const seriesItems = eventHeader.querySelectorAll('a.series-item');
            for (let j = 0; j < seriesItems.length; j++) {
              const item = seriesItems[j];
              const href = item.getAttribute('href');
              if (href && href.includes('fienta.com')) {
                // Убираем якорь (#title) из ссылки
                const cleanHref = href.split('#')[0].trim();
                
                // Собираем дату и время из текста внутри элемента
                const textElements = item.querySelectorAll('p.text-body');
                let date = '';
                let time = '';
                
                if (textElements.length > 0) {
                  date = textElements[0].textContent.trim();
                }
                if (textElements.length > 1) {
                  time = textElements[1].textContent.trim();
                }
                
                items.push({
                  href: cleanHref,
                  date: date,
                  time: time,
                });
              }
            }
          }
          return items;
        });
        
          // Фильтруем ссылки: пропускаем те, что содержат /s/
          const filteredSeriesData = seriesData.filter(item => !item.href.includes('/s/'));
          const skippedCount = seriesData.length - filteredSeriesData.length;
          
          logger.info(`  → Найдено ${seriesData.length} серий, после фильтрации: ${filteredSeriesData.length} (пропущено ${skippedCount} с /s/)`);
          
          if (card.type === 3) {
            // Тип 3: все ссылки добавляем в grouped_links с is_same_address: false
            const seriesLinks = filteredSeriesData.map(item => item.href);
            result.grouped_links.push({
              original_url: card.href,
              original_title: card.title,
              is_same_address: false, // Тип 3 - разные адреса
              links: seriesLinks,
            });
            logger.info(`  → Тип 3: добавлено ${seriesLinks.length} ссылок в grouped_links`);
          } else if (card.type === 2) {
            // Тип 2: берем только первую ссылку и все даты/времена
            if (filteredSeriesData.length === 0) {
              logger.warn(`  → Тип 2: нет ссылок после фильтрации, пропускаем`);
            } else {
              const firstLink = filteredSeriesData[0].href;
              const datesTimes = filteredSeriesData.map(item => ({
                date: item.date,
                time: item.time,
              }));
              
              result.grouped_links.push({
                original_url: card.href,
                original_title: card.title,
                is_same_address: true, // Тип 2 - один адрес, разные дни
                links: [firstLink], // только первая ссылка
                dates_times: datesTimes, // все даты и времена
              });
              
              logger.info(`  → Тип 2: добавлена первая ссылка и ${datesTimes.length} дат/времен`);
            }
          }
          
        } catch (error) {
          logger.error(`  ✗ Ошибка при обработке события ${card.href}: ${error.message}`);
          errorTexts.push(`Ошибка обработки ${card.href}: ${error.message}`);
        }
      }
    } // Конец батча обработки типов 2 и 3
    
    // ============================================
    // СОЗДАНИЕ МЕРОПРИЯТИЙ
    // ============================================
    logger.info(`\n=== СОЗДАНИЕ МЕРОПРИЯТИЙ ===`);
    await logProgress(operationId, 'Starting event creation...');
    
    allEvents = [];
    const citiesList = await loadCities();
    
    // Обработка типа 1: одно мероприятие на ссылку (батчами)
    logger.info(`\n--- Обработка типа 1 (${result.links.length} ссылок) ---`);
    const TYPE1_BATCH_SIZE = 30;
    for (let batchStart = 0; batchStart < result.links.length; batchStart += TYPE1_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + TYPE1_BATCH_SIZE, result.links.length);
      const batch = result.links.slice(batchStart, batchEnd);
      
      logger.info(`Обработка типа 1, батч ${Math.floor(batchStart / TYPE1_BATCH_SIZE) + 1}/${Math.ceil(result.links.length / TYPE1_BATCH_SIZE)} (${batchStart + 1}-${batchEnd} из ${result.links.length})`);
      await logProgress(operationId, `Processing type 1 batch ${Math.floor(batchStart / TYPE1_BATCH_SIZE) + 1}/${Math.ceil(result.links.length / TYPE1_BATCH_SIZE)}: ${batchStart + 1}-${batchEnd} of ${result.links.length}`);
      
      for (let i = 0; i < batch.length; i += 1) {
        const link = batch[i];
        const globalIndex = batchStart + i + 1;
        logger.info(`[${globalIndex}/${result.links.length}] Обработка ссылки типа 1: ${link}`);
      
      try {
        await browserPage.goto(link, { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        const pageData = await parseEventPage(browserPage, link);
        if (!pageData || !pageData.name) {
          logger.warn(`  → Пропущено: нет данных или названия`);
          continue;
        }
        
        // Парсим дату
        const eventDate = parseDateTime(pageData.dateTime);
        if (!eventDate) {
          logger.warn(`  → Пропущено: не удалось распарсить дату "${pageData.dateTime}"`);
          continue;
        }
        
        // Очищаем адрес от переносов строк и множественных пробелов
        const cleanedLocation = cleanAddress(pageData.location || '');
        
        // Находим город - функция findCity уже проверяет только последние части адреса
        const city = findCity(citiesList, cleanedLocation);
        if (!city) {
          logger.warn(`  → Пропущено: город не найден для "${cleanedLocation}"`);
          continue;
        }
        
        const newEvent = {
          name: pageData.name,
          description: pageData.description || pageData.name,
          specialization,
          admin_id: adminId,
          country_id: city.country_id || countryId,
          city_id: city._id.toString(),
          operationId: operationId,
          contacts: { website: link },
          photos: pageData.imageUrl ? [{ full_url: pageData.imageUrl }] : [],
          holding_date: formatHoldingDate([eventDate]),
          date_start: eventDate,
          date_end: eventDate,
          source: EVENT_SOURCE.fienta,
          address: cleanedLocation,
        };
        
        if (pageData.prices.length > 0) {
          newEvent.min_price = Math.min(...pageData.prices);
          newEvent.max_price = Math.max(...pageData.prices);
        }
        
        allEvents.push(newEvent);
        logger.info(`  → Создано мероприятие: "${pageData.name}"`);
      } catch (error) {
        logger.error(`  ✗ Ошибка при обработке ссылки ${link}: ${error.message}`);
        errorTexts.push(`Ошибка обработки типа 1 ${link}: ${error.message}`);
      }
    }
    } // Конец батча обработки типа 1
    
    // Обработка типа 2: одно мероприятие с несколькими датами (батчами)
    logger.info(`\n--- Обработка типа 2 (${result.grouped_links.filter(g => g.is_same_address).length} событий) ---`);
    const type2Groups = result.grouped_links.filter(g => g.is_same_address);
    const TYPE2_BATCH_SIZE = 20;
    for (let batchStart = 0; batchStart < type2Groups.length; batchStart += TYPE2_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + TYPE2_BATCH_SIZE, type2Groups.length);
      const batch = type2Groups.slice(batchStart, batchEnd);
      
      logger.info(`Обработка типа 2, батч ${Math.floor(batchStart / TYPE2_BATCH_SIZE) + 1}/${Math.ceil(type2Groups.length / TYPE2_BATCH_SIZE)} (${batchStart + 1}-${batchEnd} из ${type2Groups.length})`);
      await logProgress(operationId, `Processing type 2 batch ${Math.floor(batchStart / TYPE2_BATCH_SIZE) + 1}/${Math.ceil(type2Groups.length / TYPE2_BATCH_SIZE)}: ${batchStart + 1}-${batchEnd} of ${type2Groups.length}`);
      
      for (let i = 0; i < batch.length; i += 1) {
        const group = batch[i];
        const globalIndex = batchStart + i + 1;
        logger.info(`[${globalIndex}/${type2Groups.length}] Обработка типа 2: ${group.original_title}`);
      
      try {
        if (!group.links || group.links.length === 0) {
          logger.warn(`  → Пропущено: нет ссылок`);
          continue;
        }
        
        const firstLink = group.links[0];
        await browserPage.goto(firstLink, { waitUntil: 'networkidle2', timeout: 30000 });
        await new Promise(resolve => setTimeout(resolve, 2000));
        
        const pageData = await parseEventPage(browserPage, firstLink);
        if (!pageData || !pageData.name) {
          logger.warn(`  → Пропущено: нет данных или названия`);
          continue;
        }
        
        // Парсим все даты из dates_times используя специальную функцию
        const dates = parseDatesFromDatesTimes(group.dates_times || []);
        
        // Если не удалось распарсить из dates_times, пробуем из основной страницы
        if (dates.length === 0) {
          const mainDate = parseDateTime(pageData.dateTime);
          if (mainDate) {
            dates.push(mainDate);
          }
        }
        
        if (dates.length === 0) {
          logger.warn(`  → Пропущено: не удалось распарсить даты`);
          continue;
        }
        
        // Очищаем адрес от переносов строк и множественных пробелов
        const cleanedLocation = cleanAddress(pageData.location || '');
        
        // Находим город - функция findCity уже проверяет только последние части адреса
        const city = findCity(citiesList, cleanedLocation);
        if (!city) {
          logger.warn(`  → Пропущено: город не найден для "${cleanedLocation}"`);
          continue;
        }
        
        const dateStart = new Date(Math.min(...dates.map(d => d.getTime())));
        const dateEnd = new Date(Math.max(...dates.map(d => d.getTime())));
        
        const newEvent = {
          name: pageData.name || group.original_title,
          description: pageData.description || pageData.name || group.original_title,
          specialization,
          admin_id: adminId,
          country_id: city.country_id || countryId,
          city_id: city._id.toString(),
          operationId: operationId,
          contacts: { website: firstLink },
          photos: pageData.imageUrl ? [{ full_url: pageData.imageUrl }] : [],
          holding_date: formatHoldingDate(dates),
          date_start: dateStart,
          date_end: dateEnd,
          source: EVENT_SOURCE.fienta,
          address: cleanedLocation,
        };
        
        if (pageData.prices.length > 0) {
          newEvent.min_price = Math.min(...pageData.prices);
          newEvent.max_price = Math.max(...pageData.prices);
        }
        
        allEvents.push(newEvent);
        logger.info(`  → Создано мероприятие с ${dates.length} датами: "${pageData.name || group.original_title}"`);
      } catch (error) {
        logger.error(`  ✗ Ошибка при обработке типа 2 ${group.original_url}: ${error.message}`);
        errorTexts.push(`Ошибка обработки типа 2 ${group.original_url}: ${error.message}`);
      }
    }
    } // Конец батча обработки типа 2
    
    // Обработка типа 3: несколько мероприятий, каждое с одной датой (батчами)
    logger.info(`\n--- Обработка типа 3 (${result.grouped_links.filter(g => !g.is_same_address).length} групп) ---`);
    const type3Groups = result.grouped_links.filter(g => !g.is_same_address);
    const TYPE3_BATCH_SIZE = 10;
    for (let batchStart = 0; batchStart < type3Groups.length; batchStart += TYPE3_BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + TYPE3_BATCH_SIZE, type3Groups.length);
      const batch = type3Groups.slice(batchStart, batchEnd);
      
      logger.info(`Обработка типа 3, батч ${Math.floor(batchStart / TYPE3_BATCH_SIZE) + 1}/${Math.ceil(type3Groups.length / TYPE3_BATCH_SIZE)} (${batchStart + 1}-${batchEnd} из ${type3Groups.length})`);
      await logProgress(operationId, `Processing type 3 batch ${Math.floor(batchStart / TYPE3_BATCH_SIZE) + 1}/${Math.ceil(type3Groups.length / TYPE3_BATCH_SIZE)}: ${batchStart + 1}-${batchEnd} of ${type3Groups.length}`);
      
      for (let i = 0; i < batch.length; i += 1) {
        const group = batch[i];
        const globalIndex = batchStart + i + 1;
        logger.info(`[${globalIndex}/${type3Groups.length}] Обработка типа 3: ${group.original_title} (${group.links.length} ссылок)`);
        
        if (!group.links || group.links.length === 0) {
          logger.warn(`  → Пропущено: нет ссылок`);
          continue;
        }
        
        for (let j = 0; j < group.links.length; j += 1) {
        const link = group.links[j];
        logger.info(`  [${j + 1}/${group.links.length}] Обработка ссылки: ${link}`);
        
        try {
          await browserPage.goto(link, { waitUntil: 'networkidle2', timeout: 30000 });
          await new Promise(resolve => setTimeout(resolve, 2000));
          
          const pageData = await parseEventPage(browserPage, link);
          if (!pageData || !pageData.name) {
            logger.warn(`    → Пропущено: нет данных или названия`);
            continue;
          }
          
          // Парсим дату
          const eventDate = parseDateTime(pageData.dateTime);
          if (!eventDate) {
            logger.warn(`    → Пропущено: не удалось распарсить дату "${pageData.dateTime}"`);
            continue;
          }
          
          // Очищаем адрес от переносов строк и множественных пробелов
          const cleanedLocation = cleanAddress(pageData.location || '');
          
          // Находим город - функция findCity уже проверяет только последние части адреса
          const city = findCity(citiesList, cleanedLocation);
          if (!city) {
            logger.warn(`    → Пропущено: город не найден для "${cleanedLocation}"`);
            continue;
          }
          
          const newEvent = {
            name: pageData.name,
            description: pageData.description || pageData.name,
            specialization,
            admin_id: adminId,
            country_id: city.country_id || countryId,
            city_id: city._id.toString(),
            operationId: operationId,
            contacts: { website: link },
            photos: pageData.imageUrl ? [{ full_url: pageData.imageUrl }] : [],
            holding_date: formatHoldingDate([eventDate]),
            date_start: eventDate,
            date_end: eventDate,
            source: EVENT_SOURCE.fienta,
            address: cleanedLocation,
          };
          
          if (pageData.prices.length > 0) {
            newEvent.min_price = Math.min(...pageData.prices);
            newEvent.max_price = Math.max(...pageData.prices);
          }
          
          allEvents.push(newEvent);
          logger.info(`    → Создано мероприятие: "${pageData.name}"`);
        } catch (error) {
          logger.error(`    ✗ Ошибка при обработке ссылки ${link}: ${error.message}`);
          errorTexts.push(`Ошибка обработки типа 3 ${link}: ${error.message}`);
        }
      }
    }
    } // Конец батча обработки типа 3
    
    await browserPage.close();
    
    logger.info(`\n=== ИТОГОВЫЕ РЕЗУЛЬТАТЫ ===`);
    logger.info(`Всего создано мероприятий: ${allEvents.length}`);
    logger.info(`Тип 1: ${result.links.length} ссылок`);
    logger.info(`Тип 2: ${type2Groups.length} событий`);
    logger.info(`Тип 3: ${type3Groups.length} групп`);
    logger.info(`=== КОНЕЦ СОЗДАНИЯ МЕРОПРИЯТИЙ ===\n`);
    
    // Сохранение мероприятий в базу данных
    if (allEvents.length > 0) {
      await logProgress(operationId, `Saving ${allEvents.length} events to database...`);
      const BATCH_SIZE = 10;
      
      try {
        for (let i = 0; i < allEvents.length; i += BATCH_SIZE) {
          const batch = allEvents.slice(i, i + BATCH_SIZE);
          const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
          
          await ParsedEventsSchema.insertMany(
            batch.map(event => ({
              operation: operationId,
              event_data: event,
              batch_number: batchNumber,
            }))
          );
          
          const operation = await OperationsSchema.findById(operationId);
          await OperationsSchema.findByIdAndUpdate(operationId, {
            infoText: `${operation?.infoText || ''}\nОбработано ${i + batch.length} из ${allEvents.length} событий. Батч ${batchNumber} из ${Math.ceil(allEvents.length / BATCH_SIZE)}`,
          });
        }
        
        infoLines.push(`Создано и сохранено мероприятий: ${allEvents.length}`);
        await logProgress(operationId, `Successfully saved ${allEvents.length} events`);
      } catch (saveError) {
        const saveErrMsg = `Error saving events: ${saveError.message}`;
        errorTexts.push(saveErrMsg);
        logger.error(saveErrMsg);
        await logProgress(operationId, `ERROR: ${saveErrMsg}`);
      }
    } else {
      infoLines.push('Мероприятия не созданы');
      await logProgress(operationId, 'No events created');
    }

    // Помечаем страницу как обработанную
    await FientaPagesSchema.findByIdAndUpdate(page._id, {
      is_processed: true,
      processed_at: new Date(),
    });

    if (browser) {
      await browser.close();
      await logProgress(operationId, 'Browser closed.');
    }

    infoLines.push(`Страница ${page._id} успешно обработана`);
    await logProgress(operationId, `Page ${page._id} processed successfully`);

  } catch (e) {
    const errorMsg = e?.message || String(e);
    errorTexts.push(errorMsg);
    logger.error(`Error processing page ${page._id}: ${errorMsg}`);
    
    if (browser) {
      try {
        await browser.close();
      } catch (_) {}
    }

    await FientaPagesSchema.findByIdAndUpdate(page._id, {
      is_processed: true,
      processed_at: new Date(),
      error_message: errorMsg,
    });

    await logProgress(operationId, `ERROR processing page ${page._id}: ${errorMsg}`);
  }

  const finalInfoText = infoLines.join('\n');
  await OperationsSchema.findByIdAndUpdate(operationId, {
    status: errorTexts.length > 0 ? 'error' : 'success',
    finish_time: new Date(),
    errorText: errorTexts.join('\n') || '',
    infoText: finalInfoText,
    statistics: JSON.stringify({
      page_id: page?._id?.toString() || null,
      total_cards: totalCards || 0,
      total_events: allEvents?.length || 0,
    }),
  });
}

export default parseFienta;
