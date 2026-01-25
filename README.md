# Nomad Second Server - Parsing Service

Второй сервер для парсинга событий. Выполняет парсинг событий с внешних источников (Fienta, Eventim, Kontramarka) и сохраняет результаты в базу данных.

## Быстрый старт

**Подробная инструкция по установке:** см. [INSTALLATION.md](./INSTALLATION.md)

### Минимальные требования:
- Node.js 22.20.0 (требуется именно эта версия)
- MongoDB 4.4+
- Системные зависимости для Puppeteer (см. INSTALLATION.md)

### Установка:

```bash
# 1. Установка зависимостей
npm install

# 2. Создание .env файла
cp .env.example .env
# Отредактируйте .env и укажите DB_NAME, PORT, PARSING_SERVER_API_KEY

# 3. Запуск
npm run dev
```

## Настройка

Создайте файл `.env` в корне проекта:

```env
DB_NAME=nomad_second
PORT=4001
PARSING_SERVER_API_KEY=your-secret-api-key-here
EVENTIM_URL=https://example.com/eventim.json.gz
EVENTIM_USERNAME=username
EVENTIM_PASSWORD=password
```

## Запуск

### Development
```bash
npm run dev
# или
yarn dev
```

### Production
```bash
npm run prod
# или
yarn prod
```

## API Endpoints

Все эндпоинты требуют заголовок `X-API-Key` с правильным API ключом.

### POST /parsing/create
Создает операцию парсинга и запускает скрипт парсинга.

**Тело запроса:**
```json
{
  "type": "parsingEventsFromFienta",
  "meta": {
    "cities": [...],
    "countries": [...],
    "categories": [...]
  }
}
```

**Ответ:**
```json
{
  "status": "ok",
  "operationId": "...",
  "message": "Operation created and started"
}
```

### GET /parsing/results/:operationId
Возвращает операцию и все спарсенные события.

**Ответ:**
```json
{
  "status": "ok",
  "operation": { ... },
  "events": [ ... ],
  "totalEvents": 150
}
```

### GET /parsing/operations
Возвращает **последнюю** операцию указанного типа, ещё не взятую (is_taken: false), и её мероприятия с пагинацией. После запроса эта операция помечается как is_taken: true.

**Query параметры:**
- `type` (обязательно) — тип операции (parsingEventsFromFienta, parsingEventsFromEventim, parsingEventsFromKontramarka)
- `page` (опционально, по умолчанию 1) — номер страницы по **событиям**
- `per_page` (опционально, по умолчанию 20, макс. 100) — количество **событий** на странице

**Важно:**
- Берётся только одна операция — последняя подходящая по дате
- `page` и `per_page` задают пагинацию по событиям этой операции

**Пример запроса:**
```
GET /parsing/operations?type=parsingEventsFromFienta&page=1&per_page=20
```

**Ответ:**
```json
{
  "status": "ok",
  "operations": [
    {
      "_id": "...",
      "type": "parsingEventsFromFienta",
      "status": "success",
      "statistics": "...",
      "errorText": "",
      "infoText": "...",
      "createdAt": "2026-01-09T11:00:00.000Z",
      "updatedAt": "2026-01-09T11:05:00.000Z",
      "finish_time": "2026-01-09T11:05:00.000Z",
      "is_processed": false,
      "is_taken": true
    }
  ],
  "events": [...],
  "totalEvents": 150,
  "totalPages": 8,
  "page": 1,
  "per_page": 20
}
```
- `events` — мероприятия последней операции на текущей странице (пагинация по событиям)
- `totalEvents` — всего событий у этой операции
- `totalPages` — число страниц по событиям при заданных `per_page`
- Если подходящих операций нет: `operations: []`, `events: []`, `totalEvents: 0`, `totalPages: 0`

### POST /parsing/cleanup
Очищает старые данные (ParsedEventsSchema).

**Тело запроса:**
```json
{
  "days": 30
}
```

**Ответ:**
```json
{
  "status": "ok",
  "deletedCount": 1500,
  "message": "Cleanup completed"
}
```

### POST /parsing/sync-cities-countries
Синхронизирует страны и города с основного сервера.

**Тело запроса:**
```json
{
  "countries": [
    {
      "_id": "507f1f77bcf86cd799439011",
      "name": "Россия",
      "flag_url": "https://example.com/flag.png"
    }
  ],
  "cities": [
    {
      "_id": "507f1f77bcf86cd799439012",
      "country_id": "507f1f77bcf86cd799439011",
      "name": "Москва | Moscow",
      "sort": 1,
      "coordinates": {
        "lat": "55.7558",
        "lon": "37.6173"
      }
    }
  ],
  "replaceAll": false
}
```

**Параметры:**
- `countries` (массив) - список стран для синхронизации
- `cities` (массив) - список городов для синхронизации
- `replaceAll` (boolean) - если `true`, удаляет все существующие записи и создает новые. Если `false`, создает только новые записи (проверка по `_id`)

**Ответ:**
```json
{
  "status": "ok",
  "message": "Sync completed",
  "statistics": {
    "countries": {
      "created": 5,
      "deleted": 0
    },
    "cities": {
      "created": 10,
      "deleted": 0
    }
  }
}
```

## Архитектура

- События сохраняются частями по 10 штук (батчами)
- Прогресс парсинга сохраняется в `infoText` после каждого батча
- Если скрипт упадет - уже сохраненные батчи останутся в БД
- Операции имеют статусы: `pending`, `processing`, `success`, `error`

## Автоматический парсинг (Cron Jobs)

Сервер автоматически запускает парсинг по расписанию:

- **Понедельник 02:00 UTC (05:00 MSK)** - Kontramarka
- **Среда 02:00 UTC (05:00 MSK)** - Fienta
- **Пятница 02:00 UTC (05:00 MSK)** - Eventim

Кроны создают операции автоматически и запускают парсинг. Города загружаются из БД второго сервера (нужно загрузить дамп городов из основного сервера).

## Синхронизация данных

- **Города**: Загрузите дамп коллекции `Cities` из основного сервера в БД второго сервера
- **Формат городов**: `{country_id, name: "Русский | English", sort, coordinates: {lat: String, lon: String}}`
- Парсеры автоматически извлекают английское название из формата "Русский | English"

