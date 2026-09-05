# Nomad Second Server - Parsing Service

Второй сервер для парсинга событий. Выполняет парсинг событий с внешних источников (Fienta, Eventim, Kontramarka, Ticketmaster) и сохраняет результаты в базу данных.

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
Создаёт ParseRun и запускает парсер (ручной запуск).

**Тело запроса:**
```json
{
  "type": "parsingEventsFromTicketmaster",
  "meta": {
    "countryCode": "PL",
    "specialization": "Event"
  }
}
```

Типы: `parsingEventsFromFienta`, `parsingEventsFromEventim`, `parsingEventsFromKontramarka`, `parsingEventsFromTicketmaster`, `parsingEventsFromIsraelinfo` (или `source`: `fienta` / `eventim` / …).

**Ответ:**
```json
{
  "status": "ok",
  "runId": "...",
  "message": "Parse run created and started"
}
```

### GET /parsing/results/:runId
Возвращает ParseRun и связанные ParsedEvents.

### GET /parsing/events
Выдаёт ParsedEvents по `source`/`type` с пагинацией (`onlyPending` по умолчанию).

### GET /parsing/cron
Список jobs + enabled-флаги.

### POST /parsing/cron/:jobId/run · /enable · /disable
Ручной запуск / вкл / выкл одного cron (kontramarka, eventim, fienta, ticketmaster, israelinfo, dictSync, cleanup).

### POST /parsing/cron/stop · /start
Выкл/вкл все parser+dict jobs разом.

### POST /parsing/runs/:runId/stop
Запрос остановки активного ParseRun.

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

## Запуск парсинга

Парсинг запускается **двумя способами**:

1. **Cron** — автоматически по расписанию
2. **API** — `POST /parsing/create` для ручного/тестового запуска

### Cron (UTC)

- **Понедельник 02:00** — Kontramarka
- **Среда 02:00** — Eventim
- **Пятница 02:00** — Fienta
- **Воскресенье 02:00** — Ticketmaster (PL)

Основной сервер **забирает результаты** через `GET /parsing/events`.

Документация по Ticketmaster: [docs/ticketmaster.md](./docs/ticketmaster.md)

## Синхронизация данных

- **Города**: Загрузите дамп коллекции `Cities` из основного сервера в БД второго сервера
- **Формат городов**: `{country_id, name: "Русский | English", sort, coordinates: {lat: String, lon: String}}`
- Парсеры автоматически извлекают английское название из формата "Русский | English"

