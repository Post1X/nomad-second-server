# Nomad Second Server - Parsing Service

Второй сервер для парсинга событий. Выполняет парсинг событий с внешних источников (Fienta, Eventim, Kontramarka) и сохраняет результаты в базу данных.

## Быстрый старт

**Подробная инструкция по установке:** см. [INSTALLATION.md](./INSTALLATION.md)

### Минимальные требования:
- Node.js 16.x+ (рекомендуется 18.x LTS)
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

### GET /parsing/unprocessed
Возвращает массив всех необработанных операций (success, не прочитанные ранее).

**Ответ:**
```json
{
  "status": "ok",
  "operations": [
    {
      "operation": { ... },
      "events": [ ... ],
      "totalEvents": 150
    }
  ]
}
```

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

## Архитектура

- События сохраняются частями по 10 штук (батчами)
- Прогресс парсинга сохраняется в `infoText` после каждого батча
- Если скрипт упадет - уже сохраненные батчи останутся в БД
- Операции имеют статусы: `pending`, `processing`, `success`, `error`

