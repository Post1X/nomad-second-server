# Архитектура системы парсинга событий на отдельном сервере

## Общая логика работы

### Концепция
Парсинг событий вынесен на отдельный сервер для разгрузки основного сервера. Процесс состоит из двух этапов:
1. **Создание операции парсинга** - отправка запроса на второй сервер для запуска парсинга
2. **Чтение результатов** - получение спарсенных событий с второго сервера и их обработка на основном

### Типы парсеров
- `parsingEventsFromFienta`
- `parsingEventsFromEventim`
- `parsingEventsFromKontramarka`

### Расписание выполнения
- Каждый тип парсинга выполняется **раз в неделю**
- Между запусками разных типов - **интервал 2 дня**
- После запроса на парсинг - **через 1 день** запрос на чтение результатов
- Итого: 3 типа × 2 дня = 6 дней + 1 день "отдыха" = 7 дней (неделя)

**Пример расписания:**
- Понедельник 02:00 - запрос на парсинг Fienta
- Вторник 02:00 - запрос на чтение результатов Fienta
- Среда 02:00 - запрос на парсинг Eventim
- Четверг 02:00 - запрос на чтение результатов Eventim
- Пятница 02:00 - запрос на парсинг Kontramarka
- Суббота 02:00 - запрос на чтение результатов Kontramarka
- Воскресенье 02:00 - запрос на получение всех необработанных операций (success, не прочитанные ранее)

### Поток данных

```
[Основной сервер]                    [Второй сервер]
     |                                       |
     | 1. POST /parsing/create               |
     |    { type, meta }                     |
     |-------------------------------------->|
     |                                       | 2. Создание операции
     |                                       |    Запуск скрипта парсинга
     |                                       |    Сохранение событий частями (по 10)
     |                                       |
     | 3. GET /parsing/results/:operationId  |
     |    (через 1 день)                     |
     |-------------------------------------->|
     |                                       | 4. Возврат операции + allEvents
     |<--------------------------------------|
     |                                       |
     | 5. Сохранение операции в БД           |
     |    Разделение на eventsToCreate/      |
     |    eventsToUpdate                     |
     |    Создание/обновление событий        |
```

### Структура данных

**Запрос на создание операции:**
```json
{
  "type": "parsingEventsFromFienta" | "parsingEventsFromEventim" | "parsingEventsFromKontramarka",
  "meta": {
    "cities": [...],         // массив городов (полные объекты из БД)
    
    // ... описать, если нужны ещё какие-то другие данные
  }
}
```

**Ответ при создании операции:**
```json
{
  "status": "ok",
  "operationId": "...",     // ID операции на втором сервере
  "message": "Operation created"
}
```

**Запрос на чтение результатов:**
```
GET /parsing/results/:operationId
```

**Ответ при чтении результатов:**
```json
{
  "status": "ok",
  "operation": {
    "_id": "...",
    "type": "...",
    "status": "success" | "error" | "pending",
    "statistics": "...",
    "errorText": "...",
    "infoText": "...",        // прогресс парсинга
    "createdAt": "...",
    "finish_time": "..."
  },
  "events": [...],           // массив всех спарсенных событий
  "totalEvents": 150
}
```

---

## Блок работ для второго сервера

### 1. Схемы данных

#### OperationsSchema (на втором сервере)
```javascript
{
  type: String,              // тип парсера
  status: String,            // 'pending' | 'processing' | 'success' | 'error'
  statistics: String,        // JSON статистики
  errorText: String,         // текст ошибки
  infoText: String,          // информационный текст (прогресс парсинга)
  finish_time: Date,        // время завершения
  is_processed: Boolean,     // флаг: была ли операция обработана основным сервером (default: false)
  createdAt: Date,
  updatedAt: Date
}
```

#### ParsedEventsSchema (новая схема на втором сервере)
```javascript
{
  operation: ObjectId,       // ссылка на OperationsSchema
  event_data: Object,        // полные данные события (как в EventsSchema)
  batch_number: Number,      // номер батча (для сохранения частями)
  createdAt: Date,
  updatedAt: Date
}
```

**Индексы:**
- `operation` - для быстрого поиска по операции
- `operation + batch_number` - для оптимизации выборки

### 2. API эндпоинты

#### POST /parsing/create
**Описание:** Создает операцию парсинга и запускает скрипт парсинга

**Тело запроса:**
```json
{
  "type": "parsingEventsFromFienta",
  "meta": {
    "countries": [...],
    "cities": [...],
    "categories": [...],
    // другие параметры
  }
}
```

**Логика:**
1. Валидация типа парсера
2. Создание записи OperationsSchema со статусом `pending`
3. Запуск скрипта парсинга в фоне (async, не блокируя ответ)
4. Возврат `operationId` клиенту

**Ответ:**
```json
{
  "status": "ok",
  "operationId": "...",
  "message": "Operation created and started"
}
```

#### GET /parsing/results/:operationId
**Описание:** Возвращает операцию и все спарсенные события

**Логика:**
1. Поиск операции по ID
2. Если операция не найдена - 404
3. Поиск всех ParsedEventsSchema для данной операции
4. Преобразование `event_data` в массив событий
5. Возврат операции + массив событий
6. После успешного запроса - пометить операцию как `is_processed: true` (опционально, если нужно)

**Ответ:**
```json
{
  "status": "ok",
  "operation": { ... },
  "events": [ ... ],
  "totalEvents": 150
}
```

#### GET /parsing/unprocessed
**Описание:** Возвращает массив всех необработанных операций (только success, не прочитанные ранее)

**Логика:**
1. Поиск всех операций со статусом `success` и `is_processed: false`
2. Для каждой операции - получение всех ParsedEventsSchema
3. Возврат массива операций с их событиями

**Ответ:**
```json
{
  "status": "ok",
  "operations": [
    {
      "operation": { ... },
      "events": [ ... ],
      "totalEvents": 150
    },
    ...
  ]
}
```

#### POST /parsing/cleanup
**Описание:** Очищает старые данные (ParsedEventsSchema). Пока не вызывается с основного сервера.

**Логика:**
1. Удаление ParsedEventsSchema старше N дней (например, 30)
2. Или удаление всех ParsedEventsSchema для операций со статусом `success` и `is_processed: true`

**Ответ:**
```json
{
  "status": "ok",
  "deletedCount": 1500,
  "message": "Cleanup completed"
}
```

### 3. Скрипты парсинга

#### Структура скрипта парсинга
```javascript
async function parseFienta({ meta, operationId }) {
  const events = [];
  const errorTexts = [];
  
  try {
    // ... логика парсинга ...
    
    // Сохранение событий частями (по 10)
    const BATCH_SIZE = 10;
    for (let i = 0; i < events.length; i += BATCH_SIZE) {
      const batch = events.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      
      await ParsedEventsSchema.insertMany(
        batch.map(event => ({
          operation: operationId,
          event_data: event,
          batch_number: batchNumber
        }))
      );
      
      // Обновление прогресса в infoText
      await OperationsSchema.findByIdAndUpdate(operationId, {
        infoText: `Обработано ${i + batch.length} из ${events.length} событий. Батч ${batchNumber} из ${Math.ceil(events.length / BATCH_SIZE)}`
      });
    }
    
    // Финальное обновление операции
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'success',
      finish_time: new Date(),
      statistics: JSON.stringify({
        total: events.length,
        batches: Math.ceil(events.length / BATCH_SIZE)
      })
    });
    
  } catch (error) {
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: error.message,
      finish_time: new Date()
    });
  }
  
  return { events, errorTexts };
}
```

**Важно:**
- Сохранение происходит **частями по 10 событий**
- После каждого батча - сохранение в БД
- Если скрипт упадет - уже сохраненные батчи останутся в БД
- Прогресс парсинга сохраняется в `infoText` после каждого батча

### 4. Обработка ошибок

- Если парсинг упал - статус операции = `error`, `errorText` заполняется
- Если часть событий спарсилась - они остаются в БД, операция может иметь статус `error`, но события все равно возвращаются при запросе
- При запросе результатов - возвращаем все найденные события, даже если операция в статусе `error`
- Частичные результаты обрабатываются сразу (не ждем следующего запуска)

### 5. Авторизация

Все запросы от основного сервера должны содержать заголовок:
```
X-API-Key: <PARSING_SERVER_API_KEY>
```

На втором сервере проверять этот ключ перед обработкой запросов.

---

## Блок работ для основного сервера

### 1. Конфигурация второго сервера

Добавить в `.env`:
```
PARSING_SERVER_URL=http://second-server-url:port
PARSING_SERVER_API_KEY=...  // API ключ для авторизации на втором сервере
```

### 2. Сервис для работы со вторым сервером

#### ParsingServerServices.js (новый файл)
```javascript
import { ENV } from '../helpers/constants';

const PARSING_SERVER_URL = ENV.PARSING_SERVER_URL;
const PARSING_SERVER_API_KEY = ENV.PARSING_SERVER_API_KEY;

class ParsingServerServices {
  // Отправка запроса на создание операции парсинга
  static async createParsingOperation({ type, meta }) {
    const response = await fetch(`${PARSING_SERVER_URL}/parsing/create`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': PARSING_SERVER_API_KEY,
      },
      body: JSON.stringify({ type, meta })
    });
    
    if (!response.ok) {
      throw new Error(`Failed to create parsing operation: ${response.statusText}`);
    }
    
    const data = await response.json();
    return data.operationId;
  }
  
  // Получение результатов парсинга
  static async getParsingResults(operationId) {
    const response = await fetch(`${PARSING_SERVER_URL}/parsing/results/${operationId}`, {
      headers: {
        'X-API-Key': PARSING_SERVER_API_KEY,
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to get parsing results: ${response.statusText}`);
    }
    
    const data = await response.json();
    return {
      operation: data.operation,
      events: data.events || [],
      totalEvents: data.totalEvents || 0
    };
  }
  
  // Получение всех необработанных операций
  static async getUnprocessedOperations() {
    const response = await fetch(`${PARSING_SERVER_URL}/parsing/unprocessed`, {
      headers: {
        'X-API-Key': PARSING_SERVER_API_KEY,
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to get unprocessed operations: ${response.statusText}`);
    }
    
    const data = await response.json();
    return data.operations || [];
  }
}
```

### 3. Модификация OperationsServices.js

#### Изменения в callOperation:
- Для типов парсинга событий (`parsingEventsFromFienta`, `parsingEventsFromEventim`, `parsingEventsFromKontramarka`):
  - НЕ вызывать парсинг напрямую
  - Отправить запрос на второй сервер через `ParsingServerServices.createParsingOperation`
  - Сохранить операцию на основном сервере со статусом `pending`
  - В `meta` сохранить `remoteOperationId` (ID операции на втором сервере)

#### Новая функция для обработки результатов:
```javascript
static async processParsingResults = async ({ operationId, remoteOperationId }) => {
  try {
    // Получение результатов с второго сервера
    const { operation: remoteOperation, events } = await ParsingServerServices.getParsingResults(remoteOperationId);
    
    // Если операция еще pending - не обрабатываем
    if (remoteOperation.status === OPERATION_STATUSES.pending) {
      console.log(`Operation ${operationId} is still pending, skipping processing`);
      return { success: true, skipped: true };
    }
    
    // Берем мероприятия только когда success или error И есть events
    if ((remoteOperation.status === OPERATION_STATUSES.success || remoteOperation.status === OPERATION_STATUSES.error) && events.length > 0) {
      // Обновление операции на основном сервере
      const localOperation = await OperationsSchema.findById(operationId);
      localOperation.status = remoteOperation.status;
      localOperation.statistics = remoteOperation.statistics;
      localOperation.errorText = remoteOperation.errorText;
      localOperation.infoText = remoteOperation.infoText || '';
      localOperation.finish_time = remoteOperation.finish_time;
      await localOperation.save();
      
      // Обрабатываем события
      const result = await this.processParsedEvents({ events });
      
      // Обновляем статистику операции
      localOperation.statistics = JSON.stringify(result.statistics);
      localOperation.errorText = result.errorText;
      await localOperation.save();
      
      return { success: true, processed: true };
    }
    
    // Если нет событий, но операция завершена - просто обновляем статус
    if (remoteOperation.status === OPERATION_STATUSES.success || remoteOperation.status === OPERATION_STATUSES.error) {
      const localOperation = await OperationsSchema.findById(operationId);
      localOperation.status = remoteOperation.status;
      localOperation.statistics = remoteOperation.statistics;
      localOperation.errorText = remoteOperation.errorText;
      localOperation.infoText = remoteOperation.infoText || '';
      localOperation.finish_time = remoteOperation.finish_time;
      await localOperation.save();
      
      return { success: true, processed: false, noEvents: true };
    }
    
    return { success: true };
  } catch (error) {
    // Обновление статуса на ошибку
    const localOperation = await OperationsSchema.findById(operationId);
    localOperation.status = OPERATION_STATUSES.error;
    localOperation.errorText = error.message;
    localOperation.finish_time = new Date();
    await localOperation.save();
    
    return { success: false, error: error.message };
  }
};

// Новая функция для обработки уже спарсенных событий
static async processParsedEvents = async ({ events }) => {
  // Логика из eventParsingOperation, но без вызова parsingFunction
  // Разделение на eventsToCreate и eventsToUpdate
  // Создание/обновление событий
  // Возврат статистики
}
```

### 4. Модификация OperationsController.js

#### Изменения в callOperation:
```javascript
static callOperation = async (req, res, next) => {
  try {
    const { type, meta } = req.body;
    const { userId: adminId } = req;
    
    // Проверка на запрещенные типы операций
    const FORBIDDEN_TYPES = [
      OPERATION_TYPES.parsingEventsFromFienta,
      OPERATION_TYPES.parsingEventsFromEventim,
      OPERATION_TYPES.parsingEventsFromKontramarka
    ];
    
    if (FORBIDDEN_TYPES.includes(type)) {
      throwError(400, 'Этот тип операции нельзя вызывать вручную. Он выполняется автоматически по расписанию.');
    }
    
    // ... остальная логика ...
  } catch (e) {
    next(e);
  }
};
```

### 5. Cron задачи

#### Модификация cron.js:

```javascript
import ParsingServerServices from '../services/ParsingServerServices';
import OperationsServices from '../services/OperationsServices';
import OperationsSchema from '../schemas/OperationsSchema';
import { OPERATION_TYPES } from '../helpers/constants';

// Загрузка вспомогательных данных (один раз при старте или по требованию)
let staticDataCache = {
  countries: null,
  cities: null,
  categories: null,
  lastUpdate: null
};

async function loadStaticData() {
  // Загрузка стран, городов, категорий из БД
  // Кэширование на 1 час
}

// Создание операции парсинга
async function createParsingOperation(type) {
  const staticData = await loadStaticData();
  
  const meta = {
    countries: staticData.countries,
    cities: staticData.cities,
    categories: staticData.categories,
    // другие параметры
  };
  
  const remoteOperationId = await ParsingServerServices.createParsingOperation({ type, meta });
  
  // Создание операции на основном сервере
  const operation = new OperationsSchema({
    type,
    admin: null, // или системный админ
    status: OPERATION_STATUSES.pending,
    statistics: '',
    errorText: '',
    meta: { remoteOperationId } // сохраняем ID операции на втором сервере
  });
  await operation.save();
  
  return operation._id;
}

// Обработка результатов парсинга
async function processParsingResults(operationId) {
  const operation = await OperationsSchema.findById(operationId);
  if (!operation || !operation.meta?.remoteOperationId) {
    console.error(`Operation ${operationId} not found or missing remoteOperationId`);
    return;
  }
  
  await OperationsServices.processParsingResults({
    operationId: operation._id,
    remoteOperationId: operation.meta.remoteOperationId
  });
}

const setupCron = () => {
  // ... существующие cron задачи ...
  
  // Понедельник 02:00 - создание операции парсинга Fienta
  cron.schedule('0 2 * * 1', async () => {
    console.log('Starting Fienta parsing operation...');
    await createParsingOperation(OPERATION_TYPES.parsingEventsFromFienta);
  }, { timezone: 'UTC' });
  
  // Вторник 02:00 - обработка результатов Fienta
  cron.schedule('0 2 * * 2', async () => {
    console.log('Processing Fienta parsing results...');
    // Найти последнюю операцию Fienta со статусом pending
    const operation = await OperationsSchema.findOne({
      type: OPERATION_TYPES.parsingEventsFromFienta,
      status: OPERATION_STATUSES.pending
    }).sort({ createdAt: -1 });
    
    if (operation) {
      await processParsingResults(operation._id);
    }
  }, { timezone: 'UTC' });
  
  // Среда 02:00 - создание операции парсинга Eventim
  cron.schedule('0 2 * * 3', async () => {
    console.log('Starting Eventim parsing operation...');
    await createParsingOperation(OPERATION_TYPES.parsingEventsFromEventim);
  }, { timezone: 'UTC' });
  
  // Четверг 02:00 - обработка результатов Eventim
  cron.schedule('0 2 * * 4', async () => {
    console.log('Processing Eventim parsing results...');
    const operation = await OperationsSchema.findOne({
      type: OPERATION_TYPES.parsingEventsFromEventim,
      status: OPERATION_STATUSES.pending
    }).sort({ createdAt: -1 });
    
    if (operation) {
      await processParsingResults(operation._id);
    }
  }, { timezone: 'UTC' });
  
  // Пятница 02:00 - создание операции парсинга Kontramarka
  cron.schedule('0 2 * * 5', async () => {
    console.log('Starting Kontramarka parsing operation...');
    await createParsingOperation(OPERATION_TYPES.parsingEventsFromKontramarka);
  }, { timezone: 'UTC' });
  
  // Суббота 02:00 - обработка результатов Kontramarka
  cron.schedule('0 2 * * 6', async () => {
    console.log('Processing Kontramarka parsing results...');
    const operation = await OperationsSchema.findOne({
      type: OPERATION_TYPES.parsingEventsFromKontramarka,
      status: OPERATION_STATUSES.pending
    }).sort({ createdAt: -1 });
    
    if (operation) {
      await processParsingResults(operation._id);
    }
  }, { timezone: 'UTC' });
  
  // Воскресенье 02:00 - обработка всех необработанных операций
  cron.schedule('0 2 * * 0', async () => {
    console.log('Processing all unprocessed operations...');
    try {
      const unprocessedOperations = await ParsingServerServices.getUnprocessedOperations();
      
      for (const opData of unprocessedOperations) {
        const remoteOperationId = opData.operation._id;
        
        // Ищем соответствующую операцию на основном сервере
        const localOperation = await OperationsSchema.findOne({
          'meta.remoteOperationId': remoteOperationId
        });
        
        if (localOperation) {
          // Обрабатываем результаты
          await OperationsServices.processParsingResults({
            operationId: localOperation._id,
            remoteOperationId: remoteOperationId
          });
        } else {
          // Если операции нет на основном сервере - создаем новую
          const newOperation = new OperationsSchema({
            type: opData.operation.type,
            admin: null,
            status: opData.operation.status,
            statistics: opData.operation.statistics || '',
            errorText: opData.operation.errorText || '',
            infoText: opData.operation.infoText || '',
            finish_time: opData.operation.finish_time,
            meta: { remoteOperationId }
          });
          await newOperation.save();
          
          // Обрабатываем события, если они есть
          if (opData.events && opData.events.length > 0) {
            await OperationsServices.processParsedEvents({ events: opData.events });
          }
        }
      }
      
      console.log(`Processed ${unprocessedOperations.length} unprocessed operations`);
    } catch (error) {
      console.error('Error processing unprocessed operations:', error);
    }
  }, { timezone: 'UTC' });
  
  console.log('Cron is set up');
};
```

### 6. Модификация OperationsSchema

Добавить поля `meta` и `infoText`:
```javascript
meta: {
  type: Schema.Types.Mixed,  // или Object
  default: {}
},
infoText: {
  type: String,
  default: ''
}
```

### 7. Обработка edge cases

- **Если второй сервер недоступен:**
  - При создании операции - вернуть ошибку, операция не создается
  - При чтении результатов - пометить операцию как `error`, логировать ошибку

- **Если операция на втором сервере еще в процессе (pending):**
  - При чтении результатов - не обрабатывать, пропустить (будет обработано в воскресенье)

- **Повторная обработка:**
  - Флаг `is_processed` на втором сервере предотвращает повторную обработку
  - В воскресенье обрабатываются все необработанные операции

---

## Резюме изменений

### Ключевые моменты:

1. **infoText** - добавлен в OperationsSchema на обоих серверах для хранения прогресса парсинга
2. **Обработка только завершенных операций** - если `pending`, не трогать. Брать мероприятия только когда `success` или `error` И есть events
3. **Авторизация** - простая через API key из .env, передается в заголовке `X-API-Key`
4. **Частичные результаты** - обрабатываются сразу
5. **Воскресенье** - запрос на получение всех необработанных операций (success, не прочитанные ранее)
6. **Очистка данных** - эндпоинт `/parsing/cleanup` создан, но пока не вызывается с основного сервера
7. **Прогресс** - хранится в `infoText`, обновляется после каждого батча
8. **Retry не нужен** - в воскресенье всё равно заберётся
