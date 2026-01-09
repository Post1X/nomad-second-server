import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { OPERATION_STATUSES, OPERATION_TYPES } from '../helpers/constants';
import parseFienta from '../operations/parseFienta';
import parseEventim from '../operations/parseEventim';
import parseKontramarka from '../operations/parseKontramarka';

class ParsingController {
  // POST /parsing/create
  static create = async (req, res, next) => {
    try {
      const { type, meta } = req.body;

      // Валидация типа парсера
      if (!Object.values(OPERATION_TYPES).includes(type)) {
        return res.status(400).json({
          status: 'error',
          message: `Invalid operation type. Must be one of: ${Object.values(OPERATION_TYPES).join(', ')}`,
        });
      }

      // Создание записи операции
      const operation = new OperationsSchema({
        type,
        status: OPERATION_STATUSES.pending,
        statistics: '',
        errorText: '',
        infoText: 'Operation created, starting parsing...',
        is_processed: false,
      });
      await operation.save();

      // Запуск скрипта парсинга в фоне (async, не блокируя ответ)
      setImmediate(async () => {
        try {
          await OperationsSchema.findByIdAndUpdate(operation._id, {
            status: OPERATION_STATUSES.processing,
            infoText: 'Parsing started...',
          });

          let parseFunction;
          switch (type) {
            case OPERATION_TYPES.parsingEventsFromFienta:
              parseFunction = parseFienta;
              break;
            case OPERATION_TYPES.parsingEventsFromEventim:
              parseFunction = parseEventim;
              break;
            case OPERATION_TYPES.parsingEventsFromKontramarka:
              parseFunction = parseKontramarka;
              break;
            default:
              throw new Error(`Unknown parser type: ${type}`);
          }

          await parseFunction({ meta, operationId: operation._id });

        } catch (error) {
          console.error(`Error in parsing operation ${operation._id}:`, error);
          await OperationsSchema.findByIdAndUpdate(operation._id, {
            status: OPERATION_STATUSES.error,
            errorText: error.message || 'Unknown error occurred',
            finish_time: new Date(),
          });
        }
      });

      res.json({
        status: 'ok',
        operationId: operation._id.toString(),
        message: 'Operation created and started',
      });
    } catch (error) {
      next(error);
    }
  };

  // GET /parsing/results/:operationId
  static getResults = async (req, res, next) => {
    try {
      const { operationId } = req.params;

      // Поиск операции
      const operation = await OperationsSchema.findById(operationId);
      if (!operation) {
        return res.status(404).json({
          status: 'error',
          message: 'Operation not found',
        });
      }

      // Поиск всех событий для данной операции
      const parsedEvents = await ParsedEventsSchema.find({ operation: operationId })
        .sort({ batch_number: 1 })
        .lean();

      // Преобразование event_data в массив событий
      const events = parsedEvents.map(pe => pe.event_data);

      res.json({
        status: 'ok',
        operation: {
          _id: operation._id,
          type: operation.type,
          status: operation.status,
          statistics: operation.statistics,
          errorText: operation.errorText,
          infoText: operation.infoText,
          createdAt: operation.createdAt,
          finish_time: operation.finish_time,
        },
        events,
        totalEvents: events.length,
      });
    } catch (error) {
      next(error);
    }
  };

  // GET /parsing/unprocessed
  static getUnprocessed = async (req, res, next) => {
    try {
      // Поиск всех операций со статусом success и is_processed: false
      const operations = await OperationsSchema.find({
        status: OPERATION_STATUSES.success,
        is_processed: false,
      }).sort({ createdAt: -1 });

      const result = [];

      for (const operation of operations) {
        const parsedEvents = await ParsedEventsSchema.find({ operation: operation._id })
          .sort({ batch_number: 1 })
          .lean();

        const events = parsedEvents.map(pe => pe.event_data);

        result.push({
          operation: {
            _id: operation._id,
            type: operation.type,
            status: operation.status,
            statistics: operation.statistics,
            errorText: operation.errorText,
            infoText: operation.infoText,
            createdAt: operation.createdAt,
            finish_time: operation.finish_time,
          },
          events,
          totalEvents: events.length,
        });
      }

      res.json({
        status: 'ok',
        operations: result,
      });
    } catch (error) {
      next(error);
    }
  };

  // POST /parsing/cleanup
  static cleanup = async (req, res, next) => {
    try {
      const { days = 30 } = req.body;

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - days);

      // Удаление ParsedEventsSchema старше N дней для обработанных операций
      const processedOperations = await OperationsSchema.find({
        status: OPERATION_STATUSES.success,
        is_processed: true,
        finish_time: { $lt: cutoffDate },
      }).select('_id');

      const operationIds = processedOperations.map(op => op._id);

      const deleteResult = await ParsedEventsSchema.deleteMany({
        operation: { $in: operationIds },
        createdAt: { $lt: cutoffDate },
      });

      res.json({
        status: 'ok',
        deletedCount: deleteResult.deletedCount,
        message: 'Cleanup completed',
      });
    } catch (error) {
      next(error);
    }
  };
}

export default ParsingController;

