import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { OPERATION_STATUSES, OPERATION_TYPES } from '../helpers/constants';
import parseFienta from '../operations/parseFienta';
import parseEventim from '../operations/parseEventim';
import parseKontramarka from '../operations/parseKontramarka';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('PARSING_CONTROLLER');

class ParsingController {
  // POST /parsing/create
  static create = async (req, res, next) => {
    try {
      const { type, meta } = req.body;

      if (!Object.values(OPERATION_TYPES).includes(type)) {
        return res.status(400).json({
          status: 'error',
          message: `Invalid operation type. Must be one of: ${Object.values(OPERATION_TYPES).join(', ')}`,
        });
      }

      const operation = new OperationsSchema({
        type,
        status: OPERATION_STATUSES.pending,
        statistics: '',
        errorText: '',
        infoText: 'Operation created, starting parsing...',
        is_processed: false,
      });
      await operation.save();

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
          logger.error(`Error in parsing operation ${operation._id}: ${error.message || error}`);
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

      const operation = await OperationsSchema.findById(operationId);
      if (!operation) {
        return res.status(404).json({
          status: 'error',
          message: 'Operation not found',
        });
      }

      const parsedEvents = await ParsedEventsSchema.find({ operation: operationId })
        .sort({ batch_number: 1 })
        .lean();

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

  // GET /parsing/operations
  static getOperations = async (req, res, next) => {
    try {
      const {
        type,
        limit,
        skip,
        includeEvents = 'false',
      } = req.query;

      const filter = {
        is_taken: { $ne: true },
      };

      if (!type) {
        return res.status(400).json({
          status: 'error',
          message: 'Parameter "type" is required',
        });
      }

      filter.type = type;

      let query = OperationsSchema.find(filter).sort({ createdAt: -1       });

      const hasPagination = limit !== undefined || skip !== undefined;
      
      if (hasPagination) {
        const limitValue = limit ? parseInt(limit, 10) : undefined;
        const skipValue = skip ? parseInt(skip, 10) : 0;
        
        if (limitValue) {
          query = query.limit(limitValue);
        }
        if (skipValue) {
          query = query.skip(skipValue);
        }
      }

      const operations = await query.lean();
      const total = await OperationsSchema.countDocuments(filter);

      const result = [];
      const operationIds = [];

      const shouldIncludeEvents = includeEvents === 'true';

      for (const operation of operations) {
        operationIds.push(operation._id);

        const operationData = {
          _id: operation._id,
          type: operation.type,
          status: operation.status,
          statistics: operation.statistics,
          errorText: operation.errorText,
          infoText: operation.infoText,
          createdAt: operation.createdAt,
          updatedAt: operation.updatedAt,
          finish_time: operation.finish_time,
          is_processed: operation.is_processed,
          is_taken: operation.is_taken,
        };

        if (shouldIncludeEvents) {
          const parsedEvents = await ParsedEventsSchema.find({ operation: operation._id })
            .sort({ batch_number: 1 })
            .lean();

          const events = parsedEvents.map(pe => pe.event_data);

          operationData.events = events;
          operationData.totalEvents = events.length;
        }

        result.push(operationData);
      }

      if (operationIds.length > 0) {
        await OperationsSchema.updateMany(
          { _id: { $in: operationIds } },
          { $set: { is_taken: true } }
        );
      }

      const response = {
        status: 'ok',
        operations: result,
        total,
      };

      if (hasPagination) {
        response.limit = limit ? parseInt(limit, 10) : undefined;
        response.skip = skip ? parseInt(skip, 10) : 0;
      }

      res.json(response);
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

