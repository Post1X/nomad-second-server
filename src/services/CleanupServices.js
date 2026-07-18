import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { TAKEN_EVENTS_CLEANUP_DAYS } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CLEANUP');

class CleanupServices {
    static async cleanupTakenEvents(days = TAKEN_EVENTS_CLEANUP_DAYS) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - days);

    const takenOperations = await OperationsSchema.find({
      is_taken: true,
      taken_at: { $lt: cutoffDate },
    }).select('_id').lean();

        const legacyOps = await OperationsSchema.find({
      is_taken: true,
      taken_at: { $exists: false },
      $or: [
        { finish_time: { $lt: cutoffDate } },
        { updatedAt: { $lt: cutoffDate } },
      ],
    }).select('_id').lean();

    const operationIds = [...new Set([
      ...takenOperations.map((op) => String(op._id)),
      ...legacyOps.map((op) => String(op._id)),
    ])];

    if (operationIds.length === 0) {
      logger.info(`Cleanup: nothing to delete (cutoff=${cutoffDate.toISOString()}, days=${days})`);
      return { deletedEvents: 0, deletedOperations: 0, operationIds: [] };
    }

    const ids = operationIds;
    const deleteEvents = await ParsedEventsSchema.deleteMany({
      operation: { $in: ids },
    });
    const deleteOps = await OperationsSchema.deleteMany({
      _id: { $in: ids },
    });

    logger.info(
      `Cleanup: deleted ${deleteEvents.deletedCount} events, ${deleteOps.deletedCount} operations `
      + `(ops=${ids.length}, days=${days})`,
    );

    return {
      deletedEvents: deleteEvents.deletedCount,
      deletedOperations: deleteOps.deletedCount,
      operationIds: ids,
    };
  }
}

export default CleanupServices;
