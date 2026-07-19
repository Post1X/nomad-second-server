import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { EXPIRED_EVENTS_CLEANUP_MONTHS } from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CLEANUP');

class CleanupServices {
  static async cleanupExpiredEvents(months = EXPIRED_EVENTS_CLEANUP_MONTHS) {
    const cutoffDate = new Date();
    cutoffDate.setMonth(cutoffDate.getMonth() - months);

    const result = await ParsedEventsSchema.deleteMany({
      $or: [
        { 'event_data.date_end': { $lt: cutoffDate } },
        {
          'event_data.date_end': { $in: [null, ''] },
          'event_data.date_start': { $lt: cutoffDate },
        },
      ],
    });

    logger.info(
      `Expired cleanup: deleted ${result.deletedCount} events older than ${months} months `
      + `(cutoff=${cutoffDate.toISOString()})`,
    );

    return {
      deletedEvents: result.deletedCount,
      months,
      cutoff: cutoffDate,
    };
  }

  static async cleanupTakenEvents(months = EXPIRED_EVENTS_CLEANUP_MONTHS) {
    return this.cleanupExpiredEvents(months);
  }
}

export default CleanupServices;
