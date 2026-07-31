import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import {
  EXPIRED_EVENTS_CLEANUP_DAYS,
  EXPIRED_EVENTS_CLEANUP_MONTHS,
} from '../helpers/constants';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('CLEANUP');

class CleanupServices {
  /**
   * Delete ParsedEvents whose event date_end (or date_start fallback) is older than `days` days.
   * Default: 2 days after the event ended.
   */
  static async cleanupExpiredEventsByDays(days = EXPIRED_EVENTS_CLEANUP_DAYS) {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - Number(days));

    const result = await ParsedEventsSchema.deleteMany({
      $or: [
        { 'event_data.date_end': { $lt: cutoffDate } },
        {
          $and: [
            {
              $or: [
                { 'event_data.date_end': { $exists: false } },
                { 'event_data.date_end': null },
                { 'event_data.date_end': '' },
              ],
            },
            { 'event_data.date_start': { $lt: cutoffDate } },
          ],
        },
      ],
    });

    logger.info(
      `Expired cleanup: deleted ${result.deletedCount} events with date_end older than ${days} days `
      + `(cutoff=${cutoffDate.toISOString()})`,
    );

    return {
      deletedEvents: result.deletedCount,
      days,
      cutoff: cutoffDate,
    };
  }

  /** @deprecated use cleanupExpiredEventsByDays — kept for manual/admin callers */
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
      `Expired cleanup (months): deleted ${result.deletedCount} events older than ${months} months `
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
