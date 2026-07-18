import OperationsSchema from '../schemas/OperationsSchema';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { processParsedEvents } from '../services/ProcessParsedEventsServices';
import { createLoggerWithSource } from '../helpers/logger';

const logger = createLoggerWithSource('SAVE_EVENTS');

export async function saveProcessedEvents({
  operationId,
  events,
  source,
  infoTexts = [],
  errorTexts = [],
  extraStatistics = {},
}) {
  const { events: processed, stats: processStats } = await processParsedEvents(events || [], source);

  const BATCH_SIZE = 10;
  try {
    for (let i = 0; i < processed.length; i += BATCH_SIZE) {
      const batch = processed.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;

      // eslint-disable-next-line no-await-in-loop
      await ParsedEventsSchema.insertMany(
        batch.map((event) => ({
          operation: operationId,
          event_data: event,
          batch_number: batchNumber,
        })),
      );

      // eslint-disable-next-line no-await-in-loop
      const operation = await OperationsSchema.findById(operationId);
      // eslint-disable-next-line no-await-in-loop
      await OperationsSchema.findByIdAndUpdate(operationId, {
        infoText: `${operation?.infoText || ''}\nОбработано ${i + batch.length} из ${processed.length} событий. Батч ${batchNumber}`,
      });
    }

    const operation = await OperationsSchema.findById(operationId);
    const finalInfoText = operation?.infoText || '';
    const additionalInfo = infoTexts.length > 0 ? `\n${infoTexts.join('\n')}` : '';

    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'success',
      finish_time: new Date(),
      statistics: JSON.stringify({
        total: processed.length,
        batches: Math.ceil(processed.length / BATCH_SIZE) || 0,
        errors: errorTexts.length,
        process: processStats,
        ...extraStatistics,
      }),
      errorText: errorTexts.join('\n'),
      infoText: finalInfoText + additionalInfo,
    });

    logger.info(`Saved ${processed.length} events for operation ${operationId}`);
    return { processed, processStats };
  } catch (error) {
    logger.error(`Error saving events: ${error.message || error}`);
    await OperationsSchema.findByIdAndUpdate(operationId, {
      status: 'error',
      errorText: error.message || 'Unknown error while saving events',
      finish_time: new Date(),
    });
    throw error;
  }
}

export default saveProcessedEvents;
