import mongoose from 'mongoose';
import OperationsSchema from '../src/schemas/OperationsSchema';
import { OPERATION_STATUSES, OPERATION_TYPES } from '../src/helpers/constants';
import startParsingOperation from '../src/helpers/startParsingOperation';
import { createLoggerWithSource } from '../src/helpers/logger';

const logger = createLoggerWithSource('RUN_TICKETMASTER');

const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });

const parseArgs = () => {
  const args = process.argv.slice(2);
  const meta = { specialization: 'Event' };

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--country' && args[i + 1]) {
      meta.countryCode = args[i + 1];
      i += 1;
    } else if (arg === '--max-pages' && args[i + 1]) {
      meta.maxPages = parseInt(args[i + 1], 10);
      i += 1;
    }
  }

  return meta;
};

async function main() {
  const meta = parseArgs();
  const { DB_NAME, TICKETMASTER_API_KEY } = process.env;

  if (!DB_NAME) {
    logger.error('DB_NAME is not set in .env');
    process.exit(1);
  }

  if (!TICKETMASTER_API_KEY) {
    logger.error('TICKETMASTER_API_KEY is not set in .env');
    process.exit(1);
  }

  await mongoose.connect(`mongodb://localhost:27017/${DB_NAME}`);

  const scope = meta.countryCode ? `country=${meta.countryCode}` : 'DB countries with Ticketmaster coverage';
  logger.info(`Starting Ticketmaster parsing (${scope})...`);

  const operationId = await startParsingOperation(
    OPERATION_TYPES.parsingEventsFromTicketmaster,
    meta,
  );

  const pollIntervalMs = 5000;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    // eslint-disable-next-line no-await-in-loop
    await sleep(pollIntervalMs);

    // eslint-disable-next-line no-await-in-loop
    const operation = await OperationsSchema.findById(operationId).lean();
    if (!operation) {
      logger.error('Operation not found');
      break;
    }

    if (operation.status === OPERATION_STATUSES.success) {
      const stats = operation.statistics ? JSON.parse(operation.statistics) : {};
      logger.info(`Done: ${operation.infoText || ''}`);
      logger.info(`Statistics: ${JSON.stringify({
        total: stats.total,
        countryCodes: stats.countryCodes,
        countriesProcessed: stats.countriesProcessed,
        skippedNoVenue: stats.skippedNoVenue,
        skippedNoCity: stats.skippedNoCity,
        skippedCitiesOver5: stats.skippedCitiesOver5 || {},
      })}`);
      break;
    }

    if (operation.status === OPERATION_STATUSES.error) {
      logger.error(`Parsing failed: ${operation.errorText || 'Unknown error'}`);
      if (operation.statistics) {
        logger.error(`Statistics: ${operation.statistics}`);
      }
      break;
    }
  }

  await mongoose.disconnect();
}

main().catch((error) => {
  logger.error(`Fatal error: ${error.message || error}`, error);
  mongoose.disconnect().finally(() => process.exit(1));
});
