import mongoose from 'mongoose';
import ParsedEventsSchema from '../schemas/ParsedEventsSchema';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('DB');
const { DB_NAME } = process.env;

mongoose.set('strictQuery', false);

export default async function (cb) {
  try {
    await mongoose.connect(`mongodb://localhost:27017/${DB_NAME}`);

    logger.info('Connected to db');

    // Drop obsolete indexes (fingerprint / exported_at) to match schema
    try {
      const dropped = await ParsedEventsSchema.syncIndexes();
      if (dropped?.length) {
        logger.info(`ParsedEvents syncIndexes dropped: ${dropped.join(', ')}`);
      }
    } catch (idxErr) {
      logger.error(`ParsedEvents syncIndexes failed: ${idxErr.message || idxErr}`);
    }

    if (typeof cb === 'function') {
      cb();
    }
  } catch (e) {
    logger.error(`Database connection error: ${e.message || e}`);
  }
}

