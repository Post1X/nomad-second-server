import mongoose from 'mongoose';
import { createLoggerWithSource } from './logger';

const logger = createLoggerWithSource('DB');
const { DB_NAME } = process.env;

mongoose.set('strictQuery', false);

export default async function (cb) {
  try {
    await mongoose.connect(`mongodb://localhost:27017/${DB_NAME}`);

    logger.info('Connected to db');

    if (typeof cb === 'function') {
      cb();
    }
  } catch (e) {
    logger.error(`Database connection error: ${e.message || e}`);
  }
}

