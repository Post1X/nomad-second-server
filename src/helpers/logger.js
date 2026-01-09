import winston from 'winston';
import fs from 'fs';
import path from 'path';

const { format, createLogger, transports } = winston;
const { combine, timestamp, printf, label } = format;

const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

const logFormat = printf(({ message, label: source, timestamp: ts }) => {
  const date = new Date(ts).toISOString();
  return `${source} | ${date} | ${message}`;
});

const logger = createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp(),
    format.errors({ stack: false }),
    logFormat
  ),
  transports: [
    new transports.Console({
      format: combine(
        timestamp(),
        logFormat
      ),
    }),
    new transports.File({
      filename: 'logs/error.log',
      level: 'error',
      format: combine(
        timestamp(),
        logFormat
      ),
    }),
    new transports.File({
      filename: 'logs/combined.log',
      format: combine(
        timestamp(),
        logFormat
      ),
    }),
  ],
});

export const createLoggerWithSource = (source) => {
  return {
    info: (message) => logger.info(message, { label: source }),
    error: (message) => logger.error(message, { label: source }),
    warn: (message) => logger.warn(message, { label: source }),
    debug: (message) => logger.debug(message, { label: source }),
  };
};

export default logger;

