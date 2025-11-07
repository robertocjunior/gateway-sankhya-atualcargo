import winston from 'winston';
import path from 'path';
import fs from 'fs';
import config from './config.js';

const { combine, timestamp, printf, colorize, align } = winston.format;

// Garante que o diretório de logs exista
const logDir = 'logs';
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// Formato do Log
const logFormat = printf(({ level, message, timestamp, service }) => {
  const srv = service ? `[${service}]` : '';
  return `${timestamp} ${level}: ${srv} ${message}`;
});

const logger = winston.createLogger({
  level: config.service.logLevel,
  format: combine(timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }), logFormat),
  transports: [
    new winston.transports.Console({
      format: combine(colorize(), align(), logFormat),
    }),
    new winston.transports.File({
      filename: path.resolve(process.cwd(), logDir, 'error.log'),
      level: 'error',
    }),
    new winston.transports.File({
      filename: path.resolve(process.cwd(), logDir, 'combined.log'),
    }),
  ],
  exceptionHandlers: [
    new winston.transports.File({
      filename: path.resolve(process.cwd(), logDir, 'exceptions.log'),
    }),
  ],
  rejectionHandlers: [
    new winston.transports.File({
      filename: path.resolve(process.cwd(), logDir, 'rejections.log'),
    }),
  ],
});

/**
 * Cria um logger filho com um contexto de serviço.
 * @param {string} service - O nome do serviço (ex: 'Sankhya', 'Atualcargo')
 */
export const createLogger = (service) => {
  return logger.child({ service });
};

export default logger;