import winston, { format, transports } from 'winston'; // Importa o 'winston' (default) e também 'format' e 'transports'
import 'winston-daily-rotate-file'; 
import path from 'path';
import fs from 'fs';

const { combine, timestamp, printf, colorize, json } = format;

// Garante que o diretório de logs exista
const logDir = 'logs';
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// Formato para o console
const consoleFormat = combine(
  colorize(),
  timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  printf(({ level, message, timestamp, service }) => {
    // Adiciona o service (ex: [Sankhya]) se ele existir
    const srv = service ? ` [${service}]` : '';
    return `[${timestamp}] ${level}:${srv} ${message}`;
  })
);

// Formato para os arquivos
const fileFormat = combine(
  timestamp(),
  json()
);

// CORREÇÃO: Usamos 'winston.createLogger' para não conflitar
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info', 
  format: fileFormat,
  transports: [
    new transports.DailyRotateFile({
      level: 'error',
      filename: path.resolve(logDir, 'error-%DATE%.log'),
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '20m',
      maxFiles: '30d', 
    }),
    new transports.DailyRotateFile({
      filename: path.resolve(logDir, 'app-%DATE%.log'),
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '20m',
      maxFiles: '30d', 
    }),
  ],
  exceptionHandlers: [
    new transports.File({
      filename: path.resolve(logDir, 'exceptions.log'),
    }),
  ],
});

if (process.env.NODE_ENV !== 'production') {
  logger.add(new transports.Console({
    format: consoleFormat,
  }));
}

/**
 * Cria um logger filho com um contexto de serviço (ex: [Sankhya]).
 * Esta é a nossa função 'createLogger' personalizada.
 */
export const createLogger = (service) => {
  return logger.child({ service });
};


export default logger;