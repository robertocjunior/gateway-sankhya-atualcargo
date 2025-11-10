import winston from 'winston';
import 'winston-daily-rotate-file'; // Importa o novo pacote
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
    
    // --- TRANSPORT DE ERRO ATUALIZADO ---
    new winston.transports.DailyRotateFile({
      level: 'error',
      filename: path.resolve(logDir, 'error-%DATE%.log'), // O %DATE% é adicionado
      datePattern: 'YYYY-MM-DD', // Rotaciona diariamente
      zippedArchive: true, // Compacta logs antigos
      maxSize: '20m', // Tamanho máximo de 20MB por arquivo
      maxFiles: '30d', // Apaga arquivos mais antigos que 30 dias
    }),
    
    // --- TRANSPORT GERAL ATUALIZADO ---
    new winston.transports.DailyRotateFile({
      filename: path.resolve(logDir, 'combined-%DATE%.log'), // O %DATE% é adicionado
      datePattern: 'YYYY-MM-DD', // Rotaciona diariamente
      zippedArchive: true, // Compacta logs antigos
      maxSize: '20m', // Tamanho máximo de 20MB por arquivo
      maxFiles: '30d', // Apaga arquivos mais antigos que 30 dias
    }),
  ],
  exceptionHandlers: [
    new winston.transports.File({
      filename: path.resolve(logDir, 'exceptions.log'),
    }),
  ],
  rejectionHandlers: [
    new winston.transports.File({
      filename: path.resolve(logDir, 'rejections.log'),
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