import dotenv from 'dotenv';
dotenv.config();

export const appConfig = {
  logLevel: process.env.LOG_LEVEL || 'info',
  timeout: parseInt(process.env.REQUEST_TIMEOUT_SECONDS, 10) * 1000 || 120000,
  sankhyaRetryLimit: parseInt(process.env.SANKHYA_RETRY_LIMIT, 10) || 3,
  sankhyaRetryDelay:
    (parseInt(process.env.SANKHYA_RETRY_DELAY_MINUTES, 10) || 1) * 60 * 1000,
};