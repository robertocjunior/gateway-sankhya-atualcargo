import 'dotenv/config';
import logger from '../utils/logger.js';

// Validação de variáveis essenciais
const requiredEnvVars = [
  'SANKHYA_URL',
  'SANKHYA_USER',
  'SANKHYA_PASSWORD',
  'JOB_RETRY_DELAY_MS',
  'REQUEST_TIMEOUT_MS',
];

for (const varName of requiredEnvVars) {
  if (!process.env[varName]) {
    const errorMsg = `Variável de ambiente obrigatória ${varName} não definida.`;
    logger.error(errorMsg);
    throw new Error(errorMsg);
  }
}

// Configurações Globais
export const appConfig = {
  logLevel: process.env.LOG_LEVEL || 'info',
  timeout: Number(process.env.REQUEST_TIMEOUT_MS),
  jobRetryDelayMs: Number(process.env.JOB_RETRY_DELAY_MS),
  sankhyaRetryLimit: Number(process.env.SANKHYA_RETRY_LIMIT_BEFORE_SWAP) || 2,
};

// Configuração do Sankhya
export const sankhyaConfig = {
  url: process.env.SANKHYA_URL,
  contingencyUrl: process.env.SANKHYA_CONTINGENCY_URL || null,
  username: process.env.SANKHYA_USER,
  password: process.env.SANKHYA_PASSWORD,
  iscaDatasetId: process.env.SANKHYA_ISCA_DATASET_ID || '02S',
};

// Configuração dos Jobs (APIs de Rastreamento)
export const jobsConfig = {
  atualcargo: {
    enabled: !!(process.env.ATUALCARGO_URL && process.env.ATUALCARGO_API_KEY),
    interval: Number(process.env.JOB_INTERVAL_ATUALCARGO) || 300000,
    url: process.env.ATUALCARGO_URL,
    apiKey: process.env.ATUALCARGO_API_KEY,
    username: process.env.ATUALCARGO_USERNAME,
    password: process.env.ATUALCARGO_PASSWORD,
    tokenExpirationMs: Number(process.env.ATUALCARGO_TOKEN_EXPIRATION_MS) || 270000,
    fabricanteId: process.env.SANKHYA_ISCA_FABRICANTE_ID_ATUALCARGO || '2',
  },
  sitrax: {
    enabled: !!(process.env.SITRAX_URL && process.env.SITRAX_LOGIN),
    interval: Number(process.env.JOB_INTERVAL_SITRAX) || 300000,
    url: process.env.SITRAX_URL,
    login: process.env.SITRAX_LOGIN,
    cgruChave: process.env.SITRAX_CGRUCHAVE,
    cusuChave: process.env.SITRAX_CUSUCHAVE,
    fabricanteId: process.env.SANKHYA_ISCA_FABRICANTE_ID_SITRAX || '3', // Default '3'
  },
};