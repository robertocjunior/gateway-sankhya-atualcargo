import logger from './src/utils/logger.js';
import { jobsConfig } from './src/config/index.js';
import { createJobLoop } from './src/jobs/job.scheduler.js';

// Jobs
import * as atualcargoJob from './src/jobs/atualcargo.job.js';
import * as sitraxJob from './src/jobs/sitrax.job.js';

// --- Capturadores Globais ---
process.on('uncaughtException', (error) => {
  logger.error('Erro não capturado (uncaughtException):', error);
  process.exit(1); // Encerra o processo em caso de erro fatal
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Rejeição de Promise não tratada (unhandledRejection):', reason);
});

// --- Iniciar o Hub ---
logger.info('[Serviço] Iniciando Hub de Integração de Rastreamento...');

// Inicia o job da Atualcargo
if (jobsConfig.atualcargo.enabled) {
  createJobLoop(
    'Atualcargo',
    atualcargoJob.run,
    jobsConfig.atualcargo.interval
  );
}

// Inicia o job da Santos e Zanon (Sitrax)
if (jobsConfig.sitrax.enabled) {
  createJobLoop(
    'Sitrax',
    sitraxJob.run,
    jobsConfig.sitrax.interval
  );
}