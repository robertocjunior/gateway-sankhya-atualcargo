import logger from './logger.js';
import { jobsConfig } from './config/jobs.js';
import * as atualcargoJob from './jobs/atualcargoJob.js';
import * as sitraxJob from './jobs/sitraxJob.js';

logger.info('[Serviço] Iniciando Hub de Integração de Rastreamento...');

/**
 * Cria e gerencia um loop de job seguro (setTimeout recursivo).
 * @param {string} name - Nome do Job (para logs)
 * @param {Function} jobFunction - A função async 'run' do job
 * @param {number} intervalMs - O intervalo em milissegundos
 */
function createJob(name, jobFunction, intervalMs) {
  logger.info(
    `[JobScheduler] Agendando job [${name}] para rodar a cada ${intervalMs / 60000} minutos.`
  );

  const loop = async () => {
    logger.info(`---------------- [Job: ${name}] ----------------`);
    try {
      await jobFunction();
    } catch (error) {
      // Pega erros não tratados dentro da função 'run' do job
      logger.error(
        `[Job: ${name}] Erro fatal não tratado no loop: ${error.message}`,
        { stack: error.stack }
      );
    } finally {
      logger.info(`[Job: ${name}] Ciclo finalizado. Próxima execução em ${intervalMs / 60000} min.`);
      logger.info(`--------------------------------------------------`);
      
      // Agenda a próxima execução
      setTimeout(loop, intervalMs);
    }
  };

  // Inicia o primeiro ciclo
  loop();
}

// --- Iniciar Jobs ---

if (jobsConfig.atualcargo.enabled) {
  createJob(
    'Atualcargo',
    atualcargoJob.run,
    jobsConfig.atualcargo.interval
  );
}

if (jobsConfig.sitrax.enabled) {
  createJob(
    'Sitrax',
    sitraxJob.run,
    jobsConfig.sitrax.interval
  );
}