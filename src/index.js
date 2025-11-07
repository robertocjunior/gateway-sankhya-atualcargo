import config from './config.js';
import logger from './logger.js';
import { runEtlProcess } from './core/etlProcess.js';

/**
 * Função principal que gerencia o loop do serviço.
 * Utiliza setTimeout recursivo para evitar sobreposição de execuções.
 */
async function mainLoop() {
  logger.info('--------------------------------------------------');
  logger.info(`Iniciando novo ciclo de integração...`);
  
  try {
    await runEtlProcess();
  } catch (error) {
    // Erros graves não tratados no etlProcess são pegos aqui
    logger.error(`[MainLoop] Erro fatal não tratado no ciclo: ${error.message}`, { 
      stack: error.stack 
    });
  } finally {
    const nextRunMin = config.service.loopInterval / 60000;
    logger.info(`Ciclo finalizado. Próxima execução em ${nextRunMin} minutos.`);
    logger.info('--------------------------------------------------');
    
    // Agenda a próxima execução 
    setTimeout(mainLoop, config.service.loopInterval);
  }
}

// --- Início do Serviço ---
logger.info('[Serviço] Iniciando serviço de integração Sankhya-Atualcargo.');
logger.info(`Intervalo de execução: ${config.service.loopInterval / 60000} minutos.`);
logger.info(`Timeout de requisição: ${config.service.timeout / 1000} segundos.`);

// Inicia o primeiro ciclo
mainLoop();