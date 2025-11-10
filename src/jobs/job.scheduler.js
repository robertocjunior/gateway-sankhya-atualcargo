import logger, { createLogger } from '../utils/logger.js'; // Importa o createLogger daqui
import { delay } from '../utils/dateTime.js'; // CORRIGIDO: de 'helpers.js' para 'dateTime.js'
import { appConfig } from '../config/app.js';
import { sankhyaConfig } from '../config/sankhya.js'; // Importa o sankhyaConfig

/**
 * Cria e gerencia um loop de job seguro (setTimeout recursivo).
 * @param {string} name - Nome do Job (para logs)
 * @param {Function} jobFunction - A função async 'run' do job
 * @param {number} intervalMs - O intervalo em milissegundos
 */
export function createJobLoop(name, jobFunction, intervalMs) {
  logger.info(
    `[JobScheduler] Agendando job [${name}] para rodar a cada ${intervalMs / 60000} minutos.`
  );

  const loop = async () => {
    logger.info(`--- [Iniciando Job: ${name}] ---`);
    try {
      await jobFunction();
    } catch (error) {
      // Pega erros não tratados dentro da função 'run' do job
      logger.error(
        `[Job: ${name}] Erro fatal não tratado no loop: ${error.message}`,
        { stack: error.stack }
      );
    } finally {
      const nextRunMin = intervalMs / 60000;
      logger.info(`[Job: ${name}] Ciclo finalizado. Próxima execução em ${nextRunMin} min.`);
      logger.info(`-----------------------------------`);
      
      // Agenda a próxima execução
      setTimeout(loop, intervalMs);
    }
  };

  // Inicia o primeiro ciclo
  loop();
}


/**
 * Cria um gerenciador de estado para um job (cache, URL Sankhya).
 * @param {string} sourceName - Nome do Job (ex: 'Atualcargo')
 * @param {Object} config - Configurações (sankhyaConfig, appConfig)
 */
export function createJobStateManager(sourceName, config) {
  const logger = createLogger(`Job:${sourceName}`);
  
  return {
    cache: null,
    sankhyaUrl: config.sankhya.url, // URL principal
    primaryLoginAttempts: 0,
    
    setCache(data) {
      this.cache = data;
    },
    
    getCache() {
      return this.cache;
    },
    
    clearCache() {
      this.cache = null;
    },
    
    // Lógica de falha e troca de URL do Sankhya
    handleSankhyaError(error) {
      logger.warn(`Erro de rede no Sankhya: ${error.message}. Iniciando lógica de contingência.`);

      if (config.sankhya.contingencyUrl) {
          if (this.sankhyaUrl === config.sankhya.url) {
              this.primaryLoginAttempts++;
              logger.info(`Falha de rede no principal. Tentativa ${this.primaryLoginAttempts}/${config.app.sankhyaRetryLimit}.`);
              
              if (this.primaryLoginAttempts >= config.app.sankhyaRetryLimit) {
                  logger.warn('Limite de falhas no principal atingido. Alternando para contingência.');
                  this.sankhyaUrl = config.sankhya.contingencyUrl;
                  this.primaryLoginAttempts = 0;
              }
          } else {
              logger.warn('Falha de rede na contingência. Voltando para o principal.');
              this.sankhyaUrl = config.sankhya.url; // Volta para o principal
              this.primaryLoginAttempts = 0;
          }
      } else {
          logger.warn('Erro de rede no Sankhya, mas não há URL de contingência definida.');
      }
    },
    
    // Reseta tentativas se o login na URL principal for bem-sucedido
    handleSankhyaSuccess() {
        if (this.sankhyaUrl === config.sankhya.url) {
            this.primaryLoginAttempts = 0;
        }
    }
  };
}