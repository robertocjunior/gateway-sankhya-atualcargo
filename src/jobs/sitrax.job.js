import { createLogger } from '../utils/logger.js';
// CAMINHOS CORRIGIDOS
import { jobsConfig, sankhyaConfig, appConfig } from '../config/index.js';
import { delay } from '../utils/dateTime.js';
import { SankhyaTokenError } from '../utils/errors.js';
import { createJobStateManager } from './job.scheduler.js';

import * as sitraxApi from '../connectors/sitrax.connector.js'; // CAMINHO CORRIGIDO
import * as sankhyaProcessor from '../sankhya/sankhya.processor.js';
import { mapSitraxToStandard } from '../sankhya/sankhya.mapper.js';

const config = jobsConfig.sitrax;
const JOB_NAME = 'Sitrax';
const logger = createLogger(`Job:${JOB_NAME}`);

// --- Gerenciamento de Estado ---
const state = createJobStateManager(JOB_NAME, { sankhya: sankhyaConfig, app: appConfig });

/**
 * Ponto de entrada do Job, chamado pelo index.js
 */
export async function run() {
  try {
    // --------------------------------------------------
    // ETAPA 1: EXTRACT (Sitrax)
    // --------------------------------------------------
    if (!state.getCache()) {
      logger.info('Cache de posições vazio. Buscando na API...');
      const positions = await sitraxApi.getSitraxPositions();
      
      if (!positions || positions.length === 0) {
        logger.info('Nenhuma posição de isca recebida. Encerrando ciclo.');
        return;
      }

      const standardData = mapSitraxToStandard(positions);
      state.setCache(standardData);
      logger.info(`Dados salvos no cache: ${standardData.length} posições.`);
    } else {
      logger.info('Usando posições do cache. Pulando busca na API.');
    }

    // --------------------------------------------------
    // ETAPA 2: LOAD (Sankhya)
    // --------------------------------------------------
    const cachedData = state.getCache();
    if (!cachedData || cachedData.length === 0) {
      logger.info('Cache de posições vazio. Pulando etapa do Sankhya.');
      return;
    }
    
    await sankhyaProcessor.processPositions(
      cachedData,
      JOB_NAME,
      state.sankhyaUrl,
      config.fabricanteId
    );
    
    state.handleSankhyaSuccess();
    state.clearCache(); 

  } catch (error) {
    logger.error(`Erro no ciclo [${JOB_NAME}]: ${error.message}`);

    if (error instanceof SankhyaTokenError) {
      logger.warn(`Erro de Token/Sessão Sankhya. O job tentará novamente com os mesmos dados.`);
      // O cache NÃO é limpo
    
    } else if (error.message.includes('Sitrax')) {
        logger.warn('Erro de rede na Sitrax. Limpando cache.');
        state.clearCache();
    
    } else {
      // Erro de rede/timeout do Sankhya
      state.handleSankhyaError(error);
      // O cache NÃO é limpo
    }

    logger.info(`Aguardando ${appConfig.jobRetryDelayMs / 1000}s antes de tentar o job novamente...`);
    await delay(appConfig.jobRetryDelayMs);
  }
}