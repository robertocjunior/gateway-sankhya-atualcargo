import { createLogger } from '../utils/logger.js';
// CAMINHOS CORRIGIDOS
import { jobsConfig, sankhyaConfig, appConfig } from '../config/index.js';
import { delay } from '../utils/dateTime.js';
import { TokenError, AtualcargoTokenError, SankhyaTokenError } from '../utils/errors.js';
import { createJobStateManager } from './job.scheduler.js';

import * as atualcargoApi from '../connectors/atualcargo.connector.js'; // CAMINHO CORRIGIDO
import * as sankhyaProcessor from '../sankhya/sankhya.processor.js';
import { mapAtualcargoToStandard } from '../sankhya/sankhya.mapper.js';

const config = jobsConfig.atualcargo;
const JOB_NAME = 'Atualcargo';
const logger = createLogger(`Job:${JOB_NAME}`);

// --- Gerenciamento de Estado (Token, Cache, URL Sankhya) ---
let token = null;
let tokenTimestamp = null;
// CORRIGIDO: Passando 'sankhyaConfig' e 'appConfig'
const state = createJobStateManager(JOB_NAME, { sankhya: sankhyaConfig, app: appConfig });

/**
 * Garante que temos um token válido da Atualcargo.
 */
async function ensureAtualcargoToken() {
  const now = Date.now();
  if (tokenTimestamp && (now - tokenTimestamp > config.tokenExpirationMs)) {
    logger.info(`Token expirou (limite de ${config.tokenExpirationMs / 60000} min). Forçando renovação.`);
    token = null;
    tokenTimestamp = null;
  }
  
  if (!token) {
    logger.info('Token ausente ou expirado. Solicitando novo login...');
    token = await atualcargoApi.loginAtualcargo();
    tokenTimestamp = Date.now();
  }
}

/**
 * Ponto de entrada do Job, chamado pelo index.js
 */
export async function run() {
  try {
    // --------------------------------------------------
    // ETAPA 1: EXTRACT (Atualcargo)
    // --------------------------------------------------
    if (!state.getCache()) {
      logger.info('Cache de posições vazio. Buscando na API...');
      await ensureAtualcargoToken();
      const positions = await atualcargoApi.getAtualcargoPositions(token);
      
      if (!positions || positions.length === 0) {
        logger.info('Nenhuma posição de veículo recebida. Encerrando ciclo.');
        return; 
      }

      const standardData = mapAtualcargoToStandard(positions);
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

    if (error instanceof AtualcargoTokenError) {
      logger.warn('Forçando re-login da Atualcargo no próximo ciclo.');
      token = null;
      tokenTimestamp = null;
      state.clearCache(); 
    
    } else if (error instanceof SankhyaTokenError) {
      logger.warn(`Erro de Token/Sessão Sankhya. O job tentará novamente com os mesmos dados.`);
      // O cache NÃO é limpo
    
    } else if (error.message.includes('Atualcargo')) {
        logger.warn('Erro de rede ou Rate Limit na Atualcargo. Limpando token e cache.');
        token = null;
        tokenTimestamp = null;
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