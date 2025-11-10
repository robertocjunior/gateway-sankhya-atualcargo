import axios from 'axios';
import { jobsConfig } from '../config/jobs.js';
import { appConfig } from '../config/app.js';
import { createLogger } from '../utils/logger.js'; // CAMINHO CORRIGIDO

const logger = createLogger('SitraxAPI'); // CORRIGIDO
const config = jobsConfig.sitrax;
const { timeout } = appConfig;

const apiClient = axios.create({
  baseURL: config.url,
  timeout: timeout,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Busca as últimas posições do Sitrax.
 * @returns {Promise<Array<Object>>} Lista de posições
 */
export async function getSitraxPositions() {
  logger.info('Buscando últimas posições...');
  try {
    const requestBody = {
      login: config.login,
      cgruChave: config.cgruChave,
      cusuChave: config.cusuChave,
      pktId: 0,
    };

    const response = await apiClient.post('/ultimaposicao', requestBody);

    if (response.data && Array.isArray(response.data.posicoes)) {
      logger.info(`Recebidas ${response.data.posicoes.length} posições.`);
      return response.data.posicoes;
    }
    
    logger.warn('Resposta da API não contém dados válidos.', response.data);
    return [];

  } catch (error) {
    if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.response?.status === 504) {
      logger.error('Timeout ao buscar posições.');
      throw new Error('Timeout da API da Sitrax excedido.');
    }
    logger.error(
      `Falha ao buscar posições: ${error.message}`,
      error.response?.data
    );
    throw new Error(`Falha ao buscar posições da Sitrax: ${error.message}`);
  }
}