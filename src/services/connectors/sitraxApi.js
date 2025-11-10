import axios from 'axios';
import { jobsConfig } from '../../config/jobs.js';
import { appConfig } from '../../config/app.js';
import { createLogger } from '../../logger.js';

const logger = createLogger('SitraxAPI');
const config = jobsConfig.sitrax.api;

const apiClient = axios.create({
  baseURL: config.baseUrl,
  timeout: appConfig.timeout,
  headers: {
    'Content-Type': 'application/json',
  },
});

/**
 * Busca as últimas posições do Sitrax.
 * @returns {Promise<Array<Object>>} Lista de posições
 */
export async function getLastPositions() {
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
    logger.error(
      `Falha ao buscar posições: ${error.message}`,
      error.response?.data
    );
    throw error;
  }
}