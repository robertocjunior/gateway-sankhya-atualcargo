import axios from 'axios';
import { jobsConfig } from '../config/jobs.js'; // CAMINHO CORRIGIDO
import { appConfig } from '../config/app.js';
import logger from '../utils/logger.js';
import { AtualcargoTokenError } from '../utils/errors.js';

const config = jobsConfig.atualcargo;
const { timeout } = appConfig;

/**
 * Realiza login na API da Atualcargo.
 * @returns {Promise<string>} O token de acesso
 */
export async function loginAtualcargo() {
  logger.info('[Atualcargo] Tentando login...');
  try {
    const response = await axios.post(
      `${config.url}/api/auth/v1/login`,
      { username: config.username, password: config.password },
      {
        headers: {
          'access-key': config.apiKey,
          'Content-Type': 'application/json',
        },
      }
    );

    if (response.data?.token) {
      logger.info('[Atualcargo] Login bem-sucedido.');
      return response.data.token;
    }

    logger.error('[Atualcargo] Falha no login: Token não encontrado.', response.data);
    throw new Error('Token não retornado pela API Atualcargo');

  } catch (error) {
    logger.error(`[Atualcargo] Erro crítico ao fazer login: ${error.message}`);
    throw new Error(`Falha no login da Atualcargo: ${error.message}`);
  }
}

/**
 * Busca as últimas posições dos veículos na Atualcargo.
 * @param {string} token - O token Bearer
 * @returns {Promise<Array<Object>>} Uma lista de posições de veículos
 */
export async function getAtualcargoPositions(token) {
  logger.info('[Atualcargo] Buscando últimas posições (pode demorar até 2 min)...');
  try {
    const response = await axios.get(`${config.url}/api/positions/v1/last`, {
      headers: {
        'Authorization': `Bearer ${token}`,
        'access-key': config.apiKey,
      },
      timeout: timeout, 
    });

    if (response.data?.code === 200 && Array.isArray(response.data.data)) {
      logger.info(`[Atualcargo] Encontradas ${response.data.data.length} posições.`);
      return response.data.data; 
    }

    logger.warn('[Atualcargo] Resposta inesperada da API de posições:', response.data);
    return [];

  } catch (error) {
    if (error.code === 'ECONNABORTED' || error.code === 'ETIMEDOUT' || error.response?.status === 504) {
      logger.error('[Atualcargo] Timeout ao buscar posições. A API demorou mais que o esperado.');
      throw new Error('Timeout da API da Atualcargo excedido.');
    }
    if (error.response?.status === 401 || error.response?.status === 403) {
      logger.warn('[Atualcargo] Token expirou (401/403).');
      throw new AtualcargoTokenError('Token da Atualcargo expirado.');
    }
    if (error.response?.status === 425) {
      logger.warn('[Atualcargo] Erro 425 (Too Early / Rate Limit).');
      throw new Error('Falha da Atualcargo (Rate Limit 425).');
    }
    if (error.response?.status === 500) {
      logger.error('[Atualcargo] Erro 500 (Internal Server Error) na API.');
      throw new Error('Falha interna da API Atualcargo (500).');
    }
    logger.error(`[Atualcargo] Erro ao buscar posições: ${error.message}`);
    throw new Error(`Falha ao buscar posições da Atualcargo: ${error.message}`);
  }
}