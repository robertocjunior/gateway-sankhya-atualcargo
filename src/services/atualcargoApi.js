import axios from 'axios';
import config from '../config.js';
import { createLogger } from '../logger.js';

const logger = createLogger('Atualcargo');

// Estado do Token
let token = null;
let tokenExpiry = null;

const apiClient = axios.create({
  baseURL: config.atualcargo.baseUrl,
  timeout: config.service.timeout,
  headers: {
    'Content-Type': 'application/json',
    'access-key': config.atualcargo.accessKey,
  },
});

/**
 * Realiza o login na API Atualcargo e armazena o token.
 */
async function login() {
  logger.info('Autenticando na Atualcargo...');
  try {
    const loginBody = {
      username: config.atualcargo.username,
      password: config.atualcargo.password,
    };

    const response = await apiClient.post('/api/auth/v1/login', loginBody);
    const data = response.data;

    if (!data.token) {
      throw new Error('Token não recebido da API Atualcargo');
    }

    token = data.token;
    // Define a expiração para 4.5 minutos para ter margem de segurança
    const expiresInMs = (5 * 60 * 1000) * 0.9; // 4.5 minutos
    tokenExpiry = Date.now() + expiresInMs;

    logger.info('Autenticação na Atualcargo bem-sucedida.');
  } catch (error) {
    logger.error(
      `Falha ao autenticar na Atualcargo: ${error.message}`,
      error.response?.data
    );
    throw error;
  }
}

/**
 * Obtém um token válido, renovando se estiver expirado.
 * @returns {Promise<string>} O token Bearer
 */
async function getValidToken() {
  if (!token || Date.now() >= tokenExpiry) {
    logger.warn('Token da Atualcargo expirado ou ausente. Renovando...');
    await login();
  }
  return token;
}

/**
 * Busca as últimas posições dos veículos.
 * @returns {Promise<Array<Object>>} Lista de posições
 */
export async function getLastPositions() {
  try {
    // 1. Garante que o token é válido ANTES de logar a ação
    const authToken = await getValidToken();

    // 2. AGORA sim, loga a ação de busca
    logger.info('Buscando últimas posições na Atualcargo...');

    const response = await apiClient.get('/api/positions/v1/last', {
      headers: {
        Authorization: `Bearer ${authToken}`,
      },
    });

    if (response.data && response.data.code === 200 && Array.isArray(response.data.data)) {
      logger.info(
        `Recebidas ${response.data.data.length} posições da Atualcargo.`
      );
      return response.data.data;
    }
    
    logger.warn('Resposta da Atualcargo não contém dados válidos.', response.data);
    return [];

  } catch (error) {
    logger.error(
      `Falha ao buscar posições na Atualcargo: ${error.message}`,
      error.response?.data
    );
    throw error;
  }
}
