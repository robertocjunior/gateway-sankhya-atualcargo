import axios from 'axios';
import config from '../config.js';
import { createLogger } from '../logger.js';
import { formatForSankhyaInsert, parseAtualcargoDate } from '../utils/dateTime.js';

const logger = createLogger('Sankhya');

// --- Gerenciamento de Sessão ---
let jsessionid = null;
let loginPromise = null; // Controla requisições de login simultâneas

const apiClient = axios.create({
  baseURL: config.sankhya.baseUrl,
  timeout: config.service.timeout,
});

/**
 * Realiza o login no Sankhya e armazena o jsessionid.
 * Garante que apenas UMA requisição de login ocorra por vez.
 */
async function login() {
  // Se já temos um ID e nenhum login está em andamento, podemos sair
  if (jsessionid && !loginPromise) {
    return;
  }

  // Se um login já está em andamento, aguarda ele terminar
  if (loginPromise) {
    logger.debug('Aguardando login em andamento...');
    return loginPromise;
  }

  // Inicia um novo login e armazena a Promise
  loginPromise = (async () => {
    logger.info('Autenticando no Sankhya (iniciando nova sessão)...');
    try {
      const loginBody = {
        serviceName: 'MobileLoginSP.login',
        requestBody: {
          NOMUSU: { $: config.sankhya.username },
          INTERNO: { $: config.sankhya.password },
          KEEPCONNECTED: { $: 'S' },
        },
      };

      const response = await apiClient.post(
        '/service.sbr?serviceName=MobileLoginSP.login&outputType=json',
        loginBody
      );

      const data = response.data;
      if (data.status !== '1' || !data.responseBody?.jsessionid?.$) {
        throw new Error(
          `Falha na autenticação Sankhya: ${data.statusMessage || 'Resposta inválida'}`
        );
      }

      jsessionid = data.responseBody.jsessionid.$;
      logger.info(`Autenticação no Sankhya bem-sucedida. JSessionID: ${jsessionid.substring(0, 10)}...`);
    } catch (error) {
      logger.error(
        `Falha ao autenticar no Sankhya: ${error.message}`,
        error.response?.data
      );
      jsessionid = null; // Limpa em caso de erro
      throw error; // Propaga o erro para quem chamou
    } finally {
      loginPromise = null; // Libera a trava, permitindo novos logins (ex: em caso de expiração)
    }
  })();

  return loginPromise;
}

/**
 * Wrapper genérico para requisições ao Sankhya, com re-autenticação.
 * @param {string} serviceName - Nome do serviço (ex: DbExplorerSP.executeQuery)
 * @param {object} body - Corpo da requisição
 * @returns {Promise<object>} responseBody
 */
async function makeRequest(serviceName, requestBody) {
  // Garante que estamos logados ANTES de fazer a requisição.
  // Esta chamada agora é segura contra "race conditions".
  await login();

  const url = `/service.sbr?serviceName=${serviceName}&outputType=json`;
  const body = { serviceName, requestBody };
  const headers = {
    Cookie: `JSESSIONID=${jsessionid}`,
  };

  try {
    const response = await apiClient.post(url, body, { headers });
    
    // Sucesso
    if (response.data.status === '1') {
      return response.data.responseBody;
    }
    
    // Erro de autenticação (sessão expirou)
    if (response.data.status === '3' && response.data.statusMessage?.includes('Não autorizado')) {
      logger.warn('[Sankhya] JSessionID expirado ou inválido. Reautenticando...');
      jsessionid = null; // Limpa o ID antigo
      await login(); // Faz um novo login (que é seguro)
      
      // Tenta novamente com o novo ID
      const newHeaders = { Cookie: `JSESSIONID=${jsessionid}` };
      const retryResponse = await apiClient.post(url, body, { headers: newHeaders });

      if (retryResponse.data.status === '1') {
        logger.debug(`Requisição ${serviceName} bem-sucedida após relogin.`);
        return retryResponse.data.responseBody;
      }
      
      throw new Error(
        `Falha na requisição Sankhya após re-autenticar: ${retryResponse.data.statusMessage}`
      );
    }
    
    // Outros erros
    throw new Error(
      `Erro na requisição Sankhya (${serviceName}): ${response.data.statusMessage || 'Erro desconhecido'}`
    );

  } catch (error) {
    logger.error(
      `Falha na chamada de serviço Sankhya (${serviceName}): ${error.message}`
    );
    throw error;
  }
}

/**
 * Formata linhas de consulta do Sankhya (array de arrays) para objetos.
 * @param {object} responseBody 
 * @returns {Array<object>}
 */
function formatQueryResponse(responseBody) {
  const fields = responseBody.fieldsMetadata.map((f) => f.name);
  const rows = responseBody.rows;
  
  return rows.map((row) => {
    const obj = {};
    fields.forEach((field, index) => {
      obj[field] = row[index];
    });
    return obj;
  });
}

// --- Funções de Consulta (DbExplorerSP.executeQuery) ---

/**
 * Busca CODVEICULO pela PLACA.
 * @param {Array<string>} plates - Lista de placas
 * @returns {Promise<Array<{CODVEICULO: number, PLACA: string}>>}
 */
export async function getVehiclesByPlate(plates) {
  if (plates.length === 0) return [];
  logger.debug(`Consultando CODVEICULO para ${plates.length} placas.`);
  
  const inClause = plates.map((p) => `'${p.trim()}'`).join(',');
  const sql = `SELECT VEI.CODVEICULO, VEI.PLACA FROM TGFVEI VEI WHERE VEI.PLACA IN (${inClause})`;

  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

/**
 * Busca SEQUENCIA pelo NUMISCA.
 * @param {Array<string>} iscaNumbers - Lista de números de isca (sem o "ISCA")
 * @returns {Promise<Array<{SEQUENCIA: number, NUMISCA: string}>>}
 */
export async function getIscasByNum(iscaNumbers) {
  if (iscaNumbers.length === 0) return [];
  logger.debug(`Consultando SEQUENCIA para ${iscaNumbers.length} iscas.`);

  const inClause = iscaNumbers.map((n) => `'${n.trim()}'`).join(',');
  const sql = `SELECT SCA.SEQUENCIA, SCA.NUMISCA FROM AD_CADISCA SCA WHERE SCA.NUMISCA IN (${inClause}) AND SCA.FABRICANTE = 2 AND SCA.ATIVO = 'S'`;

  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

/**
 * Busca o último registro de histórico para TODOS os veículos.
 * @returns {Promise<Array<{CODVEICULO: number, DATHOR: string, PLACA: string}>>}
 */
export async function getLastVehicleHistory() {
  logger.debug('Consultando último histórico de veículos (AD_LOCATCAR)...');
  const sql = "WITH UltimoRegistro AS (SELECT CODVEICULO, DATHOR, PLACA, ROW_NUMBER() OVER (PARTITION BY CODVEICULO ORDER BY NUMREG DESC) AS RN FROM AD_LOCATCAR) SELECT CODVEICULO, DATHOR, PLACA FROM UltimoRegistro WHERE RN = 1";
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

/**
 * Busca o último registro de histórico para TODAS as iscas.
 * @returns {Promise<Array<{SEQUENCIA: number, DATHOR: string, ISCA: string}>>}
 */
export async function getLastIscaHistory() {
  logger.debug('Consultando último histórico de iscas (AD_LOCATISC)...');
  const sql = "WITH UltimoRegistro AS (SELECT SEQUENCIA, DATHOR, ISCA, ROW_NUMBER() OVER (PARTITION BY SEQUENCIA ORDER BY NUMREG DESC) AS RN FROM AD_LOCATISC) SELECT SEQUENCIA, DATHOR, ISCA FROM UltimoRegistro WHERE RN = 1";
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

// --- Funções de Inserção (DatasetSP.save) ---

/**
 * Insere novos registros de histórico de veículos.
 * @param {Array<object>} records - Lista de registros formatados para inserção
 */
export async function insertVehicleHistory(records) {
  if (records.length === 0) {
    logger.debug('Nenhum registro novo para AD_LOCATCAR.');
    return;
  }
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATCAR...`);

  const formattedRecords = records.map(r => {
    const dateObj = parseAtualcargoDate(r.date);
    const dathorStr = formatForSankhyaInsert(dateObj);
    const link = `https://maps.google.com/?q=${r.latlong.latitude},${r.latlong.longitude}`;

    return {
      foreignKey: {
        CODVEICULO: r.codveiculo.toString(),
      },
      values: {
        "2": r.proximity || r.address?.street || 'Localização não informada', // LOCAL
        "3": dathorStr, // DATHOR
        "4": r.plate, // PLACA
        "5": r.latlong.latitude.toString(), // LATITUDE
        "6": r.latlong.longitude.toString(), // LONGITUDE
        "7": r.speed.toString(), // VELOC
        "8": link, // LINK
      },
    };
  });

  const requestBody = {
    dataSetID: "01S",
    entityName: "AD_LOCATCAR",
    standAlone: false,
    fields: [
      "NUMREG", "CODVEICULO", "LOCAL", "DATHOR", "PLACA", 
      "LATITUDE", "LONGITUDE", "VELOC", "LINK"
    ],
    ignoreListenerMethods: "",
    records: formattedRecords,
  };

  await makeRequest('DatasetSP.save', requestBody);
  logger.info(`Inserção em AD_LOCATCAR concluída.`);
}

/**
 * Insere novos registros de histórico de iscas.
 * @param {Array<object>} records - Lista de registros formatados para inserção
 */
export async function insertIscaHistory(records) {
  if (records.length === 0) {
    logger.debug('Nenhum registro novo para AD_LOCATISC.');
    return;
  }
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATISC...`);

  const formattedRecords = records.map(r => {
    const dateObj = parseAtualcargoDate(r.date);
    const dathorStr = formatForSankhyaInsert(dateObj);
    const link = `https://maps.google.com/?q=${r.latlong.latitude},${r.latlong.longitude}`;
    
    return {
      foreignKey: {
        SEQUENCIA: r.sequencia.toString(),
      },
      values: {
        "2": r.proximity || r.address?.street || 'Localização não informada', // LOCAL
        "3": dathorStr, // DATHOR
        "4": r.plate, // ISCA
        "5": r.latlong.latitude.toString(), // LATITUDE
        "6": r.latlong.longitude.toString(), // LONGITUDE
        "7": r.speed.toString(), // VELOC
        "8": link, // LINK
      },
    };
  });
  
  const requestBody = {
    dataSetID: "01S",
    entityName: "AD_LOCATISC",
    standAlone: false,
    fields: [
      "NUMREG", "SEQUENCIA", "LOCAL", "DATHOR", "ISCA", 
      "LATITUDE", "LONGITUDE", "VELOC", "LINK"
    ],
    ignoreListenerMethods: "",
    records: formattedRecords,
  };
  
  await makeRequest('DatasetSP.save', requestBody);
  logger.info(`Inserção em AD_LOCATISC concluída.`);
}