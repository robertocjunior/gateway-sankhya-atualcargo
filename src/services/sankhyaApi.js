import axios from 'axios';
import config from '../config.js';
import { createLogger } from '../logger.js';
import { formatForSankhyaInsert, parseAtualcargoDate } from '../utils/dateTime.js';

const logger = createLogger('Sankhya');

// Estado da Sessão
let jsessionid = null;

const apiClient = axios.create({
  baseURL: config.sankhya.baseUrl,
  timeout: config.service.timeout,
});

/**
 * Realiza o login no Sankhya e armazena o jsessionid.
 */
async function login() {
  logger.info('Autenticando no Sankhya...');
  try {
    const loginBody = {
      serviceName: 'MobileLoginSP.login', // [cite: 467]
      requestBody: {
        NOMUSU: { $: config.sankhya.username }, // [cite: 467]
        INTERNO: { $: config.sankhya.password }, // [cite: 467]
        KEEPCONNECTED: { $: 'S' }, // [cite: 467]
      },
    };

    const response = await apiClient.post(
      '/service.sbr?serviceName=MobileLoginSP.login&outputType=json', // [cite: 467]
      loginBody
    );

    const data = response.data;
    if (data.status !== '1' || !data.responseBody?.jsessionid?.$) {
      throw new Error(
        `Falha na autenticação Sankhya: ${data.statusMessage || 'Resposta inválida'}`
      );
    }

    jsessionid = data.responseBody.jsessionid.$; // [cite: 468]
    logger.info(`Autenticação no Sankhya bem-sucedida. JSessionID: ${jsessionid.substring(0, 10)}...`);
  } catch (error) {
    logger.error(
      `Falha ao autenticar no Sankhya: ${error.message}`,
      error.response?.data
    );
    throw error;
  }
}

/**
 * Wrapper genérico para requisições ao Sankhya, com re-autenticação.
 * @param {string} serviceName - Nome do serviço (ex: DbExplorerSP.executeQuery)
 * @param {object} body - Corpo da requisição
 * @returns {Promise<object>} responseBody
 */
async function makeRequest(serviceName, requestBody) {
  if (!jsessionid) {
    await login();
  }

  const url = `/service.sbr?serviceName=${serviceName}&outputType=json`;
  const body = { serviceName, requestBody };

  const headers = {
    Cookie: `JSESSIONID=${jsessionid}`, // [cite: 469]
  };

  try {
    const response = await apiClient.post(url, body, { headers });
    
    // Sucesso
    if (response.data.status === '1') {
      return response.data.responseBody;
    }
    
    // Erro de autenticação [cite: 469]
    if (response.data.status === '3' && response.data.statusMessage?.includes('Não autorizado')) {
      logger.warn('[Sankhya] JSessionID expirado. Reautenticando...');
      await login();
      
      // Tenta novamente com o novo ID
      const newHeaders = { Cookie: `JSESSIONID=${jsessionid}` };
      const retryResponse = await apiClient.post(url, body, { headers: newHeaders });

      if (retryResponse.data.status === '1') {
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
  const fields = responseBody.fieldsMetadata.map((f) => f.name); // [cite: 470, 471]
  const rows = responseBody.rows; // [cite: 472]
  
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
  const sql = `SELECT VEI.CODVEICULO, VEI.PLACA FROM TGFVEI VEI WHERE VEI.PLACA IN (${inClause})`; // [cite: 470]

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
  const sql = `SELECT SCA.SEQUENCIA, SCA.NUMISCA FROM AD_CADISCA SCA WHERE SCA.NUMISCA IN (${inClause}) AND SCA.FABRICANTE = 2 AND SCA.ATIVO = 'S'`; // [cite: 474]

  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

/**
 * Busca o último registro de histórico para TODOS os veículos.
 * @returns {Promise<Array<{CODVEICULO: number, DATHOR: string, PLACA: string}>>}
 */
export async function getLastVehicleHistory() {
  logger.debug('Consultando último histórico de veículos (AD_LOCATCAR)...');
  const sql = "WITH UltimoRegistro AS (SELECT CODVEICULO, DATHOR, PLACA, ROW_NUMBER() OVER (PARTITION BY CODVEICULO ORDER BY NUMREG DESC) AS RN FROM AD_LOCATCAR) SELECT CODVEICULO, DATHOR, PLACA FROM UltimoRegistro WHERE RN = 1"; // [cite: 478]
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody); // [cite: 479, 480, 481, 482, 483]
}

/**
 * Busca o último registro de histórico para TODAS as iscas.
 * @returns {Promise<Array<{SEQUENCIA: number, DATHOR: string, ISCA: string}>>}
 */
export async function getLastIscaHistory() {
  logger.debug('Consultando último histórico de iscas (AD_LOCATISC)...');
  const sql = "WITH UltimoRegistro AS (SELECT SEQUENCIA, DATHOR, ISCA, ROW_NUMBER() OVER (PARTITION BY SEQUENCIA ORDER BY NUMREG DESC) AS RN FROM AD_LOCATISC) SELECT SEQUENCIA, DATHOR, ISCA FROM UltimoRegistro WHERE RN = 1"; // [cite: 607]
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody); // [cite: 608, 609, 610, 611]
}

// --- Funções de Inserção (DatasetSP.save) ---

/**
 * Insere novos registros de histórico de veículos.
 * @param {Array<object>} records - Lista de registros formatados para inserção
 */
export async function insertVehicleHistory(records) {
  if (records.length === 0) return;
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATCAR...`);

  const formattedRecords = records.map(r => {
    const dateObj = parseAtualcargoDate(r.date);
    const dathorStr = formatForSankhyaInsert(dateObj);
    const link = `https://maps.google.com/?q=${r.latlong.latitude},${r.latlong.longitude}`;

    return {
      foreignKey: {
        CODVEICULO: r.codveiculo.toString(), // [cite: 614]
      },
      values: {
        "2": r.proximity || r.address.street, // LOCAL [cite: 614]
        "3": dathorStr, // DATHOR [cite: 614]
        "4": r.plate, // PLACA [cite: 614]
        "5": r.latlong.latitude.toString(), // LATITUDE [cite: 614]
        "6": r.latlong.longitude.toString(), // LONGITUDE [cite: 615]
        "7": r.speed.toString(), // VELOC [cite: 615]
        "8": link, // LINK [cite: 615]
      },
    };
  });

  const requestBody = {
    dataSetID: "01S", // [cite: 613]
    entityName: "AD_LOCATCAR", // [cite: 613]
    standAlone: false,
    fields: [
      "NUMREG", "CODVEICULO", "LOCAL", "DATHOR", "PLACA", 
      "LATITUDE", "LONGITUDE", "VELOC", "LINK"
    ], // [cite: 613]
    ignoreListenerMethods: "",
    records: formattedRecords, // [cite: 613]
  };

  await makeRequest('DatasetSP.save', requestBody);
  logger.info(`Inserção em AD_LOCATCAR concluída.`);
}

/**
 * Insere novos registros de histórico de iscas.
 * @param {Array<object>} records - Lista de registros formatados para inserção
 */
export async function insertIscaHistory(records) {
  if (records.length === 0) return;
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATISC...`);

  const formattedRecords = records.map(r => {
    const dateObj = parseAtualcargoDate(r.date);
    const dathorStr = formatForSankhyaInsert(dateObj);
    const link = `https://maps.google.com/?q=${r.latlong.latitude},${r.latlong.longitude}`;
    
    return {
      foreignKey: {
        SEQUENCIA: r.sequencia.toString(), // [cite: 618]
      },
      values: {
        "2": r.proximity || r.address.street, // LOCAL [cite: 618]
        "3": dathorStr, // DATHOR [cite: 618]
        "4": r.plate, // ISCA [cite: 618]
        "5": r.latlong.latitude.toString(), // LATITUDE [cite: 618]
        "6": r.latlong.longitude.toString(), // LONGITUDE [cite: 619]
        "7": r.speed.toString(), // VELOC [cite: 619]
        "8": link, // LINK [cite: 619]
      },
    };
  });
  
  const requestBody = {
    dataSetID: "01S", // [cite: 617]
    entityName: "AD_LOCATISC", // [cite: 617]
    standAlone: false,
    fields: [
      "NUMREG", "SEQUENCIA", "LOCAL", "DATHOR", "ISCA", 
      "LATITUDE", "LONGITUDE", "VELOC", "LINK"
    ], // [cite: 617]
    ignoreListenerMethods: "",
    records: formattedRecords, // [cite: 617]
  };
  
  await makeRequest('DatasetSP.save', requestBody);
  logger.info(`Inserção em AD_LOCATISC concluída.`);
}