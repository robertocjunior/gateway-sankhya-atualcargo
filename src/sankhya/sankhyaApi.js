import axios from 'axios';
import { sankhyaConfig } from '../config/sankhya.js'; // Caminho atualizado
import { appConfig } from '../config/app.js'; // Caminho atualizado
import { createLogger } from '../logger.js';
import { parseAtualcargoDate, formatForSankhyaInsert } from '../utils/dateTime.js';
import { TextDecoder } from 'util';

const logger = createLogger('SankhyaAPI');

// --- Gerenciamento de Sessão ---
let jsessionid = null;
let loginPromise = null;

const apiClient = axios.create({
  baseURL: sankhyaConfig.baseUrl,
  timeout: appConfig.timeout, // Usando config geral
  responseType: 'arraybuffer', 
  transformResponse: [data => {
    try {
      const decoder = new TextDecoder('iso-8859-1');
      const decoded = decoder.decode(data);
      return JSON.parse(decoded);
    } catch (e) {
      logger.error('Falha ao decodificar ou parsear resposta do Sankhya.', e);
      return data;
    }
  }],
});

async function performLogin() {
  logger.info('Autenticando no Sankhya (iniciando nova sessão)...');
  try {
    const loginBody = {
      serviceName: 'MobileLoginSP.login',
      requestBody: {
        NOMUSU: { $: sankhyaConfig.username },
        INTERNO: { $: sankhyaConfig.password },
        KEEPCONNECTED: { $: 'S' },
      },
    };
    
    const response = await axios.post(
      '/service.sbr?serviceName=MobileLoginSP.login&outputType=json',
      loginBody,
      {
        baseURL: apiClient.defaults.baseURL,
        timeout: apiClient.defaults.timeout,
        responseType: 'json' 
      }
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
    jsessionid = null;
    throw error;
  } finally {
    loginPromise = null;
  }
}

async function login() {
  if (jsessionid && !loginPromise) {
    return;
  }
  if (loginPromise) {
    logger.debug('Aguardando login em andamento...');
    return loginPromise;
  }
  loginPromise = performLogin();
  return loginPromise;
}

async function makeRequest(serviceName, requestBody) {
  await login();

  const url = `/service.sbr?serviceName=${serviceName}&outputType=json`;
  const body = { serviceName, requestBody };
  const headers = {
    Cookie: `JSESSIONID=${jsessionid}`,
  };

  try {
    const response = await apiClient.post(url, body, { headers });
    
    if (response.data.status === '1') {
      return response.data.responseBody;
    }
    
    if (response.data.status === '3' && response.data.statusMessage === 'Não autorizado.') {
      logger.warn('[Sankhya] JSessionID expirado ou inválido (Não autorizado). Reautenticando...');
      jsessionid = null; 
      await login(); 
      
      const newHeaders = { Cookie: `JSESSIONID=${jsessionid}` };
      logger.debug(`Repetindo a requisição ${serviceName} com nova sessão...`);
      const retryResponse = await apiClient.post(url, body, { headers: newHeaders });

      if (retryResponse.data.status === '1') {
        logger.debug(`Requisição ${serviceName} bem-sucedida após relogin.`);
        return retryResponse.data.responseBody;
      }
      
      throw new Error(
        `Falha na requisição Sankhya (${serviceName}) após re-autenticar: ${retryResponse.data.statusMessage}`
      );
    }
    
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

function formatQueryResponse(responseBody) {
  const fields = responseBody.fieldsMetadata?.map((f) => f.name) || [];
  const rows = responseBody.rows || [];
  
  return rows.map((row) => {
    const obj = {};
    fields.forEach((field, index) => {
      obj[field] = row[index];
    });
    return obj;
  });
}

// --- Funções de Consulta (DbExplorerSP.executeQuery) ---

export async function getVehiclesByPlate(plates) {
  if (plates.length === 0) return [];
  logger.debug(`Consultando CODVEICULO para ${plates.length} placas.`);
  
  const inClause = plates.map((p) => `'${p.trim()}'`).join(',');
  const sql = `SELECT VEI.CODVEICULO, VEI.PLACA FROM TGFVEI VEI WHERE VEI.PLACA IN (${inClause})`;

  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

export async function getIscasByNum(iscaNumbers) {
  if (iscaNumbers.length === 0) return [];
  logger.debug(`Consultando SEQUENCIA para ${iscaNumbers.length} iscas.`);

  const inClause = iscaNumbers.map((n) => `'${n.trim()}'`).join(',');
  const sql = `SELECT SCA.SEQUENCIA, SCA.NUMISCA FROM AD_CADISCA SCA WHERE SCA.NUMISCA IN (${inClause}) AND SCA.ATIVO = 'S'`; // Removido o FABRICANTE = 2

  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

export async function getLastVehicleHistory() {
  logger.debug('Consultando último histórico de veículos (AD_LOCATCAR)...');
  const sql = "WITH UltimoRegistro AS (SELECT CODVEICULO, DATHOR, PLACA, ROW_NUMBER() OVER (PARTITION BY CODVEICULO ORDER BY NUMREG DESC) AS RN FROM AD_LOCATCAR) SELECT CODVEICULO, DATHOR, PLACA FROM UltimoRegistro WHERE RN = 1";
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

export async function getLastIscaHistory() {
  logger.debug('Consultando último histórico de iscas (AD_LOCATISC)...');
  const sql = "WITH UltimoRegistro AS (SELECT SEQUENCIA, DATHOR, ISCA, ROW_NUMBER() OVER (PARTITION BY SEQUENCIA ORDER BY NUMREG DESC) AS RN FROM AD_LOCATISC) SELECT SEQUENCIA, DATHOR, ISCA FROM UltimoRegistro WHERE RN = 1";
  
  const responseBody = await makeRequest('DbExplorerSP.executeQuery', { sql, params: {} });
  return formatQueryResponse(responseBody);
}

// --- Funções de Inserção (DatasetSP.save) ---

// O 'records' aqui agora espera o formato padronizado
export async function insertVehicleHistory(records) {
  if (records.length === 0) {
    logger.debug('Nenhum registro novo para AD_LOCATCAR.');
    return;
  }
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATCAR...`);

  const formattedRecords = records.map(r => {
    const dathorStr = formatForSankhyaInsert(r.date);
    const link = `https://maps.google.com/?q=$${r.lat},${r.lon}`;

    return {
      foreignKey: {
        CODVEICULO: r.codveiculo.toString(),
      },
      values: {
        "2": r.location, // LOCAL
        "3": dathorStr, // DATHOR
        "4": r.insertValue, // PLACA
        "5": r.lat.toString(), // LATITUDE
        "6": r.lon.toString(), // LONGITUDE
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

// O 'records' aqui agora espera o formato padronizado
export async function insertIscaHistory(records) {
  if (records.length === 0) {
    logger.debug('Nenhum registro novo para AD_LOCATISC.');
    return;
  }
  logger.info(`Inserindo ${records.length} novos registros em AD_LOCATISC...`);

  const formattedRecords = records.map(r => {
    const dathorStr = formatForSankhyaInsert(r.date);
    const link = `https://maps.google.com/?q=$${r.lat},${r.lon}`;
    
    return {
      foreignKey: {
        SEQUENCIA: r.sequencia.toString(),
      },
      values: {
        "2": r.location, // LOCAL
        "3": dathorStr, // DATHOR
        "4": r.insertValue, // ISCA (seja a placa ou o SN)
        "5": r.lat.toString(), // LATITUDE
        "6": r.lon.toString(), // LONGITUDE
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