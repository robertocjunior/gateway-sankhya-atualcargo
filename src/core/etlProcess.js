import { createLogger } from '../logger.js';
import * as atualcargoApi from '../services/atualcargoApi.js';
import * as sankhyaApi from '../services/sankhyaApi.js';
import { isNewer, parseAtualcargoDate } from '../utils/dateTime.js';

const logger = createLogger('ETL-Process');

/**
 * Executa um ciclo completo do processo de ETL.
 */
export async function runEtlProcess() {
  logger.info('Iniciando ciclo de integração...');
  let positions = [];

  // 1. EXTRACT
  logger.info('ETL [1/5] Extract: Buscando dados da API Atualcargo...');
  try {
    positions = await atualcargoApi.getLastPositions();
  } catch (error) {
    logger.error(`Falha ao extrair dados da Atualcargo. Ciclo abortado: ${error.message}`);
    return; // Aborta o ciclo atual em caso de falha
  }

  if (!positions || positions.length === 0) {
    logger.info('Nenhuma posição recebida da Atualcargo. Ciclo concluído.');
    return;
  }
  logger.info(`ETL [1/5] Extract: ${positions.length} posições recebidas.`);

  // 2. TRANSFORM (Part 1: Separate)
  logger.info('ETL [2/5] Transform: Validando e separando Veículos de Iscas...');
  const vehiclePositions = [];
  const iscaPositions = [];

  for (const pos of positions) {
    // Validação básica do registro
    if (!pos.plate || !pos.date || !pos.latlong) {
      logger.warn('Registro da Atualcargo ignorado (dados incompletos):', pos);
      continue;
    }
    
    // Tenta parsear a data. Se for inválida, ignora o registro.
    if (!parseAtualcargoDate(pos.date)) {
      logger.warn(`Registro ignorado (data inválida: ${pos.date}):`, pos.plate);
      continue;
    }

    if (pos.plate.startsWith('ISCA')) { //
      iscaPositions.push(pos);
    } else {
      vehiclePositions.push(pos);
    }
  }
  logger.info(`ETL [2/5] Transform: ${vehiclePositions.length} posições de veículos, ${iscaPositions.length} posições de iscas.`);


  // 3. TRANSFORM (Part 2: Get Sankhya Data)
  logger.info('ETL [3/5] Transform: Buscando dados de mapeamento e históricos do Sankhya...');
  const vehiclePlates = [...new Set(vehiclePositions.map(p => p.plate))];
  const iscaPlates = [...new Set(iscaPositions.map(p => p.plate))];
  
  // ***** LINHA CORRIGIDA *****
  // O 'p' aqui já é a string da placa, não o objeto 'pos'
  const iscaNumbers = iscaPlates.map(p => p.replace('ISCA', '')); //

  let vehicleMap, iscaMap, lastVehicleHistory, lastIscaHistory;

  try {
    // Executa consultas em paralelo
    logger.debug('Buscando mapeamentos de veículos e iscas...');
    logger.debug('Buscando últimos históricos de veículos e iscas...');
    const [
      vehicleMappingResult,
      iscaMappingResult,
      vehicleHistoryResult,
      iscaHistoryResult
    ] = await Promise.all([
      sankhyaApi.getVehiclesByPlate(vehiclePlates),
      sankhyaApi.getIscasByNum(iscaNumbers),
      sankhyaApi.getLastVehicleHistory(),
      sankhyaApi.getLastIscaHistory()
    ]);

    // Cria os Maps para facilitar a consulta
    vehicleMap = new Map(vehicleMappingResult.map(v => [v.PLACA, v.CODVEICULO]));
    iscaMap = new Map(iscaMappingResult.map(i => [i.NUMISCA, i.SEQUENCIA]));
    lastVehicleHistory = new Map(vehicleHistoryResult.map(h => [h.CODVEICULO, h.DATHOR]));
    lastIscaHistory = new Map(iscaHistoryResult.map(h => [h.SEQUENCIA, h.DATHOR]));
    
    logger.info(`ETL [3/5] Transform: ${vehicleMap.size} veículos e ${iscaMap.size} iscas mapeados.`);
    logger.info(`ETL [3/5] Transform: ${lastVehicleHistory.size} históricos de veículos e ${lastIscaHistory.size} de iscas carregados.`);

  } catch (error) {
    logger.error(`Falha ao buscar dados do Sankhya. Ciclo abortado: ${error.message}`);
    return;
  }

  // 4. TRANSFORM (Part 3: Filter)
  logger.info('ETL [4/5] Transform: Filtrando registros novos...');
  const newVehicleRecords = [];
  for (const pos of vehiclePositions) {
    const codveiculo = vehicleMap.get(pos.plate);
    if (!codveiculo) {
      logger.debug(`Placa ${pos.plate} ignorada (não cadastrada no Sankhya).`);
      continue;
    }
    
    const lastDathor = lastVehicleHistory.get(codveiculo); //
    if (isNewer(pos.date, lastDathor)) { //
      newVehicleRecords.push({ ...pos, codveiculo });
    }
  }

  const newIscaRecords = [];
  for (const pos of iscaPositions) {
    const iscaNum = pos.plate.replace('ISCA', '');
    const sequencia = iscaMap.get(iscaNum);
    if (!sequencia) {
      logger.debug(`Isca ${pos.plate} (Nº ${iscaNum}) ignorada (não cadastrada no Sankhya).`);
      continue;
    }

    const lastDathor = lastIscaHistory.get(sequencia); //
    if (isNewer(pos.date, lastDathor)) { //
      newIscaRecords.push({ ...pos, sequencia });
    }
  }
  logger.info(`ETL [4/5] Transform: ${newVehicleRecords.length} novos veículos e ${newIscaRecords.length} novas iscas para inserir.`);

  // 5. LOAD
  logger.info('ETL [5/5] Load: Iniciando inserção de dados no Sankhya...');
  try {
    // Executa inserções em paralelo
    await Promise.all([
      sankhyaApi.insertVehicleHistory(newVehicleRecords),
      sankhyaApi.insertIscaHistory(newIscaRecords)
    ]);

    logger.info(
      `[ETL Concluído] ${newVehicleRecords.length} registros de veículos e ${newIscaRecords.length} de iscas inseridos com sucesso.`
    );
  } catch (error) {
    logger.error(`Falha na etapa de carregamento (LOAD) no Sankhya: ${error.message}`);
  }
}