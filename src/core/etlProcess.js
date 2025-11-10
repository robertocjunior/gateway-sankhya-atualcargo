import { createLogger } from '../logger.js';
import * as atualcargoApi from '../services/atualcargoApi.js';
import * as sankhyaApi from '../services/sankhyaApi.js';
import { isNewer, parseAtualcargoDate } from '../utils/dateTime.js';
import config from '../config.js'; // Importar config

const logger = createLogger('ETL-Process');

// Pega as configurações de retentativa
const { sankhyaRetryLimit, sankhyaRetryDelay } = config.service;

// Função helper para criar uma pausa (delay)
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Etapa 2: Processa os dados no Sankhya, com lógica de retentativa.
 * (Antigos passos 2-5)
 * @param {Array<Object>} positions - Os dados brutos da Atualcargo
 */
async function processSankhyaData(positions) {
  // Inicia um loop de retentativas
  for (let attempt = 1; attempt <= sankhyaRetryLimit; attempt++) {
    try {
      logger.info(
        `Iniciando processamento Sankhya (Tentativa ${attempt}/${sankhyaRetryLimit})...`
      );

      // 2. TRANSFORM (Part 1: Separate)
      logger.info('ETL [2/5] Transform: Validando e separando Veículos de Iscas...');
      const vehiclePositions = [];
      const iscaPositions = [];

      for (const pos of positions) {
        if (!pos.plate || !pos.date || !pos.latlong) {
          logger.warn('Registro da Atualcargo ignorado (dados incompletos):', pos);
          continue;
        }
        if (!parseAtualcargoDate(pos.date)) {
          logger.warn(`Registro ignorado (data inválida: ${pos.date}):`, pos.plate);
          continue;
        }
        if (pos.plate.startsWith('ISCA')) {
          iscaPositions.push(pos);
        } else {
          vehiclePositions.push(pos);
        }
      }
      logger.info(
        `ETL [2/5] Transform: ${vehiclePositions.length} posições de veículos, ${iscaPositions.length} posições de iscas.`
      );

      // 3. TRANSFORM (Part 2: Get Sankhya Data)
      logger.info(
        'ETL [3/5] Transform: Buscando dados de mapeamento e históricos do Sankhya...'
      );
      const vehiclePlates = [...new Set(vehiclePositions.map((p) => p.plate))];
      const iscaPlates = [...new Set(iscaPositions.map((p) => p.plate))];
      const iscaNumbers = iscaPlates.map((p) => p.replace('ISCA', ''));

      const [
        vehicleMappingResult,
        iscaMappingResult,
        vehicleHistoryResult,
        iscaHistoryResult,
      ] = await Promise.all([
        sankhyaApi.getVehiclesByPlate(vehiclePlates),
        sankhyaApi.getIscasByNum(iscaNumbers),
        sankhyaApi.getLastVehicleHistory(),
        sankhyaApi.getLastIscaHistory(),
      ]);

      const vehicleMap = new Map(
        vehicleMappingResult.map((v) => [v.PLACA, v.CODVEICULO])
      );
      const iscaMap = new Map(
        iscaMappingResult.map((i) => [i.NUMISCA, i.SEQUENCIA])
      );
      const lastVehicleHistory = new Map(
        vehicleHistoryResult.map((h) => [h.CODVEICULO, h.DATHOR])
      );
      const lastIscaHistory = new Map(
        iscaHistoryResult.map((h) => [h.SEQUENCIA, h.DATHOR])
      );

      logger.info(
        `ETL [3/5] Transform: ${vehicleMap.size} veículos e ${iscaMap.size} iscas mapeados.`
      );
      logger.info(
        `ETL [3/5] Transform: ${lastVehicleHistory.size} históricos de veículos e ${lastIscaHistory.size} de iscas carregados.`
      );

      // 4. TRANSFORM (Part 3: Filter)
      logger.info('ETL [4/5] Transform: Filtrando registros novos...');
      const newVehicleRecords = [];
      for (const pos of vehiclePositions) {
        const codveiculo = vehicleMap.get(pos.plate);
        if (!codveiculo) {
          logger.debug(`Placa ${pos.plate} ignorada (não cadastrada no Sankhya).`);
          continue;
        }
        const lastDathor = lastVehicleHistory.get(codveiculo);
        if (isNewer(pos.date, lastDathor)) {
          newVehicleRecords.push({ ...pos, codveiculo });
        }
      }

      const newIscaRecords = [];
      for (const pos of iscaPositions) {
        const iscaNum = pos.plate.replace('ISCA', '');
        const sequencia = iscaMap.get(iscaNum);
        if (!sequencia) {
          logger.debug(
            `Isca ${pos.plate} (Nº ${iscaNum}) ignorada (não cadastrada no Sankhya).`
          );
          continue;
        }
        const lastDathor = lastIscaHistory.get(sequencia);
        if (isNewer(pos.date, lastDathor)) {
          newIscaRecords.push({ ...pos, sequencia });
        }
      }
      logger.info(
        `ETL [4/5] Transform: ${newVehicleRecords.length} novos veículos e ${newIscaRecords.length} novas iscas para inserir.`
      );

      // 5. LOAD
      logger.info('ETL [5/5] Load: Iniciando inserção de dados no Sankhya...');
      await Promise.all([
        sankhyaApi.insertVehicleHistory(newVehicleRecords),
        sankhyaApi.insertIscaHistory(newIscaRecords),
      ]);

      logger.info(
        `[ETL Concluído] ${newVehicleRecords.length} registros de veículos e ${newIscaRecords.length} de iscas inseridos com sucesso.`
      );

      // Sai do loop de retentativa.
      return;
    } catch (error) {
      logger.error(
        `Falha na tentativa ${attempt} de processar dados no Sankhya: ${error.message}`
      );

      if (attempt < sankhyaRetryLimit) {
        const delayMinutes = sankhyaRetryDelay / 60000;
        logger.warn(
          `Aguardando ${delayMinutes} minuto(s) para a próxima tentativa...`
        );
        await wait(sankhyaRetryDelay); // Espera antes de tentar de novo
      } else {
        logger.error(
          `[ETL Falhou] Limite de ${sankhyaRetryLimit} retentativas para o Sankhya atingido. Descartando dados deste ciclo.`
        );
      }
    }
  }
}

/**
 * Etapa 1: Extrai dados da Atualcargo.
 * Ponto de entrada principal do ciclo.
 */
export async function runEtlProcess() {
  logger.info('Iniciando ciclo de integração...');
  let positions = [];

  // 1. EXTRACT
  logger.info('ETL [1/5] Extract: Buscando dados da API Atualcargo...');
  try {
    positions = await atualcargoApi.getLastPositions();
  } catch (error) {
    logger.error(
      `Falha ao extrair dados da Atualcargo. Ciclo abortado: ${error.message}`
    );
    return; // Aborta e espera o mainLoop (5 min)
  }

  if (!positions || positions.length === 0) {
    logger.info('Nenhuma posição recebida da Atualcargo. Ciclo concluído.');
    return;
  }
  logger.info(`ETL [1/5] Extract: ${positions.length} posições recebidas.`);

  // CHAMA A ETAPA DE PROCESSAMENTO
  // O mainLoop em index.js agora vai esperar esta função (com suas retentativas)
  // terminar antes de agendar o próximo ciclo de 5 minutos.
  await processSankhyaData(positions);
}
