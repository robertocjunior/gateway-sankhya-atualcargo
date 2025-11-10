import { createLogger } from '../logger.js';
import * as sankhyaApi from './sankhyaApi.js';
import { isNewer } from '../utils/dateTime.js';
import { appConfig } from '../config/app.js';

const logger = createLogger('SankhyaProcessor');

const { sankhyaRetryLimit, sankhyaRetryDelay } = appConfig;

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Processa um lote de dados de veículos e iscas no Sankhya.
 * @param {{vehicles: Array<Object>, iscas: Array<Object>}} data - Dados padronizados
 * @param {string} sourceName - Nome da fonte (ex: 'Atualcargo')
 */
export async function processarPosicoes(data, sourceName) {
  const { vehicles, iscas } = data;
  if (vehicles.length === 0 && iscas.length === 0) {
    logger.info(`[${sourceName}] Nenhum dado válido para processar.`);
    return;
  }

  logger.info(
    `[${sourceName}] Processando ${vehicles.length} veículos e ${iscas.length} iscas no Sankhya.`
  );

  for (let attempt = 1; attempt <= sankhyaRetryLimit; attempt++) {
    try {
      // 1. Obter dados de mapeamento e históricos do Sankhya
      logger.info(
        `[${sourceName}] Buscando dados do Sankhya (Tentativa ${attempt}/${sankhyaRetryLimit})...`
      );
      
      const vehiclePlates = vehicles.map((v) => v.identifier);
      const iscaNumbers = iscas.map((i) => i.identifier);

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
        `[${sourceName}] ${vehicleMap.size} veículos e ${iscaMap.size} iscas mapeados.`
      );

      // 2. Filtrar registros novos
      logger.info(`[${sourceName}] Filtrando registros novos...`);
      const newVehicleRecords = [];
      for (const vehicle of vehicles) {
        const codveiculo = vehicleMap.get(vehicle.identifier);
        if (!codveiculo) {
          logger.debug(
            `[${sourceName}] Veículo ${vehicle.identifier} ignorado (não cadastrado no Sankhya).`
          );
          continue;
        }
        const lastDathor = lastVehicleHistory.get(codveiculo);
        if (isNewer(vehicle.date, lastDathor)) {
          newVehicleRecords.push({ ...vehicle, codveiculo });
        }
      }

      const newIscaRecords = [];
      for (const isca of iscas) {
        const sequencia = iscaMap.get(isca.identifier);
        if (!sequencia) {
          logger.debug(
            `[${sourceName}] Isca ${isca.identifier} ignorada (não cadastrada no Sankhya).`
          );
          continue;
        }
        const lastDathor = lastIscaHistory.get(sequencia);
        if (isNewer(isca.date, lastDathor)) {
          newIscaRecords.push({ ...isca, sequencia });
        }
      }
      logger.info(
        `[${sourceName}] ${newVehicleRecords.length} novos veículos e ${newIscaRecords.length} novas iscas para inserir.`
      );

      // 3. Inserir no Sankhya
      logger.info(`[${sourceName}] Iniciando inserção de dados no Sankhya...`);
      await Promise.all([
        sankhyaApi.insertVehicleHistory(newVehicleRecords),
        sankhyaApi.insertIscaHistory(newIscaRecords),
      ]);

      logger.info(
        `[${sourceName}] Processamento Sankhya concluído com sucesso.`
      );

      // Sucesso! Sair do loop de retentativa.
      return;

    } catch (error) {
      logger.error(
        `[${sourceName}] Falha na tentativa ${attempt} de processar dados no Sankhya: ${error.message}`
      );

      if (attempt < sankhyaRetryLimit) {
        const delayMinutes = sankhyaRetryDelay / 60000;
        logger.warn(
          `[${sourceName}] Aguardando ${delayMinutes} minuto(s) para a próxima tentativa...`
        );
        await wait(sankhyaRetryDelay);
      } else {
        logger.error(
          `[${sourceName}] Limite de ${sankhyaRetryLimit} retentativas para o Sankhya atingido. Descartando dados.`
        );
      }
    }
  }
}