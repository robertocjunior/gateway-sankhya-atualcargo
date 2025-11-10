import { createLogger } from '../logger.js';
import * as sitraxApi from '../services/connectors/sitraxApi.js';
import { mapSitraxToSankhya } from '../sankhya/sankhyaMapper.js';
import { processarPosicoes } from '../sankhya/sankhyaProcessor.js';

const logger = createLogger('Job:Sitrax');
const JOB_NAME = 'Sitrax';

export async function run() {
  logger.info(`Iniciando job...`);
  let positions = [];

  try {
    positions = await sitraxApi.getLastPositions();
  } catch (error) {
    logger.error(`Falha ao extrair dados. Job abortado: ${error.message}`);
    return; // Aborta e espera o próximo intervalo
  }

  if (!positions || positions.length === 0) {
    logger.info('Nenhuma posição recebida. Job concluído.');
    return;
  }

  logger.info(`Extração concluída. ${positions.length} posições recebidas.`);

  // Mapeia para o formato padrão
  const standardData = mapSitraxToSankhya(positions);

  // Envia para o processador central do Sankhya
  await processarPosicoes(standardData, JOB_NAME);
  
  logger.info(`Job finalizado.`);
}