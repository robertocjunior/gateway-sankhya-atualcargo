import { createLogger } from '../logger.js';
import * as atualcargoApi from '../services/connectors/atualcargoApi.js';
import { mapAtualcargoToSankhya } from '../sankhya/sankhyaMapper.js';
import { processarPosicoes } from '../sankhya/sankhyaProcessor.js';

const logger = createLogger('Job:Atualcargo');
const JOB_NAME = 'Atualcargo';

export async function run() {
  logger.info(`Iniciando job...`);
  let positions = [];

  try {
    positions = await atualcargoApi.getLastPositions();
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
  const standardData = mapAtualcargoToSankhya(positions);
  
  // Envia para o processador central do Sankhya
  // A lógica de retentativa do Sankhya está DENTRO do processador
  await processarPosicoes(standardData, JOB_NAME);
  
  logger.info(`Job finalizado.`);
}