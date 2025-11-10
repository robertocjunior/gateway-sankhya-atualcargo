import { parseAtualcargoDate, parseSitraxDate } from '../utils/dateTime.js'; // CAMINHO CORRIGIDO
import logger from '../utils/logger.js'; // CAMINHO CORRIGIDO

/**
 * Mapeia os dados da Atualcargo para o formato padrão do hub.
 */
export function mapAtualcargoToStandard(positions) {
  const standardPositions = [];
  
  for (const pos of positions) {
    const date = parseAtualcargoDate(pos.date);
    if (!pos.plate || !date || !pos.latlong) {
      logger.warn(`[AtualcargoMapper] Registro ignorado (dados/data inválida): ${pos.plate}`);
      continue;
    }

    const isIsca = pos.plate.startsWith('ISCA');
    
    standardPositions.push({
      type: isIsca ? 'isca' : 'vehicle',
      identifier: isIsca ? pos.plate.replace('ISCA', '') : pos.plate,
      insertValue: pos.plate,
      date: date,
      lat: pos.latlong.latitude,
      lon: pos.latlong.longitude,
      speed: pos.speed,
      ignition: pos.ignition === 'ON' ? 'S' : 'N',
      location: pos.proximity || pos.address?.street || 'Localização não informada',
    });
  }
  
  logger.info(`[AtualcargoMapper] Mapeadas ${standardPositions.length} posições.`);
  return standardPositions;
}


/**
 * Mapeia os dados do Sitrax para o formato padrão do hub.
 */
export function mapSitraxToStandard(positions) {
  const standardPositions = [];

  for (const pos of positions) {
    // DATHOR é para o campo llpoDataStatus
    const date = parseSitraxDate(pos.llpoDataStatus);
    
    // Validação
    if (!pos.cveiPlaca || !date || pos.llpoLatitude === undefined || pos.llpoLongitude === undefined) {
      logger.warn(`[SitraxMapper] Registro ignorado (dados/data inválida): ${pos.cveiPlaca}`);
      continue;
    }
    
    // LOCAL deve ser populado com truaNome, + tmunNome + - + MG
    const location = `${pos.truaNome || ''}, ${pos.tmunNome || ''} - ${pos.testAbrev || ''}`;

    standardPositions.push({
      type: 'isca', // Sitrax só tem iscas
      identifier: pos.cveiPlaca.toString(), // cveiPlaca é o NUMISCA para busca
      insertValue: pos.cequSN.toString(), // cequSN é o valor para inserir
      date: date,
      lat: pos.llpoLatitude,
      lon: pos.llpoLongitude,
      speed: pos.llpoVelocidade,
      ignition: pos.llpoIgn === 'S' ? 'S' : 'N',
      location: location,
    });
  }
  
  logger.info(`[SitraxMapper] Mapeadas ${standardPositions.length} posições.`);
  return standardPositions;
}