import { parseAtualcargoDate, parseDateTime } from '../utils/dateTime.js';
import { createLogger } from '../logger.js';

const logger = createLogger('SankhyaMapper');

/**
 * Mapeia os dados da Atualcargo para o formato padrão do hub.
 * @param {Array<Object>} positions - Dados da API Atualcargo
 * @returns {{vehicles: Array<Object>, iscas: Array<Object>}}
 */
export function mapAtualcargoToSankhya(positions) {
  const vehicles = [];
  const iscas = [];

  for (const pos of positions) {
    // Validação básica (já feita no processador, mas bom ter aqui)
    const date = parseAtualcargoDate(pos.date);
    if (!pos.plate || !date || !pos.latlong) {
      logger.warn('[Atualcargo] Registro ignorado (dados/data inválida):', pos.plate);
      continue;
    }

    // Formato padrão
    const standardFormat = {
      date: date,
      lat: pos.latlong.latitude,
      lon: pos.latlong.longitude,
      speed: pos.speed,
      location: pos.proximity || pos.address?.street || 'Localização não informada',
    };

    if (pos.plate.startsWith('ISCA')) {
      iscas.push({
        ...standardFormat,
        identifier: pos.plate.replace('ISCA', ''), // O número '0189'
        insertValue: pos.plate, // O valor 'ISCA0189'
      });
    } else {
      vehicles.push({
        ...standardFormat,
        identifier: pos.plate, // A placa 'ABC1234'
        insertValue: pos.plate, // A placa 'ABC1234'
      });
    }
  }
  return { vehicles, iscas };
}


/**
 * Mapeia os dados do Sitrax para o formato padrão do hub.
 * @param {Array<Object>} positions - Dados da API Sitrax
 * @returns {{vehicles: Array<Object>, iscas: Array<Object>}}
 */
export function mapSitraxToSankhya(positions) {
  const iscas = [];
  // Este sistema só tem iscas, então 'vehicles' fica vazio
  const vehicles = []; 

  for (const pos of positions) {
    const date = parseDateTime(pos.llpoDataStatus); // Usa o novo parser
    
    // Validação
    if (!pos.cveiPlaca || !date || pos.llpoLatitude === undefined || pos.llpoLongitude === undefined) {
      logger.warn('[Sitrax] Registro ignorado (dados/data inválida):', pos.cveiPlaca);
      continue;
    }
    
    // Constrói o local conforme solicitado
    const location = `${pos.truaNome || ''}, ${pos.tmunNome || ''} - ${pos.testAbrev || ''}`;

    iscas.push({
      date: date,
      lat: pos.llpoLatitude,
      lon: pos.llpoLongitude,
      speed: pos.llpoVelocidade,
      location: location,
      identifier: pos.cveiPlaca.toString(), // "2010014393" (para buscar em AD_CADISCA.NUMISCA)
      insertValue: pos.cequSN.toString(), // "2010014393" (para inserir em AD_LOCATISC.ISCA)
    });
  }
  
  return { vehicles, iscas };
}