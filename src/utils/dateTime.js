import logger from './logger.js';
import { parse, isAfter, format, isValid } from 'date-fns';

// Formato Atualcargo: 2025-11-07 15:38:12
const ATUALCARGO_FORMAT = 'yyyy-MM-dd HH:mm:ss';

// Formato Sankhya (consulta): 07112025 13:58:42
const SANKHYA_QUERY_FORMAT = 'ddMMyyyy HH:mm:ss';

// Formato Sankhya (insert) e Sitrax (data): 03/11/2025 08:38:00
const DDMMYYYY_HHMMSS_FORMAT = 'dd/MM/yyyy HH:mm:ss';

/**
 * Converte uma string de data da Atualcargo para um objeto Date.
 */
export const parseAtualcargoDate = (dateString) => {
  const date = parse(dateString, ATUALCARGO_FORMAT, new Date());
  if (!isValid(date)) {
    logger.warn(`Data (Atualcargo) inválida: ${dateString}.`);
    return null;
  }
  return date;
};

/**
 * Converte uma string de data do DB Sankhya (consulta) para um objeto Date.
 */
export const parseSankhyaQueryDate = (dateString) => {
  const date = parse(dateString, SANKHYA_QUERY_FORMAT, new Date());
  if (!isValid(date)) {
    logger.warn(`Data (Sankhya Query) inválida: ${dateString}.`);
    return null;
  }
  return date;
};

/**
 * Converte uma string de data (dd/MM/yyyy) para um objeto Date.
 * Usado pelo Sitrax e pelo insert do Sankhya.
 */
export const parseSitraxDate = (dateString) => {
  const date = parse(dateString, DDMMYYYY_HHMMSS_FORMAT, new Date());
  if (!isValid(date)) {
    logger.warn(`Data (Sitrax) inválida: ${dateString}.`);
    return null;
  }
  return date;
}

/**
 * Formata um objeto Date para o padrão de inserção do Sankhya (DD/MM/YYYY HH:mm:ss).
 */
export const formatForSankhyaInsert = (dateObj) => {
  if (!dateObj || !isValid(dateObj)) {
     logger.warn(`Data (formatSankhyaInsert) inválida: ${dateObj}.`);
     return null;
  }
  return format(dateObj, DDMMYYYY_HHMMSS_FORMAT);
};

/**
 * Compara uma nova data (Date object) com a última data registrada (do Sankhya Query).
 * Retorna true se a nova data for mais recente.
 */
export const isNewer = (newDate, lastDateStr) => {
  if (!newDate || !isValid(newDate)) {
    return false; // Data nova é inválida
  }
  
  if (!lastDateStr) {
    return true; // Não há data antiga, aceita a nova
  }

  const lastDate = parseSankhyaQueryDate(lastDateStr);

  if (!lastDate || !isValid(lastDate)) {
    return true; // Data antiga é inválida, aceita a nova
  }

  return newDate.getTime() > lastDate.getTime();
};

/**
 * Cria uma pausa assíncrona
 * @param {number} ms - Tempo em milissegundos
 */
export const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));