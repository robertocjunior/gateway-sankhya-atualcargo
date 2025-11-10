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
  return isValid(date) ? date : null;
};

/**
 * Converte uma string de data do DB Sankhya (consulta) para um objeto Date.
 */
export const parseSankhyaQueryDate = (dateString) => {
  const date = parse(dateString, SANKHYA_QUERY_FORMAT, new Date());
  return isValid(date) ? date : null;
};

/**
 * NOVO: Converte uma string de data (dd/MM/yyyy) para um objeto Date.
 * Usado pelo Sitrax e pelo insert do Sankhya.
 */
export const parseDateTime = (dateString) => {
  const date = parse(dateString, DDMMYYYY_HHMMSS_FORMAT, new Date());
  return isValid(date) ? date : null;
}

/**
 * Formata um objeto Date para o padrão de inserção do Sankhya.
 */
export const formatForSankhyaInsert = (dateObj) => {
  return format(dateObj, DDMMYYYY_HHMMSS_FORMAT);
};

/**
 * Compara uma nova data (Date object) com a última data registrada (do Sankhya Query).
 * Retorna true se a nova data for mais recente.
 */
export const isNewer = (newDate, lastDateStr) => {
  if (!isValid(newDate)) {
    return false; // Data nova é inválida
  }
  
  // Se não houver data antiga, qualquer data nova válida é aceita
  if (!lastDateStr) {
    return true;
  }

  const lastDate = parseSankhyaQueryDate(lastDateStr);

  if (!isValid(lastDate)) {
    return true; // Data antiga é inválida, aceita a nova
  }

  return isAfter(newDate, lastDate);
};