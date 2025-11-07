import { parse, isAfter, format, isValid } from 'date-fns';

// Formato da API Atualcargo: 2025-11-07 15:38:12 [cite: 2]
const ATUALCARGO_FORMAT = 'yyyy-MM-dd HH:mm:ss';

// Formato do banco de dados Sankhya (consulta): 07112025 13:58:42 [cite: 482]
const SANKHYA_DB_FORMAT = 'ddMMyyyy HH:mm:ss';

// Formato de inserção no Sankhya (DatasetSP.save): 03/11/2025 08:38:00 [cite: 614]
const SANKHYA_INSERT_FORMAT = 'dd/MM/yyyy HH:mm:ss';

/**
 * Converte uma string de data da Atualcargo para um objeto Date.
 * @param {string} dateString
 * @returns {Date | null}
 */
export const parseAtualcargoDate = (dateString) => {
  const date = parse(dateString, ATUALCARGO_FORMAT, new Date());
  return isValid(date) ? date : null;
};

/**
 * Converte uma string de data do DB Sankhya para um objeto Date.
 * @param {string} dateString
 * @returns {Date | null}
 */
export const parseSankhyaDate = (dateString) => {
  const date = parse(dateString, SANKHYA_DB_FORMAT, new Date());
  return isValid(date) ? date : null;
};

/**
 * Formata um objeto Date para o padrão de inserção do Sankhya.
 * @param {Date} dateObj
 * @returns {string}
 */
export const formatForSankhyaInsert = (dateObj) => {
  return format(dateObj, SANKHYA_INSERT_FORMAT);
};

/**
 * Compara uma nova data (da Atualcargo) com a última data registrada (do Sankhya).
 * Retorna true se a nova data for mais recente.
 * @param {string} newDateStr (Formato Atualcargo)
 * @param {string} lastDateStr (Formato Sankhya DB)
 * @returns {boolean}
 */
export const isNewer = (newDateStr, lastDateStr) => {
  const newDate = parseAtualcargoDate(newDateStr);

  // Se não houver data antiga, qualquer data nova é válida
  if (!lastDateStr) {
    return isValid(newDate);
  }

  const lastDate = parseSankhyaDate(lastDateStr);

  if (!isValid(newDate) || !isValid(lastDate)) {
    return false; // Não arrisca inserir se alguma data for inválida
  }

  return isAfter(newDate, lastDate);
};