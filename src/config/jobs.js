import dotenv from 'dotenv';
dotenv.config();

// Helper para converter minutos para milissegundos (ou usa o valor direto)
const getInterval = (envVar, fallback) => {
  const value = parseInt(process.env[envVar], 10);
  return isNaN(value) ? fallback : value;
}

export const jobsConfig = {
  // --- JOB 1: ATUALCARGO ---
  atualcargo: {
    enabled: !!(process.env.ATUALCARGO_URL && process.env.ATUALCARGO_API_KEY),
    interval: getInterval('JOB_INTERVAL_ATUALCARGO', 300000), // 5 min
    url: process.env.ATUALCARGO_URL,
    apiKey: process.env.ATUALCARGO_API_KEY,
    username: process.env.ATUALCARGO_USERNAME,
    password: process.env.ATUALCARGO_PASSWORD,
    tokenExpirationMs: getInterval('ATUALCARGO_TOKEN_EXPIRATION_MS', 270000), // 4.5 min
    fabricanteId: process.env.SANKHYA_ISCA_FABRICANTE_ID_ATUALCARGO || '2',
  },

  // --- JOB 2: SITRAX (Santos e Zanon) ---
  sitrax: {
    enabled: !!(process.env.SITRAX_URL && process.env.SITRAX_LOGIN),
    interval: getInterval('JOB_INTERVAL_SITRAX', 300000), // 5 min
    url: process.env.SITRAX_URL,
    login: process.env.SITRAX_LOGIN,
    cgruChave: process.env.SITRAX_CGRUCHAVE,
    cusuChave: process.env.SITRAX_CUSUCHAVE,
    fabricanteId: process.env.SANKHYA_ISCA_FABRICANTE_ID_SITRAX || '3',
  },
};