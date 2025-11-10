import dotenv from 'dotenv';
dotenv.config();

// Helper para converter minutos para milissegundos
const minToMs = (min) => (parseInt(min, 10) || 5) * 60 * 1000;

export const jobsConfig = {
  // --- JOB 1: ATUALCARGO ---
  atualcargo: {
    enabled: true,
    interval: minToMs(process.env.JOB_INTERVAL_ATUALCARGO),
    api: {
      baseUrl: process.env.ATUALCARGO_URL,
      accessKey: process.env.ATUALCARGO_ACCESS_KEY,
      username: process.env.ATUALCARGO_USERNAME,
      password: process.env.ATUALCARGO_PASSWORD,
    },
  },

  // --- JOB 2: SITRAX (Santos e Zanon) ---
  sitrax: {
    enabled: true,
    interval: minToMs(process.env.JOB_INTERVAL_SITRAX),
    api: {
      baseUrl: process.env.SITRAX_URL,
      login: process.env.SITRAX_LOGIN,
      cgruChave: process.env.SITRAX_CGRUCHAVE,
      cusuChave: process.env.SITRAX_CUSUCHAVE,
    },
  },
};