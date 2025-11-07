import dotenv from 'dotenv';
import path from 'path';

// Carrega as variáveis de ambiente do .env
dotenv.config({ path: path.resolve(process.cwd(), '.env') });

const config = {
  atualcargo: {
    baseUrl: process.env.ATUALCARGO_URL,
    accessKey: process.env.ATUALCARGO_ACCESS_KEY,
    username: process.env.ATUALCARGO_USERNAME, // NOVO
    password: process.env.ATUALCARGO_PASSWORD, // NOVO
  },
  sankhya: {
    baseUrl: process.env.SANKHYA_URL,
    username: process.env.SANKHYA_USER,
    password: process.env.SANKHYA_PASSWORD,
  },
  service: {
    // Converte minutos para milissegundos
    loopInterval: parseInt(process.env.LOOP_INTERVAL_MINUTES, 10) * 60 * 1000,
    // Converte segundos para milissegundos
    timeout: parseInt(process.env.REQUEST_TIMEOUT_SECONDS, 10) * 1000,
    logLevel: process.env.LOG_LEVEL || 'info',
  },
};

// Validação simples para garantir que as variáveis essenciais foram carregadas
const requiredKeys = [
  config.atualcargo.baseUrl,
  config.atualcargo.accessKey,
  config.atualcargo.username, // NOVO
  config.atualcargo.password, // NOVO
  config.sankhya.baseUrl,
  config.sankhya.username,
  config.sankhya.password,
];

if (requiredKeys.some((key) => !key)) {
  console.error(
    'ERRO: Variáveis de ambiente essenciais não definidas. Verifique seu arquivo .env'
  );
  process.exit(1);
}

export default config;