// ecosystem.config.cjs
// Usamos .cjs (CommonJS) para garantir que o PM2 consiga ler.
module.exports = {
  apps: [
    {
      name: 'integracao-hub',
      script: 'index.js', // O ponto de entrada principal
      watch: false,
      instances: 1,
      autorestart: true,
      restart_delay: 5000, // 5 segundos
      max_restarts: 10,
    },
  ],
};