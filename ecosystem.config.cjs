// ecosystem.config.cjs
module.exports = {
  apps: [
    {
      name: 'gateway-sankhya',
      script: 'src/index.js',
      watch: false,
      instances: 1,
      autorestart: true,
      restart_delay: 5000, // 5 segundos
      max_restarts: 10,
    },
  ],
};