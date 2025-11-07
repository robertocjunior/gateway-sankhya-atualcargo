# 1. Base Image
FROM node:20-alpine

# 2. Set working directory
WORKDIR /app

# 3. Install PM2 globally within the container
RUN npm install -g pm2

# 4. Copy package files and install dependencies
COPY package.json .
COPY package-lock.json .

# Usamos 'npm ci' para uma instalação limpa de produção
RUN npm ci --omit=dev

# 5. Copy application code
COPY . .

# 6. Expose port (se houver um servidor web, não é o caso aqui, mas é boa prática)
# EXPOSE 3000

# 7. Start the application using pm2-runtime
# pm2-runtime é feito para containers, mantendo o processo no foreground
CMD [ "pm2-runtime", "start", "ecosystem.config.cjs" ]