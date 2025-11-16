FROM node:18-slim
WORKDIR /app
COPY package.json .
RUN npm install --omit=dev
COPY . .
EXPOSE 7860
CMD ["node", "dark-server-vps.js"]