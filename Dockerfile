FROM node:18-alpine

WORKDIR /app

# Install curl so the container healthcheck (CMD curl -f http://localhost:8080/health) works
RUN apk add --no-cache curl

COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY . .

RUN mkdir -p /app/exports

EXPOSE 8080

CMD ["node", "src/server.js"]
