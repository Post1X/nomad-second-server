# Быстрая установка (шпаргалка)

## 1. Установка системных зависимостей (Ubuntu/Debian)

```bash
# Node.js 18.x
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# MongoDB
wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo systemctl start mongod
sudo systemctl enable mongod

# Зависимости для Puppeteer
sudo apt-get install -y \
  ca-certificates fonts-liberation libappindicator3-1 libasound2 \
  libatk-bridge2.0-0 libatk1.0-0 libc6 libcairo2 libcups2 libdbus-1-3 \
  libexpat1 libfontconfig1 libgbm1 libgcc1 libglib2.0-0 libgtk-3-0 \
  libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 \
  libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 \
  libxext6 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 libxtst6 \
  lsb-release wget xdg-utils

# PM2 (опционально, для production)
sudo npm install -g pm2
```

## 2. Установка проекта

```bash
cd /path/to/nomad-second-server
npm install
```

## 3. Настройка .env

```bash
cat > .env << EOF
DB_NAME=nomad_second
PORT=4001
PARSING_SERVER_API_KEY=$(openssl rand -hex 32)
EVENTIM_URL=
EVENTIM_USERNAME=
EVENTIM_PASSWORD=
EOF
```

**Важно:** Замените `PARSING_SERVER_API_KEY` на свой секретный ключ!

## 4. Запуск

### Development:
```bash
npm run dev
```

### Production (с PM2):
```bash
npm run build
pm2 start dist/bin/www --name nomad-second-server
pm2 save
```

## 5. Проверка

```bash
# Проверка доступности
curl http://localhost:4001/ping
# Должен вернуть: "pong"

# Проверка с API key
curl -H "X-API-Key: YOUR_API_KEY" http://localhost:4001/
```

## Проблемы?

См. подробную инструкцию: [INSTALLATION.md](./INSTALLATION.md)

