# Инструкция по установке второго сервера

## Системные требования

### 1. Node.js
- **Версия:** Node.js 16.x или выше (рекомендуется 18.x LTS)
- **Проверка:** `node --version`
- **Установка (Ubuntu/Debian):**
  ```bash
  curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
  sudo apt-get install -y nodejs
  ```

### 2. MongoDB
- **Версия:** MongoDB 4.4 или выше
- **Проверка:** `mongod --version`
- **Установка (Ubuntu/Debian):**
  ```bash
  wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | sudo apt-key add -
  echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/6.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list
  sudo apt-get update
  sudo apt-get install -y mongodb-org
  sudo systemctl start mongod
  sudo systemctl enable mongod
  ```

### 3. Системные зависимости для Puppeteer (Linux)

Puppeteer требует установки Chromium и его системных зависимостей:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y \
  ca-certificates \
  fonts-liberation \
  libappindicator3-1 \
  libasound2 \
  libatk-bridge2.0-0 \
  libatk1.0-0 \
  libc6 \
  libcairo2 \
  libcups2 \
  libdbus-1-3 \
  libexpat1 \
  libfontconfig1 \
  libgbm1 \
  libgcc1 \
  libglib2.0-0 \
  libgtk-3-0 \
  libnspr4 \
  libnss3 \
  libpango-1.0-0 \
  libpangocairo-1.0-0 \
  libstdc++6 \
  libx11-6 \
  libx11-xcb1 \
  libxcb1 \
  libxcomposite1 \
  libxcursor1 \
  libxdamage1 \
  libxext6 \
  libxfixes3 \
  libxi6 \
  libxrandr2 \
  libxrender1 \
  libxss1 \
  libxtst6 \
  lsb-release \
  wget \
  xdg-utils
```

**CentOS/RHEL:**
```bash
sudo yum install -y \
  alsa-lib \
  atk \
  cups-libs \
  gtk3 \
  ipa-gothic-fonts \
  libXcomposite \
  libXcursor \
  libXdamage \
  libXext \
  libXi \
  libXrandr \
  libXScrnSaver \
  libXtst \
  pango \
  xorg-x11-fonts-100dpi \
  xorg-x11-fonts-75dpi \
  xorg-x11-utils
```

### 4. PM2 (для production, опционально)
```bash
sudo npm install -g pm2
```

## Установка проекта

### 1. Клонирование/копирование проекта
```bash
cd /path/to/nomad-second-server
```

### 2. Установка зависимостей
```bash
npm install
# или
yarn install
```

**Важно:** При установке Puppeteer автоматически загрузит Chromium (~170MB). Это может занять некоторое время.

### 3. Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```bash
cp .env.example .env
nano .env
```

Заполните следующие переменные:

```env
# Database
DB_NAME=nomad_second

# Server
PORT=4001

# API Key для авторизации запросов от основного сервера
# ВАЖНО: Используйте сложный случайный ключ!
PARSING_SERVER_API_KEY=your-secret-api-key-here-change-this

# Eventim credentials (опционально, если используется парсинг Eventim)
EVENTIM_URL=https://example.com/eventim.json.gz
EVENTIM_USERNAME=username
EVENTIM_PASSWORD=password
```

### 4. Проверка подключения к MongoDB

Убедитесь, что MongoDB запущен:
```bash
sudo systemctl status mongod
# или
sudo service mongod status
```

Если не запущен:
```bash
sudo systemctl start mongod
```

## Запуск сервера

### Development режим
```bash
npm run dev
# или
yarn dev
```

### Production режим

#### Вариант 1: С PM2 (рекомендуется)
```bash
# Сборка проекта
npm run build

# Запуск с PM2
pm2 start dist/bin/www --name nomad-second-server

# Сохранение конфигурации PM2
pm2 save
pm2 startup
```

#### Вариант 2: Без PM2
```bash
npm run prod
```

### Проверка работы

1. **Проверка доступности:**
   ```bash
   curl http://localhost:4001/ping
   # Должен вернуть: "pong"
   ```

2. **Проверка корневого эндпоинта:**
   ```bash
   curl http://localhost:4001/
   # Должен вернуть JSON с status: "ok"
   ```

3. **Проверка авторизации:**
   ```bash
   curl -H "X-API-Key: your-secret-api-key-here" http://localhost:4001/parsing/unprocessed
   # Должен вернуть JSON с массивом операций
   ```

## Настройка firewall (если нужно)

Если сервер должен быть доступен извне:

```bash
# Ubuntu/Debian (ufw)
sudo ufw allow 4001/tcp

# CentOS/RHEL (firewalld)
sudo firewall-cmd --permanent --add-port=4001/tcp
sudo firewall-cmd --reload
```

## Настройка Nginx (опционально, для production)

Если нужно использовать Nginx как reverse proxy:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:4001;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

## Мониторинг и логи

### PM2
```bash
# Просмотр логов
pm2 logs nomad-second-server

# Просмотр статуса
pm2 status

# Перезапуск
pm2 restart nomad-second-server
```

### Без PM2
Логи выводятся в консоль. Для production рекомендуется использовать PM2 или systemd.

## Troubleshooting

### Проблема: Puppeteer не запускается

**Ошибка:** `Failed to launch browser`

**Решение:**
1. Убедитесь, что установлены все системные зависимости (см. выше)
2. Проверьте, что в коде используются флаги `--no-sandbox` и `--disable-setuid-sandbox` (уже есть в коде)
3. Если проблема сохраняется, попробуйте:
   ```bash
   sudo apt-get install -y chromium-browser
   ```

### Проблема: MongoDB не подключается

**Ошибка:** `MongooseError: connect ECONNREFUSED`

**Решение:**
1. Проверьте, что MongoDB запущен: `sudo systemctl status mongod`
2. Проверьте, что в `.env` указано правильное имя БД
3. Проверьте права доступа к MongoDB

### Проблема: Порт уже занят

**Ошибка:** `EADDRINUSE: address already in use`

**Решение:**
1. Измените порт в `.env` файле
2. Или найдите и остановите процесс, использующий порт:
   ```bash
   sudo lsof -i :4001
   sudo kill -9 <PID>
   ```

## Обновление

```bash
# Остановить сервер
pm2 stop nomad-second-server  # если используете PM2

# Обновить код
git pull  # или скопировать новые файлы

# Обновить зависимости
npm install

# Пересобрать
npm run build

# Запустить снова
pm2 restart nomad-second-server  # если используете PM2
```

## Дополнительные настройки

### Увеличение лимита памяти для Node.js

Если парсинг больших объемов данных вызывает проблемы с памятью:

```bash
# В package.json изменить скрипт:
"prod": "node --max-old-space-size=4096 -r dotenv/config ./dist/bin/www"

# Или в PM2 создать ecosystem.config.js:
module.exports = {
  apps: [{
    name: 'nomad-second-server',
    script: './dist/bin/www',
    max_memory_restart: '1G',
    env: {
      NODE_ENV: 'production'
    }
  }]
}
```

### Настройка автоматического перезапуска при сбое

PM2 автоматически перезапускает процесс при сбое. Для systemd создайте сервис:

```bash
sudo nano /etc/systemd/system/nomad-second-server.service
```

```ini
[Unit]
Description=Nomad Second Server
After=network.target mongod.service

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/nomad-second-server
ExecStart=/usr/bin/node -r dotenv/config /path/to/nomad-second-server/dist/bin/www
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Затем:
```bash
sudo systemctl daemon-reload
sudo systemctl enable nomad-second-server
sudo systemctl start nomad-second-server
```

