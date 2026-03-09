FROM python:3.11-slim


WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV TELEGRAM_BOT_TOKEN=
ENV CATALOG_URL=http://catalogue:8000
ENV ALERT_GENERATOR_URL=http://alert-generator:8091
ENV ANALYTICS_URL=http://analytics:8090

ENV SERVICE_ID=telegram-bot
ENV SERVICE_NAME=TelegramBot
ENV SERVICE_TYPE=telegram_bot
ENV REGISTER_INTERVAL=60

ENV CONFIG_FILE=/app/config.json
ENV AUTH_FILE=/app/auth.json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY config.json .
COPY auth.json .

CMD ["python", "main.py"]