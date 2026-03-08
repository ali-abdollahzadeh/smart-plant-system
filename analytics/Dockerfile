FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV SERVICE_ID=analytics-control
ENV SERVICE_NAME=AnalyticsControlService
ENV SERVICE_TYPE=analytics_control
ENV SERVICE_HOST=0.0.0.0
ENV SERVICE_PORT=8090

ENV CATALOG_URL=http://catalogue:8000
ENV REGISTER_INTERVAL=60

ENV MQTT_BROKER=mosquitto
ENV MQTT_PORT=1883

ENV CONFIG_FILE=/app/config.json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY config.json .

EXPOSE 8090

CMD ["python", "main.py"]