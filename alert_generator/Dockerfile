FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV SERVICE_ID=alert-generator
ENV SERVICE_NAME=AlertGenerator
ENV SERVICE_TYPE=alert_generator
ENV SERVICE_HOST=0.0.0.0
ENV SERVICE_PORT=8091

ENV CATALOG_URL=http://catalogue:8000
ENV REGISTER_INTERVAL=60

ENV MQTT_BROKER=mosquitto
ENV MQTT_PORT=1883

ENV THINGSPEAK_ADAPTER_URL=http://thingspeak-adapter:8080
ENV CONFIG_FILE=/app/config.json

COPY alert_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alert_generator/main.py .
COPY alert_generator/config.json .

EXPOSE 8091

CMD ["python", "main.py"]