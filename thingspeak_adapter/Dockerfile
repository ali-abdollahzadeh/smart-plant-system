FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV SERVICE_ID=thingspeak-adapter
ENV SERVICE_NAME=ThingSpeak Adapter
ENV SERVICE_TYPE=thingspeak_adapter
ENV SERVICE_HOST=0.0.0.0
ENV SERVICE_PORT=8080

ENV CATALOG_URL=http://catalogue:8000
ENV REGISTER_INTERVAL=60

ENV MQTT_BROKER=mosquitto
ENV MQTT_PORT=1883

ENV CONFIG_FILE=config.json
ENV REGISTRY_FILE=channel_registry.json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY config.json .
COPY channel_registry.json .

EXPOSE 8080

CMD ["python", "main.py"]