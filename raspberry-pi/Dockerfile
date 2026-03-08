FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV DEVICE_ID=raspi-01
ENV DEVICE_NAME="Plant Sensor Node 1"
ENV DEVICE_TYPE=sensor_node

ENV MQTT_BROKER=mosquitto
ENV MQTT_PORT=1883
ENV MQTT_TOPIC_BASE=smartplant/sensors
ENV MQTT_COMMAND_TOPIC_BASE=smartplant/commands

ENV CATALOG_URL=http://catalogue:8000
ENV PUBLISH_INTERVAL=10
ENV REGISTER_INTERVAL=60

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]