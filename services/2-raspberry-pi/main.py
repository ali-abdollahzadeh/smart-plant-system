import json
import os
import random
import time
from datetime import datetime, timezone

import requests
import paho.mqtt.publish as publish


# ----------------- Environment variables -----------------
DEVICE_ID = os.environ.get("DEVICE_ID", "raspi-01")
DEVICE_NAME = os.environ.get("DEVICE_NAME", "Plant Sensor Node 1")
DEVICE_TYPE = os.environ.get("DEVICE_TYPE", "sensor_node")

MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC_BASE = os.environ.get("MQTT_TOPIC_BASE", "smartplant/sensors")

CATALOG_URL = os.environ.get("CATALOG_URL", "http://catalogue:8000")
PUBLISH_INTERVAL = int(os.environ.get("PUBLISH_INTERVAL", 10))
REGISTER_INTERVAL = int(os.environ.get("REGISTER_INTERVAL", 60))

DEVICE_ENDPOINT = os.environ.get("DEVICE_ENDPOINT", f"http://{DEVICE_ID}:5000")


class SensorNode:
    def __init__(self):
        self.last_registration = 0

    # ----------------- Simulated sensors -----------------
    def simulate_temperature_sensor(self):
        return round(random.uniform(15.0, 30.0), 2)

    def simulate_soil_moisture_sensor(self):
        return round(random.uniform(30.0, 70.0), 2)

    def simulate_light_sensor(self):
        return round(random.uniform(100.0, 1000.0), 2)

    def now_utc_iso(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def read_sensors(self):
        return {
            "device_id": DEVICE_ID,
            "temperature": self.simulate_temperature_sensor(),
            "soil_moisture": self.simulate_soil_moisture_sensor(),
            "light": self.simulate_light_sensor(),
            "timestamp": self.now_utc_iso()
        }

    # ----------------- Catalogue registration -----------------
    def register_device(self):
        payload = {
            "id": DEVICE_ID,
            "name": DEVICE_NAME,
            "type": DEVICE_TYPE,
            "endpoint": DEVICE_ENDPOINT,
            "mqtt_topic": f"{MQTT_TOPIC_BASE}/{DEVICE_ID}",
            "status": "active"
        }

        try:
            response = requests.post(
                f"{CATALOG_URL}/devices",
                json=payload,
                timeout=5
            )

            if response.status_code in (200, 201):
                print(f"[CATALOGUE] Device registered successfully: {payload}")
                self.last_registration = time.time()
            else:
                print(
                    f"[CATALOGUE] Registration failed "
                    f"({response.status_code}): {response.text}"
                )

        except requests.RequestException as e:
            print(f"[CATALOGUE] Registration error: {e}")

    def register_if_needed(self):
        now = time.time()
        if now - self.last_registration >= REGISTER_INTERVAL:
            self.register_device()

    # ----------------- MQTT publishing -----------------
    def publish_sensor_data(self, sensor_data):
        topic = f"{MQTT_TOPIC_BASE}/{DEVICE_ID}"
        payload = json.dumps(sensor_data)

        try:
            publish.single(
                topic,
                payload=payload,
                hostname=MQTT_BROKER,
                port=MQTT_PORT
            )
            print(f"[MQTT] Published on {topic}: {payload}")
        except Exception as e:
            print(f"[MQTT] Failed to publish sensor data: {e}")

    # ----------------- Main loop -----------------
    def run(self):
        print("[START] Raspberry Pi sensor node started")
        print(f"[INFO] DEVICE_ID={DEVICE_ID}")
        print(f"[INFO] MQTT_BROKER={MQTT_BROKER}:{MQTT_PORT}")
        print(f"[INFO] CATALOG_URL={CATALOG_URL}")
        print(f"[INFO] PUBLISH_INTERVAL={PUBLISH_INTERVAL}s")
        print(f"[INFO] REGISTER_INTERVAL={REGISTER_INTERVAL}s")

        # First registration attempt at startup
        self.register_device()

        while True:
            self.register_if_needed()
            sensor_data = self.read_sensors()
            self.publish_sensor_data(sensor_data)
            time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    node = SensorNode()
    node.run()