import json
import os
import random
import time
from datetime import datetime, timezone

import requests
import paho.mqtt.publish as publish


def load_config():
    """
    Load configuration for one Raspberry Pi sensor node.

    The same code can be reused for any number of Raspberry Pi devices.
    Only DEVICE_ID and DEVICE_NAME need to change per instance.
    """
    return {
        # Unique for each Raspberry Pi instance
        "device_id": os.environ.get("DEVICE_ID", "raspi-01"),
        "device_name": os.environ.get("DEVICE_NAME", "Plant Sensor Node 1"),
        "device_type": os.environ.get("DEVICE_TYPE", "sensor_node"),

        # Shared infrastructure
        "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
        "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
        "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
        "mqtt_topic_base": os.environ.get("MQTT_TOPIC_BASE", "smartplant/sensors"),

        # Timing
        "publish_interval": int(os.environ.get("PUBLISH_INTERVAL", 10)),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
    }


CONFIG = load_config()


class SensorNode:
    def __init__(self, config):
        self.config = config
        self.last_registration_time = 0

    def now_utc_iso(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # -----------------------------
    # Simulated sensors
    # Replace these later with real Raspberry Pi sensor readings if needed
    # -----------------------------
    def read_temperature(self):
        return round(random.uniform(18.0, 32.0), 2)

    def read_soil_moisture(self):
        return round(random.uniform(25.0, 80.0), 2)

    def read_light(self):
        return round(random.uniform(100.0, 1000.0), 2)

    def collect_data(self):
        return {
            "device_id": self.config["device_id"],
            "temperature": self.read_temperature(),
            "soil_moisture": self.read_soil_moisture(),
            "light": self.read_light(),
            "timestamp": self.now_utc_iso()
        }

    # -----------------------------
    # Catalogue registration
    # -----------------------------
    def build_registration_payload(self):
        return {
            "id": self.config["device_id"],
            "name": self.config["device_name"],
            "type": self.config["device_type"],
            "mqtt_topic": f"{self.config['mqtt_topic_base']}/{self.config['device_id']}",
            "status": "active"
        }

    def register_device(self):
        payload = self.build_registration_payload()

        try:
            response = requests.post(
                f"{self.config['catalog_url']}/devices",
                json=payload,
                timeout=5
            )

            if response.status_code in (200, 201):
                print(f"[CATALOGUE] Registration successful: {payload}")
                self.last_registration_time = time.time()
            else:
                print(
                    f"[CATALOGUE] Registration failed - "
                    f"status={response.status_code}, response={response.text}"
                )

        except requests.RequestException as e:
            print(f"[CATALOGUE] Registration error: {e}")

    def register_if_needed(self):
        current_time = time.time()
        if current_time - self.last_registration_time >= self.config["register_interval"]:
            self.register_device()

    # -----------------------------
    # MQTT publishing
    # -----------------------------
    def publish_data(self, data):
        topic = f"{self.config['mqtt_topic_base']}/{self.config['device_id']}"
        payload = json.dumps(data)

        try:
            publish.single(
                topic=topic,
                payload=payload,
                hostname=self.config["mqtt_broker"],
                port=self.config["mqtt_port"]
            )
            print(f"[MQTT] Published to {topic}: {payload}")

        except Exception as e:
            print(f"[MQTT] Publish error: {e}")

    # -----------------------------
    # Main execution loop
    # -----------------------------
    def run(self):
        print("[START] Raspberry Pi sensor node started")
        print(f"[INFO] Device ID: {self.config['device_id']}")
        print(f"[INFO] Device Name: {self.config['device_name']}")
        print(f"[INFO] Device Type: {self.config['device_type']}")
        print(f"[INFO] Catalogue URL: {self.config['catalog_url']}")
        print(f"[INFO] MQTT Broker: {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
        print(f"[INFO] MQTT Topic Base: {self.config['mqtt_topic_base']}")
        print(f"[INFO] Publish Interval: {self.config['publish_interval']} seconds")
        print(f"[INFO] Register Interval: {self.config['register_interval']} seconds")

        # Initial registration
        self.register_device()

        while True:
            self.register_if_needed()
            sensor_data = self.collect_data()
            self.publish_data(sensor_data)
            time.sleep(self.config["publish_interval"])


if __name__ == "__main__":
    node = SensorNode(CONFIG)
    node.run()