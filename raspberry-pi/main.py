import json
import os
import threading
import time
from typing import Any, Dict

from shared.MyMQTT import MyMQTT
from simulator import PlantSimulator
import requests


def load_config() -> Dict[str, Any]:
    return {
        "device_id": os.environ.get("DEVICE_ID", "raspi-01"),
        "device_name": os.environ.get("DEVICE_NAME", "Plant Sensor Node 1"),
        "device_type": os.environ.get("DEVICE_TYPE", "sensor_node"),
        "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
        "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
        "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
        "mqtt_topic_base": os.environ.get("MQTT_TOPIC_BASE", "smartplant/sensors"),
        "mqtt_command_topic_base": os.environ.get("MQTT_COMMAND_TOPIC_BASE", "smartplant/commands"),
        "publish_interval": int(os.environ.get("PUBLISH_INTERVAL", 10)),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
    }


class SensorNode:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.last_registration_time = 0.0
        self.simulator = PlantSimulator(self.config["device_id"])

        self.mqtt_connected = False
        self.mqtt_client = MyMQTT(
            clientID=self.config["device_id"],
            broker=self.config["mqtt_broker"],
            port=self.config["mqtt_port"],
            notifier=self
        )

    # --------------------------------------------------
    # Topic helpers
    # --------------------------------------------------
    def sensor_topic(self) -> str:
        return f"{self.config['mqtt_topic_base']}/{self.config['device_id']}"

    def command_topic(self) -> str:
        return f"{self.config['mqtt_command_topic_base']}/{self.config['device_id']}"

    # --------------------------------------------------
    # Sensor data collection
    # --------------------------------------------------
    def collect_data(self) -> Dict[str, Any]:
        return self.simulator.collect_data()

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def build_registration_payload(self) -> Dict[str, Any]:
        return {
            "id": self.config["device_id"],
            "name": self.config["device_name"],
            "type": self.config["device_type"],
            "mqtt_topic": self.sensor_topic(),
            "command_topic": self.command_topic(),
            "status": "active"
        }

    def register_device(self) -> None:
        payload = self.build_registration_payload()

        try:
            response = requests.post(
                f"{self.config['catalog_url']}/devices",
                json=payload,
                timeout=10
            )

            if response.status_code in (200, 201):
                self.last_registration_time = time.time()
                print(f"[CATALOGUE] Registration successful: {payload}")
            else:
                print(
                    f"[CATALOGUE] Registration failed - "
                    f"status={response.status_code}, response={response.text}"
                )

        except requests.RequestException as e:
            print(f"[CATALOGUE] Registration error: {e}")

    def register_loop(self) -> None:
        while True:
            now_ts = time.time()
            if now_ts - self.last_registration_time >= self.config["register_interval"]:
                self.register_device()
            time.sleep(5)

    # --------------------------------------------------
    # MQTT Callbacks & Actions
    # --------------------------------------------------
    def notify(self, topic: str, payload: str) -> None:
        try:
            payload_dict = json.loads(payload)
            print(f"[MQTT] Command received on {topic}: {payload_dict}")
            self.simulator.handle_command(payload_dict)
        except json.JSONDecodeError:
            print(f"[MQTT] Invalid command JSON on topic {topic}")
        except Exception as e:
            print(f"[MQTT] Command processing error: {e}")

    # --------------------------------------------------
    # Sensor publishing
    # --------------------------------------------------
    def publish_data(self, data: Dict[str, Any]) -> None:
        topic = self.sensor_topic()

        try:
            self.mqtt_client.myPublish(topic, data)
            print(f"[MQTT] Published to {topic}: {data}")
        except Exception as e:
            print(f"[MQTT] Publish error: {e}")

    def publish_loop(self) -> None:
        while True:
            sensor_data = self.collect_data()
            self.publish_data(sensor_data)
            time.sleep(self.config["publish_interval"])

    # --------------------------------------------------
    # Run
    # --------------------------------------------------
    def run(self) -> None:
        print("[START] Raspberry Pi sensor node started")
        print(f"[INFO] Device ID: {self.config['device_id']}")
        print(f"[INFO] Device Name: {self.config['device_name']}")
        print(f"[INFO] Device Type: {self.config['device_type']}")
        print(f"[INFO] Catalogue URL: {self.config['catalog_url']}")
        print(f"[INFO] MQTT Broker: {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
        print(f"[INFO] Sensor Topic: {self.sensor_topic()}")
        print(f"[INFO] Command Topic: {self.command_topic()}")
        print(f"[INFO] Publish Interval: {self.config['publish_interval']} seconds")
        print(f"[INFO] Register Interval: {self.config['register_interval']} seconds")

        self.register_device()

        threading.Thread(target=self.register_loop, daemon=True).start()
        threading.Thread(target=self.publish_loop, daemon=True).start()

        # Start MQTT client
        self.mqtt_client.start()
        self.mqtt_client.mySubscribe(self.command_topic())

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("[STOP] Stopping MQTT client")
            self.mqtt_client.stop()


if __name__ == "__main__":
    node = SensorNode(load_config())
    node.run()