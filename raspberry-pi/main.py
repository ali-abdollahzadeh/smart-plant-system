import json
import os
import math
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests
import paho.mqtt.client as mqtt


def load_config() -> Dict[str, Any]:
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
        "mqtt_command_topic_base": os.environ.get("MQTT_COMMAND_TOPIC_BASE", "smartplant/commands"),

        # Timing
        "publish_interval": int(os.environ.get("PUBLISH_INTERVAL", 10)),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
    }


CONFIG = load_config()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SensorNode:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.last_registration_time = 0.0
        self.soil = random.uniform(55, 70)

        # local simulated actuator/control state
        self.control_state = {
            "watering": "idle",
            "temperature_control": "idle",
            "humidity_control": "idle",
            "last_command": None,
            "last_command_time": None
        }

        # MQTT client for both publish and subscribe
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # --------------------------------------------------
    # Simulated sensors
    # --------------------------------------------------
    def _day_fraction(self) -> float:
        now = datetime.now()
        seconds_today = now.hour * 3600 + now.minute * 60 + now.second
        return seconds_today / 86400.0
    
    def read_temperature(self) -> float:
        frac = self._day_fraction()
        base = 24 + 6 * math.sin(2 * math.pi * frac)  # 18–30°C cycle
        noise = random.uniform(-0.7, 0.7)
        return round(base + noise, 1)

    def read_soil_moisture(self):
        decay = random.uniform(0.5, 2.0)
        self.soil = max(150.0, self.soil - decay)
        return round(self.soil, 1)

    def read_humidity(self) -> float:
        frac = self._day_fraction()
        base = 60 - 15 * math.sin(2 * math.pi * frac)  # 45–75% cycle
        noise = random.uniform(-1.5, 1.5)
        return round(base + noise, 1)

    def collect_data(self) -> Dict[str, Any]:
        return {
            "device_id": self.config["device_id"],
            "temperature": self.read_temperature(),
            "soil_moisture": self.read_soil_moisture(),
            "humidity": self.read_humidity(),
            "timestamp": now_utc_iso()
        }

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def build_registration_payload(self) -> Dict[str, Any]:
        return {
            "id": self.config["device_id"],
            "name": self.config["device_name"],
            "type": self.config["device_type"],
            "mqtt_topic": f"{self.config['mqtt_topic_base']}/{self.config['device_id']}",
            "command_topic": f"{self.config['mqtt_command_topic_base']}/{self.config['device_id']}",
            "status": "active"
        }

    def register_device(self) -> None:
        payload = self.build_registration_payload()

        try:
            response = requests.post(
                f"{self.config['catalog_url']}/devices",
                json=payload,
                timeout=5
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
            now = time.time()
            if now - self.last_registration_time >= self.config["register_interval"]:
                self.register_device()
            time.sleep(5)

    # --------------------------------------------------
    # MQTT command subscriber
    # --------------------------------------------------
    def command_topic(self) -> str:
        return f"{self.config['mqtt_command_topic_base']}/{self.config['device_id']}"

    def sensor_topic(self) -> str:
        return f"{self.config['mqtt_topic_base']}/{self.config['device_id']}"

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = self.command_topic()
            client.subscribe(topic)
            print(f"[MQTT] Connected to {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
            print(f"[MQTT] Subscribed to command topic: {topic}")
        else:
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            print(f"[MQTT] Command received on {msg.topic}: {payload}")
            self.handle_command(payload)
        except json.JSONDecodeError:
            print(f"[MQTT] Invalid command JSON on topic {msg.topic}")
        except Exception as e:
            print(f"[MQTT] Command processing error: {e}")

    def handle_command(self, command_payload: Dict[str, Any]) -> None:
        command = command_payload.get("command")
        reason = command_payload.get("reason")
        sensor_type = command_payload.get("sensor_type")

        self.control_state["last_command"] = command
        self.control_state["last_command_time"] = now_utc_iso()

        # Simulated local actions
        if command == "increase_watering":
            self.control_state["watering"] = "increase_requested"

        elif command == "reduce_watering":
            self.control_state["watering"] = "reduction_requested"

        elif command == "stop_watering_adjustment":
            self.control_state["watering"] = "idle"

        elif command == "start_cooling":
            self.control_state["temperature_control"] = "cooling"

        elif command == "start_heating":
            self.control_state["temperature_control"] = "heating"

        elif command == "stop_temperature_control":
            self.control_state["temperature_control"] = "idle"

        elif command == "increase_humidity":
            self.control_state["humidity_control"] = "increase_requested"

        elif command == "decrease_humidity":
            self.control_state["humidity_control"] = "decrease_requested"

        elif command == "stop_humidity_adjustment":
            self.control_state["humidity_control"] = "idle"

        print("[CONTROL] Updated simulated control state:")
        print(json.dumps({
            "device_id": self.config["device_id"],
            "command": command,
            "reason": reason,
            "sensor_type": sensor_type,
            "control_state": self.control_state
        }, indent=2))

    def mqtt_loop(self) -> None:
        while True:
            try:
                self.mqtt_client.connect(
                    self.config["mqtt_broker"],
                    self.config["mqtt_port"],
                    keepalive=60
                )
                self.mqtt_client.loop_forever()
            except Exception as e:
                print(f"[MQTT] Connection error: {e}")
                print("[MQTT] Retrying in 5 seconds...")
                time.sleep(5)

    # --------------------------------------------------
    # Sensor publisher
    # --------------------------------------------------
    def publish_data(self, data: Dict[str, Any]) -> None:
        topic = self.sensor_topic()
        payload = json.dumps(data)

        try:
            result = self.mqtt_client.publish(topic, payload)
            if result.rc == 0:
                print(f"[MQTT] Published to {topic}: {payload}")
            else:
                print(f"[MQTT] Failed to publish to {topic}, rc={result.rc}")
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

        # first registration
        self.register_device()

        # background threads
        threading.Thread(target=self.register_loop, daemon=True).start()
        threading.Thread(target=self.publish_loop, daemon=True).start()

        # blocking MQTT subscriber loop
        self.mqtt_loop()


if __name__ == "__main__":
    node = SensorNode(CONFIG)
    node.run()