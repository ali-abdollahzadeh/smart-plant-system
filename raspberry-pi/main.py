import json
import math
import os
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

import paho.mqtt.client as mqtt
import requests


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


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
        self.state_lock = threading.Lock()

        # Simulated environment state
        self.soil_moisture_value = random.uniform(55.0, 70.0)
        self.temperature_bias = 0.0
        self.humidity_bias = 0.0
        self.last_sensor_update = time.time()

        # Simulated actuator/control state
        self.control_state = {
            "watering": "idle",
            "temperature_control": "idle",
            "humidity_control": "idle",
            "last_command": None,
            "last_command_time": None,
            "last_command_reason": None,
            "last_sensor_type": None
        }

        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.on_message = self.on_message

    # --------------------------------------------------
    # Topic helpers
    # --------------------------------------------------
    def sensor_topic(self) -> str:
        return f"{self.config['mqtt_topic_base']}/{self.config['device_id']}"

    def command_topic(self) -> str:
        return f"{self.config['mqtt_command_topic_base']}/{self.config['device_id']}"

    # --------------------------------------------------
    # Time helpers
    # --------------------------------------------------
    def day_fraction(self) -> float:
        now = datetime.now()
        seconds_today = now.hour * 3600 + now.minute * 60 + now.second
        return seconds_today / 86400.0

    # --------------------------------------------------
    # Simulated environment evolution
    # --------------------------------------------------
    def update_environment_state(self) -> None:
        now_ts = time.time()
        elapsed = now_ts - self.last_sensor_update
        self.last_sensor_update = now_ts

        elapsed_factor = elapsed / 10.0

        with self.state_lock:
            # Soil moisture naturally decays
            natural_decay_per_10s = 0.25
            decay = natural_decay_per_10s * elapsed_factor

            if self.control_state["watering"] == "increase_requested":
                self.soil_moisture_value += 2.0 * elapsed_factor
            elif self.control_state["watering"] == "reduction_requested":
                self.soil_moisture_value -= 0.6 * elapsed_factor
            else:
                self.soil_moisture_value -= decay

            self.soil_moisture_value = max(10.0, min(95.0, self.soil_moisture_value))

            # Temperature bias evolves according to temperature control
            if self.control_state["temperature_control"] == "cooling":
                self.temperature_bias -= 0.25 * elapsed_factor
            elif self.control_state["temperature_control"] == "heating":
                self.temperature_bias += 0.25 * elapsed_factor
            else:
                self.temperature_bias *= 0.97

            self.temperature_bias = max(-8.0, min(8.0, self.temperature_bias))

            # Humidity bias evolves according to humidity control
            if self.control_state["humidity_control"] == "increase_requested":
                self.humidity_bias += 0.5 * elapsed_factor
            elif self.control_state["humidity_control"] == "decrease_requested":
                self.humidity_bias -= 0.5 * elapsed_factor
            else:
                self.humidity_bias *= 0.97

            self.humidity_bias = max(-20.0, min(20.0, self.humidity_bias))

    # --------------------------------------------------
    # Simulated sensors
    # --------------------------------------------------
    def read_temperature(self) -> float:
        frac = self.day_fraction()
        base = 24.0 + 5.5 * math.sin(2 * math.pi * frac)
        noise = random.uniform(-0.6, 0.6)

        with self.state_lock:
            value = base + self.temperature_bias + noise

        return round(max(5.0, min(45.0, value)), 1)

    def read_soil_moisture(self) -> float:
        with self.state_lock:
            noise = random.uniform(-0.5, 0.5)
            value = self.soil_moisture_value + noise

        return round(max(0.0, min(100.0, value)), 1)

    def read_humidity(self) -> float:
        frac = self.day_fraction()
        base = 60.0 - 12.0 * math.sin(2 * math.pi * frac)
        noise = random.uniform(-1.5, 1.5)

        with self.state_lock:
            value = base + self.humidity_bias + noise

        return round(max(10.0, min(100.0, value)), 1)

    def collect_data(self) -> Dict[str, Any]:
        self.update_environment_state()

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
    # MQTT
    # --------------------------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            topic = self.command_topic()
            client.subscribe(topic)
            print(f"[MQTT] Connected to {self.config['mqtt_broker']}:{self.config['mqtt_port']}")
            print(f"[MQTT] Subscribed to command topic: {topic}")
            print(f"[MQTT] Publishing sensor data to: {self.sensor_topic()}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            print(f"[MQTT] Command received on {msg.topic}: {payload}")
            self.handle_command(payload)
        except json.JSONDecodeError:
            print(f"[MQTT] Invalid command JSON on topic {msg.topic}")
        except Exception as e:
            print(f"[MQTT] Command processing error: {e}")

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
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {e}")
                print("[MQTT] Retrying in 5 seconds...")
                time.sleep(5)

    # --------------------------------------------------
    # Command handling
    # --------------------------------------------------
    def handle_command(self, command_payload: Dict[str, Any]) -> None:
        command = command_payload.get("command")
        reason = command_payload.get("reason")
        sensor_type = command_payload.get("sensor_type")

        with self.state_lock:
            self.control_state["last_command"] = command
            self.control_state["last_command_time"] = now_utc_iso()
            self.control_state["last_command_reason"] = reason
            self.control_state["last_sensor_type"] = sensor_type

            if command == "increase_watering":
                self.control_state["watering"] = "increase_requested"
                action_message = "Watering increase requested"

            elif command == "reduce_watering":
                self.control_state["watering"] = "reduction_requested"
                action_message = "Watering reduction requested"

            elif command == "stop_watering_adjustment":
                self.control_state["watering"] = "idle"
                action_message = "Watering adjustment stopped"

            elif command == "start_cooling":
                self.control_state["temperature_control"] = "cooling"
                action_message = "Cooling activated"

            elif command == "start_heating":
                self.control_state["temperature_control"] = "heating"
                action_message = "Heating activated"

            elif command == "stop_temperature_control":
                self.control_state["temperature_control"] = "idle"
                action_message = "Temperature control stopped"

            elif command == "increase_humidity":
                self.control_state["humidity_control"] = "increase_requested"
                action_message = "Humidity increase requested"

            elif command == "decrease_humidity":
                self.control_state["humidity_control"] = "decrease_requested"
                action_message = "Humidity decrease requested"

            elif command == "stop_humidity_adjustment":
                self.control_state["humidity_control"] = "idle"
                action_message = "Humidity adjustment stopped"

            else:
                action_message = f"Unknown command received: {command}"

        print(f"[ACTION] {action_message} for {self.config['device_id']}")
        self.print_control_state()

    def print_control_state(self) -> None:
        with self.state_lock:
            snapshot = {
                "device_id": self.config["device_id"],
                "watering": self.control_state["watering"],
                "temperature_control": self.control_state["temperature_control"],
                "humidity_control": self.control_state["humidity_control"],
                "last_command": self.control_state["last_command"],
                "last_command_time": self.control_state["last_command_time"],
                "last_command_reason": self.control_state["last_command_reason"],
                "last_sensor_type": self.control_state["last_sensor_type"]
            }

        print("[CONTROL] Updated simulated control state:")
        print(json.dumps(snapshot, indent=2))

    # --------------------------------------------------
    # Sensor publishing
    # --------------------------------------------------
    def publish_data(self, data: Dict[str, Any]) -> None:
        topic = self.sensor_topic()
        payload = json.dumps(data)

        try:
            result = self.mqtt_client.publish(topic, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
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

        self.register_device()

        threading.Thread(target=self.register_loop, daemon=True).start()
        threading.Thread(target=self.publish_loop, daemon=True).start()

        self.mqtt_loop()


if __name__ == "__main__":
    node = SensorNode(load_config())
    node.run()