import json
import os
import threading
import time
import requests
import paho.mqtt.client as mqtt
from simulator import PlantSimulator


class SensorNode:

    def __init__(self, config_file="config.json"):
        self.config_file = os.environ.get("CONFIG_FILE", config_file)
        self.config = self.load_config(self.config_file)

        # Load configuration parameters from config.json with environment variable overrides
        self.id = os.environ.get("DEVICE_ID", self.config.get("device_id", "raspi-01"))
        self.device_name = os.environ.get("DEVICE_NAME", self.config.get("device_name", "Plant Sensor Node 1"))
        self.device_type = os.environ.get("DEVICE_TYPE", self.config.get("device_type", "sensor_node"))
        self.catalog_url = os.environ.get("CATALOG_URL", self.config.get("catalog_url", "http://catalogue:8000")).rstrip("/")
        self.publish_interval = int(os.environ.get("PUBLISH_INTERVAL", self.config.get("publish_interval", 10)))
        self.registration_retry_delay = int(os.environ.get("REGISTRATION_RETRY_DELAY", self.config.get("registration_retry_delay", 5)))

        mqtt_info = self.config.get("mqtt_info", {})
        self.broker = os.environ.get("MQTT_BROKER", mqtt_info.get("broker", "mosquitto"))
        self.port = int(os.environ.get("MQTT_PORT", mqtt_info.get("port", 1883)))

        sensor_topic_base = os.environ.get("MQTT_TOPIC_BASE", mqtt_info.get("sensor_topic_base", "smartplant/sensors"))
        command_topic_base = os.environ.get("MQTT_COMMAND_TOPIC_BASE", mqtt_info.get("command_topic_base", "smartplant/commands"))

        # Dynamic Topics
        self.sensor_topic = f"{sensor_topic_base}/{self.id}"
        self.command_topic = f"{command_topic_base}/{self.id}"

        self.running = True
        self.mqtt_connected = False
        self.simulator = PlantSimulator(self.id)

        # MQTT Setup with unique client ID
        client_id = f"rpi-{self.id}"
        self.mqtt_client = mqtt.Client(client_id=client_id, clean_session=True)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

    def load_config(self, config_path):
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"[CONFIG] Warning: Failed to load config file '{config_path}': {e}")
        else:
            print(f"[CONFIG] Warning: Config file '{config_path}' not found. Using default settings.")
        return {}

    # =========================================================================
    # Catalogue Registration
    # =========================================================================
    def register_device(self):
        payload = {
            "id": self.id,
            "name": self.device_name,
            "type": self.device_type,
            "mqtt_topic": self.sensor_topic,
            "command_topic": self.command_topic,
            "status": "active"
        }

        try:
            url = f"{self.catalog_url}/devices"
            response = requests.post(url, json=payload, timeout=10)

            if response.status_code in (200, 201):
                try:
                    action = response.json().get("action")
                except ValueError:
                    action = None

                if action == "updated":
                    print("[CATALOGUE] Device already registered; information refreshed")
                else:
                    print(f"[CATALOGUE] Registration successful: {payload}")
                return True

            if response.status_code == 409:
                try:
                    error_message = str(response.json().get("error", "")).lower()
                except ValueError:
                    error_message = response.text.lower()

                if "already exists" in error_message:
                    print("[CATALOGUE] Device already registered; continuing without retry")
                    return True

            print(f"[CATALOGUE] Registration failed - status={response.status_code}, response={response.text}")

        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_task(self):
        while self.running and not self.register_device():
            print(f"[CATALOGUE] Retrying registration in {self.registration_retry_delay} seconds...")
            time.sleep(self.registration_retry_delay)

    # =========================================================================
    # MQTT Callbacks
    # =========================================================================
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.command_topic, qos=2)
            print(f"[MQTT] Connected to {self.broker}:{self.port}")
            print(f"[MQTT] Subscribed to {self.command_topic}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            print(f"[MQTT] Command received on {message.topic}: {payload}")
            self.simulator.handle_command(payload)
        except json.JSONDecodeError:
            print(f"[MQTT] Invalid command JSON on topic {message.topic}")
        except Exception as error:
            print(f"[MQTT] Command processing error: {error}")

    # =========================================================================
    # Worker Loops
    # =========================================================================
    def mqtt_loop(self):
        while self.running:
            try:
                self.mqtt_client.connect(self.broker, self.port, keepalive=60)
                self.mqtt_client.loop_forever()
            except Exception as error:
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {error}")
                time.sleep(5)

    def publish_loop(self):
        while self.running:
            if self.mqtt_connected:
                try:
                    data = self.simulator.collect_data()
                    info = self.mqtt_client.publish(
                        self.sensor_topic,
                        json.dumps(data),
                        qos=2
                    )
                    info.wait_for_publish()

                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        print(f"[MQTT] Published to {self.sensor_topic}: {data}")
                    else:
                        print(f"[MQTT] Sensor publish failed with rc={info.rc}")
                except Exception as error:
                    print(f"[MQTT] Publish error: {error}")
            else:
                print("[MQTT] Cannot publish sensor data because MQTT is not connected")

            time.sleep(self.publish_interval)

    # =========================================================================
    # Run & Stop
    # =========================================================================
    def run(self):
        print("[START] Raspberry Pi sensor node started")
        print(f"[INFO] Config File: {self.config_file}")
        print(f"[INFO] Device ID: {self.id}")
        print(f"[INFO] Catalogue URL: {self.catalog_url}")
        print(f"[INFO] MQTT Broker: {self.broker}:{self.port}")

        threading.Thread(target=self.registration_task, daemon=True, name="registration-thread").start()
        threading.Thread(target=self.mqtt_loop, daemon=True, name="mqtt-thread").start()
        threading.Thread(target=self.publish_loop, daemon=True, name="publish-thread").start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        print("\n[STOP] Stopping Raspberry Pi sensor node")
        self.running = False
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    node = SensorNode(config_file="config.json")
    node.run()