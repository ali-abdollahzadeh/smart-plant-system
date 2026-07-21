import json
import os
import threading
import time
from typing import Any, Dict

import paho.mqtt.client as mqtt
import requests

from simulator import PlantSimulator


def load_config() -> Dict[str, Any]:
    return {
        "device_id": os.environ.get(
            "DEVICE_ID",
            "raspi-01"
        ),
        "device_name": os.environ.get(
            "DEVICE_NAME",
            "Plant Sensor Node 1"
        ),
        "device_type": os.environ.get(
            "DEVICE_TYPE",
            "sensor_node"
        ),
        "catalog_url": os.environ.get(
            "CATALOG_URL",
            "http://catalogue:8000"
        ),
        "mqtt_broker": os.environ.get(
            "MQTT_BROKER",
            "mosquitto"
        ),
        "mqtt_port": int(
            os.environ.get("MQTT_PORT", 1883)
        ),
        "mqtt_topic_base": os.environ.get(
            "MQTT_TOPIC_BASE",
            "smartplant/sensors"
        ),
        "mqtt_command_topic_base": os.environ.get(
            "MQTT_COMMAND_TOPIC_BASE",
            "smartplant/commands"
        ),
        "publish_interval": int(
            os.environ.get("PUBLISH_INTERVAL", 10)
        ),
        "registration_retry_delay": int(
            os.environ.get(
                "REGISTRATION_RETRY_DELAY",
                5
            )
        ),
    }


class SensorNode:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.simulator = PlantSimulator(
            self.config["device_id"]
        )

        self.mqtt_connected = False
        self.stop_event = threading.Event()

        self.mqtt_client = mqtt.Client(
            client_id=self.config["device_id"],
            clean_session=True
        )

        self.mqtt_client.on_connect = (
            self.on_mqtt_connect
        )
        self.mqtt_client.on_disconnect = (
            self.on_mqtt_disconnect
        )
        self.mqtt_client.on_message = (
            self.on_mqtt_message
        )

    # --------------------------------------------------
    # Topic helpers
    # --------------------------------------------------
    def sensor_topic(self) -> str:
        return (
            f"{self.config['mqtt_topic_base']}/"
            f"{self.config['device_id']}"
        )

    def command_topic(self) -> str:
        return (
            f"{self.config['mqtt_command_topic_base']}/"
            f"{self.config['device_id']}"
        )

    # --------------------------------------------------
    # Sensor data collection
    # --------------------------------------------------
    def collect_data(self) -> Dict[str, Any]:
        return self.simulator.collect_data()

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def build_registration_payload(
        self
    ) -> Dict[str, Any]:
        return {
            "id": self.config["device_id"],
            "name": self.config["device_name"],
            "type": self.config["device_type"],
            "mqtt_topic": self.sensor_topic(),
            "command_topic": self.command_topic(),
            "status": "active"
        }

    def register_device(self) -> bool:
        payload = self.build_registration_payload()

        try:
            response = requests.post(
                f"{self.config['catalog_url']}/devices",
                json=payload,
                timeout=10
            )

            if response.status_code in (200, 201):
                try:
                    result = response.json()
                    action = result.get("action")
                except ValueError:
                    action = None

                if action == "updated":
                    print(
                        "[CATALOGUE] Device already "
                        "registered; information refreshed"
                    )
                else:
                    print(
                        "[CATALOGUE] Registration "
                        f"successful: {payload}"
                    )

                return True

            # The current Catalogue returns HTTP 409 when the
            # device is already registered. This is not a startup
            # failure, so treat it as a successful registration
            # and stop the retry loop.
            if response.status_code == 409:
                try:
                    result = response.json()
                    error_message = str(
                        result.get("error", "")
                    ).lower()
                except ValueError:
                    error_message = response.text.lower()

                if "already exists" in error_message:
                    print(
                        "[CATALOGUE] Device already registered; "
                        "continuing without retry"
                    )
                    return True

            print(
                "[CATALOGUE] Registration failed - "
                f"status={response.status_code}, "
                f"response={response.text}"
            )

        except requests.RequestException as error:
            print(
                f"[CATALOGUE] Registration error: "
                f"{error}"
            )

        return False

    def registration_startup_task(self) -> None:
        retry_delay = self.config[
            "registration_retry_delay"
        ]

        while (
            not self.stop_event.is_set()
            and not self.register_device()
        ):
            print(
                "[CATALOGUE] Retrying registration "
                f"in {retry_delay} seconds..."
            )
            self.stop_event.wait(retry_delay)

    # --------------------------------------------------
    # MQTT callbacks
    # --------------------------------------------------
    def on_mqtt_connect(
        self,
        client,
        userdata,
        flags,
        rc
    ) -> None:
        if rc == 0:
            self.mqtt_connected = True

            client.subscribe(
                self.command_topic(),
                qos=2
            )

            print(
                f"[MQTT] Connected to "
                f"{self.config['mqtt_broker']}:"
                f"{self.config['mqtt_port']}"
            )
            print(
                f"[MQTT] Subscribed to "
                f"{self.command_topic()}"
            )

        else:
            self.mqtt_connected = False
            print(
                f"[MQTT] Connection failed with rc={rc}"
            )

    def on_mqtt_disconnect(
        self,
        client,
        userdata,
        rc
    ) -> None:
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(
        self,
        client,
        userdata,
        message
    ) -> None:
        try:
            payload = message.payload.decode("utf-8")
            payload_dict = json.loads(payload)

            print(
                f"[MQTT] Command received on "
                f"{message.topic}: {payload_dict}"
            )

            self.simulator.handle_command(
                payload_dict
            )

        except json.JSONDecodeError:
            print(
                "[MQTT] Invalid command JSON on "
                f"topic {message.topic}"
            )

        except Exception as error:
            print(
                f"[MQTT] Command processing error: "
                f"{error}"
            )

    def mqtt_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.mqtt_client.connect(
                    self.config["mqtt_broker"],
                    self.config["mqtt_port"],
                    keepalive=60
                )

                self.mqtt_client.loop_forever()

            except Exception as error:
                self.mqtt_connected = False
                print(
                    f"[MQTT] Connection error: {error}"
                )
                self.stop_event.wait(5)

    # --------------------------------------------------
    # Sensor publishing
    # --------------------------------------------------
    def publish_data(
        self,
        data: Dict[str, Any]
    ) -> None:
        if not self.mqtt_connected:
            print(
                "[MQTT] Cannot publish sensor data "
                "because MQTT is not connected"
            )
            return

        topic = self.sensor_topic()

        try:
            information = self.mqtt_client.publish(
                topic,
                json.dumps(data),
                qos=2
            )

            information.wait_for_publish()

            if information.rc == mqtt.MQTT_ERR_SUCCESS:
                print(
                    f"[MQTT] Published to {topic}: "
                    f"{data}"
                )
            else:
                print(
                    "[MQTT] Sensor publish failed "
                    f"with rc={information.rc}"
                )

        except Exception as error:
            print(f"[MQTT] Publish error: {error}")

    def publish_loop(self) -> None:
        interval = self.config["publish_interval"]

        while not self.stop_event.is_set():
            try:
                sensor_data = self.collect_data()
                self.publish_data(sensor_data)

            except Exception as error:
                print(
                    f"[SENSOR] Data collection error: "
                    f"{error}"
                )

            self.stop_event.wait(interval)

    # --------------------------------------------------
    # Run and stop
    # --------------------------------------------------
    def run(self) -> None:
        print(
            "[START] Raspberry Pi sensor node started"
        )
        print(
            f"[INFO] Device ID: "
            f"{self.config['device_id']}"
        )
        print(
            f"[INFO] Device Name: "
            f"{self.config['device_name']}"
        )
        print(
            f"[INFO] Device Type: "
            f"{self.config['device_type']}"
        )
        print(
            f"[INFO] Catalogue URL: "
            f"{self.config['catalog_url']}"
        )
        print(
            f"[INFO] MQTT Broker: "
            f"{self.config['mqtt_broker']}:"
            f"{self.config['mqtt_port']}"
        )
        print(
            f"[INFO] Sensor Topic: "
            f"{self.sensor_topic()}"
        )
        print(
            f"[INFO] Command Topic: "
            f"{self.command_topic()}"
        )
        print(
            f"[INFO] Publish Interval: "
            f"{self.config['publish_interval']} "
            "seconds"
        )

        threading.Thread(
            target=self.registration_startup_task,
            daemon=True,
            name="catalogue-registration-thread"
        ).start()

        threading.Thread(
            target=self.mqtt_loop,
            daemon=True,
            name="mqtt-thread"
        ).start()

        threading.Thread(
            target=self.publish_loop,
            daemon=True,
            name="sensor-publish-thread"
        ).start()

        try:
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        print(
            "[STOP] Stopping Raspberry Pi "
            "sensor node"
        )

        self.stop_event.set()

        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    node = SensorNode(load_config())
    node.run()