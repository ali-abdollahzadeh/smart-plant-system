import copy
import json
import os
import threading
import time
from datetime import datetime, timezone

import cherrypy
import paho.mqtt.client as mqtt
import requests


class AnalyticsControlService(object):
    exposed = True

    def __init__(self):
        # --------------------------------------------------
        # Runtime configuration
        # --------------------------------------------------
        self.service_id = os.environ.get(
            "SERVICE_ID",
            "analytics-control"
        )
        self.service_name = os.environ.get(
            "SERVICE_NAME",
            "Analytics Control Service"
        )
        self.service_type = os.environ.get(
            "SERVICE_TYPE",
            "analytics_control"
        )
        self.service_host = os.environ.get(
            "SERVICE_HOST",
            "0.0.0.0"
        )
        self.service_port = int(
            os.environ.get("SERVICE_PORT", 8090)
        )

        self.catalog_url = os.environ.get(
            "CATALOG_URL",
            "http://catalogue:8000"
        ).rstrip("/")

        self.registration_retry_delay = int(
            os.environ.get(
                "REGISTRATION_RETRY_DELAY",
                5
            )
        )

        self.threshold_refresh_interval = int(
            os.environ.get(
                "THRESHOLD_REFRESH_INTERVAL",
                60
            )
        )

        self.mqtt_broker = os.environ.get(
            "MQTT_BROKER",
            "mosquitto"
        )
        self.mqtt_port = int(
            os.environ.get("MQTT_PORT", 1883)
        )

        self.config_file = os.environ.get(
            "CONFIG_FILE",
            "/app/config.json"
        )

        # --------------------------------------------------
        # Load local configuration
        # Only MQTT topics and commands are stored locally.
        # Thresholds are loaded from Catalogue.
        # --------------------------------------------------
        with open(
            self.config_file,
            "r",
            encoding="utf-8"
        ) as file:
            self.config = json.load(file)

        self.validate_local_config()

        # --------------------------------------------------
        # Shared service state
        # --------------------------------------------------
        self.lock = threading.RLock()

        self.latest_data = {}
        self.command_history = []
        self.last_command_by_device = {}

        self.thresholds = None
        self.thresholds_updated_at = None
        self.thresholds_ready = threading.Event()

        # --------------------------------------------------
        # MQTT client
        # --------------------------------------------------
        self.mqtt_connected = False

        self.mqtt_client = mqtt.Client(
            client_id=self.service_id,
            clean_session=True
        )

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = (
            self.on_mqtt_disconnect
        )
        self.mqtt_client.on_message = self.on_mqtt_message

    # --------------------------------------------------
    # General helpers
    # --------------------------------------------------
    def now_utc_iso(self):
        return (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
        )

    def sensor_topic(self):
        return self.config["mqtt"]["sensor_topic"]

    def command_topic_base(self):
        return self.config["mqtt"][
            "command_topic_base"
        ]

    def command_topic(self, device_id):
        return (
            f"{self.command_topic_base()}/{device_id}"
        )

    def validate_local_config(self):
        mqtt_config = self.config.get("mqtt")
        commands = self.config.get("commands")

        if not isinstance(mqtt_config, dict):
            raise ValueError(
                "Missing 'mqtt' section in config.json"
            )

        if not mqtt_config.get("sensor_topic"):
            raise ValueError(
                "Missing mqtt.sensor_topic in config.json"
            )

        if not mqtt_config.get("command_topic_base"):
            raise ValueError(
                "Missing mqtt.command_topic_base "
                "in config.json"
            )

        if not isinstance(commands, dict):
            raise ValueError(
                "Missing 'commands' section in config.json"
            )

        required_commands = [
            "temperature_low",
            "temperature_high",
            "temperature_normal",
            "soil_moisture_low",
            "soil_moisture_high",
            "soil_moisture_normal",
            "humidity_low",
            "humidity_high",
            "humidity_normal"
        ]

        missing_commands = [
            name
            for name in required_commands
            if not commands.get(name)
        ]

        if missing_commands:
            raise ValueError(
                "Missing commands in config.json: "
                + ", ".join(missing_commands)
            )

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def registration_payload(self):
        return {
            "id": self.service_id,
            "name": self.service_name,
            "type": self.service_type,
            "endpoint": (
                f"http://{self.service_id}:"
                f"{self.service_port}"
            ),
            "status": "active"
        }

    def register_service(self):
        payload = self.registration_payload()

        try:
            response = requests.post(
                f"{self.catalog_url}/services",
                json=payload,
                timeout=5
            )

            if response.status_code in (200, 201):
                try:
                    result = response.json()
                    action = result.get("action")
                except ValueError:
                    action = None

                if action == "updated":
                    print(
                        "[CATALOGUE] Service already "
                        "registered; information refreshed"
                    )
                else:
                    print(
                        "[CATALOGUE] Service registered: "
                        f"{payload}"
                    )

                return True

            print(
                "[CATALOGUE] Registration failed: "
                f"{response.status_code} "
                f"{response.text}"
            )

        except requests.RequestException as error:
            print(
                "[CATALOGUE] Registration error: "
                f"{error}"
            )

        return False

    def registration_startup_task(self):
        while not self.register_service():
            print(
                "[CATALOGUE] Retrying registration in "
                f"{self.registration_retry_delay} "
                "seconds..."
            )
            time.sleep(
                self.registration_retry_delay
            )

    # --------------------------------------------------
    # Threshold configuration from Catalogue
    # --------------------------------------------------
    def extract_thresholds_from_response(
        self,
        response_data
    ):
        if not isinstance(response_data, dict):
            raise ValueError(
                "Catalogue config response must be "
                "a JSON object"
            )

        # Supports both possible response forms:
        # 1. GET /config -> {"thresholds": {...}}
        # 2. GET /config -> {"config": {"thresholds": {...}}}
        if isinstance(
            response_data.get("config"),
            dict
        ):
            config_data = response_data["config"]
        else:
            config_data = response_data

        thresholds = config_data.get("thresholds")

        if not isinstance(thresholds, dict):
            raise ValueError(
                "Catalogue config does not contain "
                "'thresholds'"
            )

        return thresholds

    def validate_thresholds(self, thresholds):
        required_sensors = [
            "temperature",
            "soil_moisture",
            "humidity"
        ]

        validated = {}

        for sensor_name in required_sensors:
            limits = thresholds.get(sensor_name)

            if not isinstance(limits, dict):
                raise ValueError(
                    f"Missing thresholds for "
                    f"'{sensor_name}'"
                )

            try:
                threshold_min = float(limits["min"])
                threshold_max = float(limits["max"])
            except (KeyError, TypeError, ValueError):
                raise ValueError(
                    f"Invalid min/max thresholds for "
                    f"'{sensor_name}'"
                )

            if threshold_min > threshold_max:
                raise ValueError(
                    f"Minimum threshold is greater "
                    f"than maximum for '{sensor_name}'"
                )

            validated[sensor_name] = {
                "min": threshold_min,
                "max": threshold_max
            }

        return validated

    def load_thresholds_from_catalogue(self):
        try:
            response = requests.get(
                f"{self.catalog_url}/config",
                timeout=5
            )
            response.raise_for_status()

            response_data = response.json()

            thresholds = (
                self.extract_thresholds_from_response(
                    response_data
                )
            )

            validated_thresholds = (
                self.validate_thresholds(
                    thresholds
                )
            )

            with self.lock:
                changed = (
                    self.thresholds
                    != validated_thresholds
                )

                self.thresholds = (
                    validated_thresholds
                )
                self.thresholds_updated_at = (
                    self.now_utc_iso()
                )
                self.thresholds_ready.set()

                latest_snapshot = [
                    dict(sensor_data)
                    for sensor_data
                    in self.latest_data.values()
                ]

            if changed:
                print(
                    "[CATALOGUE] Thresholds loaded "
                    f"from {self.catalog_url}/config: "
                    f"{validated_thresholds}"
                )

                # Re-evaluate the latest known values when
                # the central thresholds change.
                for sensor_data in latest_snapshot:
                    self.evaluate_controls(sensor_data)
            else:
                print(
                    "[CATALOGUE] Thresholds refreshed; "
                    "no changes detected"
                )

            return True

        except requests.RequestException as error:
            print(
                "[CATALOGUE] Threshold request error: "
                f"{error}"
            )

        except (
            ValueError,
            json.JSONDecodeError
        ) as error:
            print(
                "[CATALOGUE] Invalid threshold "
                f"configuration: {error}"
            )

        return False

    def threshold_refresh_task(self):
        while True:
            loaded = (
                self.load_thresholds_from_catalogue()
            )

            if not loaded:
                print(
                    "[CATALOGUE] Thresholds are not "
                    "available. Analytics will not "
                    "evaluate sensor rules until they "
                    "are loaded."
                )
                delay = (
                    self.registration_retry_delay
                )
            else:
                delay = (
                    self.threshold_refresh_interval
                )

            time.sleep(delay)

    def get_thresholds_snapshot(self):
        with self.lock:
            if self.thresholds is None:
                return None

            return copy.deepcopy(self.thresholds)

    # --------------------------------------------------
    # MQTT callbacks
    # --------------------------------------------------
    def on_mqtt_connect(
        self,
        client,
        userdata,
        flags,
        rc
    ):
        if rc == 0:
            self.mqtt_connected = True

            client.subscribe(
                self.sensor_topic(),
                qos=2
            )

            print(
                "[MQTT] Connected to "
                f"{self.mqtt_broker}:"
                f"{self.mqtt_port}"
            )
            print(
                "[MQTT] Subscribed to "
                f"{self.sensor_topic()}"
            )

        else:
            self.mqtt_connected = False
            print(
                "[MQTT] Connection failed "
                f"with rc={rc}"
            )

    def on_mqtt_disconnect(
        self,
        client,
        userdata,
        rc
    ):
        self.mqtt_connected = False
        print(
            f"[MQTT] Disconnected with rc={rc}"
        )

    def on_mqtt_message(
        self,
        client,
        userdata,
        message
    ):
        try:
            payload = message.payload.decode(
                "utf-8"
            )
            sensor_data = json.loads(payload)

            sensor_data["_mqtt_topic"] = (
                message.topic
            )
            sensor_data["_received_at"] = (
                self.now_utc_iso()
            )

            device_id = sensor_data.get(
                "device_id"
            )

            if not device_id:
                print(
                    "[MQTT] Missing device_id "
                    "in payload"
                )
                return

            with self.lock:
                self.latest_data[device_id] = (
                    sensor_data
                )

            print(
                "[MQTT] Received from "
                f"{message.topic}: {sensor_data}"
            )

            self.evaluate_controls(sensor_data)

        except json.JSONDecodeError:
            print(
                "[MQTT] Invalid JSON payload "
                "received"
            )

        except Exception as error:
            print(
                "[MQTT] Processing error: "
                f"{error}"
            )

    def mqtt_loop(self):
        while True:
            try:
                self.mqtt_client.connect(
                    self.mqtt_broker,
                    self.mqtt_port,
                    keepalive=60
                )

                self.mqtt_client.loop_forever()

            except Exception as error:
                self.mqtt_connected = False
                print(
                    "[MQTT] Connection error: "
                    f"{error}"
                )
                time.sleep(5)

    # --------------------------------------------------
    # Command history helpers
    # --------------------------------------------------
    def get_last_command(
        self,
        device_id,
        sensor_type
    ):
        with self.lock:
            return (
                self.last_command_by_device
                .get(device_id, {})
                .get(sensor_type)
            )

    def save_command(
        self,
        device_id,
        sensor_type,
        payload
    ):
        with self.lock:
            self.command_history.append(payload)

            if len(self.command_history) > 200:
                self.command_history = (
                    self.command_history[-200:]
                )

            if (
                device_id
                not in self.last_command_by_device
            ):
                self.last_command_by_device[
                    device_id
                ] = {}

            self.last_command_by_device[
                device_id
            ][sensor_type] = payload["command"]

    # --------------------------------------------------
    # MQTT command publishing
    # --------------------------------------------------
    def publish_command(
        self,
        device_id,
        command,
        reason,
        sensor_type
    ):
        last_command = self.get_last_command(
            device_id,
            sensor_type
        )

        if last_command == command:
            return

        if not self.mqtt_connected:
            print(
                "[MQTT] Cannot publish command "
                "because MQTT is not connected"
            )
            return

        payload = {
            "device_id": device_id,
            "command": command,
            "reason": reason,
            "sensor_type": sensor_type,
            "timestamp": self.now_utc_iso()
        }

        topic = self.command_topic(device_id)

        try:
            information = (
                self.mqtt_client.publish(
                    topic,
                    json.dumps(payload),
                    qos=2
                )
            )

            information.wait_for_publish()

            if (
                information.rc
                == mqtt.MQTT_ERR_SUCCESS
            ):
                self.save_command(
                    device_id,
                    sensor_type,
                    payload
                )

                print(
                    "[MQTT] Published command "
                    f"to {topic}: {payload}"
                )

            else:
                print(
                    "[MQTT] Publish failed "
                    f"with rc={information.rc}"
                )

        except Exception as error:
            print(
                "[MQTT] Publish command error: "
                f"{error}"
            )

    # --------------------------------------------------
    # Rule engine
    # --------------------------------------------------
    def evaluate_sensor_rule(
        self,
        device_id,
        sensor_type,
        value,
        threshold_min,
        threshold_max,
        low_command,
        high_command,
        normal_command,
        low_reason,
        high_reason,
        normal_reason
    ):
        if value is None:
            return

        try:
            value = float(value)
        except (TypeError, ValueError):
            print(
                f"[RULES] Invalid {sensor_type} "
                f"value for {device_id}: {value}"
            )
            return

        if value < threshold_min:
            self.publish_command(
                device_id,
                low_command,
                low_reason,
                sensor_type
            )

        elif value > threshold_max:
            self.publish_command(
                device_id,
                high_command,
                high_reason,
                sensor_type
            )

        else:
            self.publish_command(
                device_id,
                normal_command,
                normal_reason,
                sensor_type
            )

    def evaluate_controls(self, sensor_data):
        thresholds = self.get_thresholds_snapshot()

        if thresholds is None:
            print(
                "[RULES] Thresholds have not been "
                "loaded from Catalogue yet; "
                "sensor evaluation skipped"
            )
            return

        device_id = sensor_data["device_id"]
        commands = self.config["commands"]

        self.evaluate_sensor_rule(
            device_id,
            "temperature",
            sensor_data.get("temperature"),
            thresholds["temperature"]["min"],
            thresholds["temperature"]["max"],
            commands["temperature_low"],
            commands["temperature_high"],
            commands["temperature_normal"],
            "temperature_below_min_threshold",
            "temperature_above_max_threshold",
            "temperature_back_to_normal_range"
        )

        self.evaluate_sensor_rule(
            device_id,
            "soil_moisture",
            sensor_data.get("soil_moisture"),
            thresholds["soil_moisture"]["min"],
            thresholds["soil_moisture"]["max"],
            commands["soil_moisture_low"],
            commands["soil_moisture_high"],
            commands["soil_moisture_normal"],
            "soil_moisture_below_min_threshold",
            "soil_moisture_above_max_threshold",
            "soil_moisture_back_to_normal_range"
        )

        self.evaluate_sensor_rule(
            device_id,
            "humidity",
            sensor_data.get("humidity"),
            thresholds["humidity"]["min"],
            thresholds["humidity"]["max"],
            commands["humidity_low"],
            commands["humidity_high"],
            commands["humidity_normal"],
            "humidity_below_min_threshold",
            "humidity_above_max_threshold",
            "humidity_back_to_normal_range"
        )

    # --------------------------------------------------
    # REST API
    # --------------------------------------------------
    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
        # GET /
        if len(path) == 0:
            return {
                "message": (
                    "Analytics Control Service "
                    "is running"
                ),
                "endpoints": {
                    "health": "/health",
                    "devices": "/devices",
                    "device_by_id": (
                        "/devices/<device_id>"
                    ),
                    "commands": "/commands",
                    "rules": "/rules",
                    "summary": "/summary"
                }
            }

        resource = path[0]

        # GET /health
        if resource == "health":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid health path"
                )

            return {
                "status": (
                    "ok"
                    if self.thresholds_ready.is_set()
                    else "degraded"
                ),
                "timestamp": self.now_utc_iso(),
                "service_id": self.service_id,
                "mqtt_connected": (
                    self.mqtt_connected
                ),
                "thresholds_loaded": (
                    self.thresholds_ready.is_set()
                ),
                "thresholds_source": (
                    f"{self.catalog_url}/config"
                ),
                "thresholds_updated_at": (
                    self.thresholds_updated_at
                )
            }

        # GET /devices
        # GET /devices/<device_id>
        if resource == "devices":
            if len(path) > 2:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid devices path"
                )

            with self.lock:
                if len(path) == 2:
                    device_id = path[1]
                    device = self.latest_data.get(
                        device_id
                    )

                    if device is None:
                        raise cherrypy.HTTPError(
                            404,
                            f"Device '{device_id}' "
                            "not found"
                        )

                    return dict(device)

                devices = copy.deepcopy(
                    self.latest_data
                )

            return {
                "count": len(devices),
                "devices": devices
            }

        # GET /commands
        if resource == "commands":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid commands path"
                )

            with self.lock:
                commands = copy.deepcopy(
                    self.command_history
                )

            return {
                "count": len(commands),
                "commands": commands
            }

        # GET /rules
        if resource == "rules":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid rules path"
                )

            return {
                "thresholds": (
                    self.get_thresholds_snapshot()
                ),
                "thresholds_source": (
                    f"{self.catalog_url}/config"
                ),
                "thresholds_updated_at": (
                    self.thresholds_updated_at
                ),
                "commands": copy.deepcopy(
                    self.config["commands"]
                )
            }

        # GET /summary
        if resource == "summary":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid summary path"
                )

            with self.lock:
                devices_count = len(
                    self.latest_data
                )
                commands_count = len(
                    self.command_history
                )

            return {
                "devices_count": devices_count,
                "commands_count": commands_count,
                "mqtt_connected": (
                    self.mqtt_connected
                ),
                "thresholds_loaded": (
                    self.thresholds_ready.is_set()
                ),
                "thresholds_updated_at": (
                    self.thresholds_updated_at
                ),
                "last_update": self.now_utc_iso()
            }

        raise cherrypy.HTTPError(
            404,
            "Endpoint not found"
        )

    # --------------------------------------------------
    # Start and stop
    # --------------------------------------------------
    def start(self):
        threading.Thread(
            target=self.registration_startup_task,
            daemon=True,
            name=(
                "catalogue-registration-thread"
            )
        ).start()

        threading.Thread(
            target=self.threshold_refresh_task,
            daemon=True,
            name=(
                "catalogue-threshold-thread"
            )
        ).start()

        threading.Thread(
            target=self.mqtt_loop,
            daemon=True,
            name="mqtt-thread"
        ).start()

    def stop(self):
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    analytics = AnalyticsControlService()

    print(
        "[START] Analytics Control Service "
        "starting..."
    )
    print(
        f"[INFO] Service ID: "
        f"{analytics.service_id}"
    )
    print(
        "[INFO] MQTT Broker: "
        f"{analytics.mqtt_broker}:"
        f"{analytics.mqtt_port}"
    )
    print(
        "[INFO] Sensor Topic: "
        f"{analytics.sensor_topic()}"
    )
    print(
        "[INFO] Threshold source: "
        f"{analytics.catalog_url}/config"
    )

    analytics.start()

    configuration = {
        "/": {
            "request.dispatch": (
                cherrypy.dispatch.MethodDispatcher()
            ),
            "tools.sessions.on": True
        }
    }

    cherrypy.tree.mount(
        analytics,
        "/",
        configuration
    )

    cherrypy.config.update({
        "server.socket_host": (
            analytics.service_host
        ),
        "server.socket_port": (
            analytics.service_port
        ),
        "log.screen": True
    })

    cherrypy.engine.subscribe(
        "stop",
        analytics.stop
    )

    cherrypy.engine.start()
    cherrypy.engine.block()