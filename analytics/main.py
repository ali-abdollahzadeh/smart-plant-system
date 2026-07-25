import copy
import json
import os
import threading
import time
from datetime import datetime, timezone

import cherrypy
import paho.mqtt.client as mqtt
import requests


class AnalyticsControlService:
    exposed = True

    SENSOR_RULES = {
        "temperature": {
            "low_command": "temperature_low",
            "high_command": "temperature_high",
            "normal_command": "temperature_normal",
        },
        "soil_moisture": {
            "low_command": "soil_moisture_low",
            "high_command": "soil_moisture_high",
            "normal_command": "soil_moisture_normal",
        },
        "humidity": {
            "low_command": "humidity_low",
            "high_command": "humidity_high",
            "normal_command": "humidity_normal",
        },
    }

    def __init__(self):
        self.service_id = os.environ.get("SERVICE_ID", "analytics")
        self.service_name = os.environ.get("SERVICE_NAME", "Analytics Service")
        self.service_type = os.environ.get("SERVICE_TYPE", "analytics")
        self.service_host = os.environ.get("SERVICE_HOST", "0.0.0.0")
        self.service_port = int(os.environ.get("SERVICE_PORT", 8090))

        self.catalog_url = os.environ.get(
            "CATALOG_URL", "http://catalogue:8000"
        ).rstrip("/")
        self.registration_retry_delay = int(
            os.environ.get("REGISTRATION_RETRY_DELAY", 5)
        )
        self.threshold_refresh_interval = int(
            os.environ.get("THRESHOLD_REFRESH_INTERVAL", 60)
        )

        self.mqtt_broker = os.environ.get("MQTT_BROKER", "mosquitto")
        self.mqtt_port = int(os.environ.get("MQTT_PORT", 1883))
        self.config_file = os.environ.get("CONFIG_FILE", "/app/config.json")

        with open(self.config_file, "r", encoding="utf-8") as file:
            self.config = json.load(file)

        self._validate_local_config()

        self.lock = threading.RLock()
        self.latest_data = {}
        self.command_history = []
        self.analysis_history = []
        self.last_command_by_device = {}

        self.thresholds = None
        self.thresholds_updated_at = None
        self.thresholds_ready = threading.Event()

        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(
            client_id=self.service_id,
            clean_session=True,
        )
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

    def now_utc_iso(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def sensor_topic(self):
        return self.config["mqtt"]["sensor_topic"]

    def command_topic(self, device_id):
        return f'{self.config["mqtt"]["command_topic_base"]}/{device_id}'

    def analysis_topic(self, device_id):
        return f'{self.config["mqtt"]["analysis_topic_base"]}/{device_id}'

    def _validate_local_config(self):
        mqtt_config = self.config.get("mqtt", {})
        commands = self.config.get("commands", {})

        required_mqtt = (
            "sensor_topic",
            "command_topic_base",
            "analysis_topic_base",
        )
        missing_mqtt = [key for key in required_mqtt if not mqtt_config.get(key)]
        if missing_mqtt:
            raise ValueError(
                "Missing MQTT configuration: " + ", ".join(missing_mqtt)
            )

        required_commands = []
        for rule in self.SENSOR_RULES.values():
            required_commands.extend(rule.values())

        missing_commands = [
            key for key in required_commands if not commands.get(key)
        ]
        if missing_commands:
            raise ValueError(
                "Missing command configuration: "
                + ", ".join(missing_commands)
            )

    # ------------------------------------------------------------------
    # Catalogue
    # ------------------------------------------------------------------
    def registration_payload(self):
        return {
            "id": self.service_id,
            "name": self.service_name,
            "type": self.service_type,
            "endpoint": f"http://{self.service_id}:{self.service_port}",
            "status": "active",
        }

    def register_service(self):
        try:
            response = requests.post(
                f"{self.catalog_url}/services",
                json=self.registration_payload(),
                timeout=5,
            )
            if response.status_code in (200, 201):
                print("[CATALOGUE] Analytics service registered/refreshed")
                return True

            print(
                "[CATALOGUE] Registration failed: "
                f"{response.status_code} {response.text}"
            )
        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_task(self):
        while not self.register_service():
            time.sleep(self.registration_retry_delay)

    def _extract_thresholds(self, response_data):
        if not isinstance(response_data, dict):
            raise ValueError("Catalogue config response must be an object")

        config_data = response_data.get("config", response_data)
        thresholds = config_data.get("thresholds")

        if not isinstance(thresholds, dict):
            raise ValueError("Catalogue config has no thresholds object")

        validated = {}
        for sensor_name in self.SENSOR_RULES:
            limits = thresholds.get(sensor_name)
            if not isinstance(limits, dict):
                raise ValueError(f"Missing thresholds for {sensor_name}")

            try:
                threshold_min = float(limits["min"])
                threshold_max = float(limits["max"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(
                    f"Invalid thresholds for {sensor_name}"
                ) from error

            if threshold_min > threshold_max:
                raise ValueError(
                    f"Minimum exceeds maximum for {sensor_name}"
                )

            validated[sensor_name] = {
                "min": threshold_min,
                "max": threshold_max,
            }

        return validated

    def load_thresholds_from_catalogue(self):
        try:
            response = requests.get(
                f"{self.catalog_url}/config",
                timeout=5,
            )
            response.raise_for_status()
            new_thresholds = self._extract_thresholds(response.json())

            with self.lock:
                changed = new_thresholds != self.thresholds
                self.thresholds = new_thresholds
                self.thresholds_updated_at = self.now_utc_iso()
                self.thresholds_ready.set()
                latest_snapshot = [
                    copy.deepcopy(item)
                    for item in self.latest_data.values()
                ]

            if changed:
                print(
                    "[CATALOGUE] Thresholds loaded/updated: "
                    f"{new_thresholds}"
                )
                for sensor_data in latest_snapshot:
                    self.evaluate_controls(sensor_data)
            else:
                print("[CATALOGUE] Thresholds refreshed; no changes")

            return True

        except (requests.RequestException, ValueError, json.JSONDecodeError) as error:
            print(f"[CATALOGUE] Threshold load error: {error}")
            return False

    def threshold_refresh_task(self):
        while True:
            loaded = self.load_thresholds_from_catalogue()
            delay = (
                self.threshold_refresh_interval
                if loaded
                else self.registration_retry_delay
            )
            time.sleep(delay)

    def get_thresholds_snapshot(self):
        with self.lock:
            return copy.deepcopy(self.thresholds)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.sensor_topic(), qos=2)
            print(
                f"[MQTT] Connected to {self.mqtt_broker}:{self.mqtt_port}"
            )
            print(f"[MQTT] Subscribed to {self.sensor_topic()}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(self, client, userdata, message):
        try:
            sensor_data = json.loads(
                message.payload.decode("utf-8")
            )
            device_id = sensor_data.get("device_id")
            if not device_id:
                print("[MQTT] Missing device_id in sensor payload")
                return

            sensor_data["_mqtt_topic"] = message.topic
            sensor_data["_received_at"] = self.now_utc_iso()

            with self.lock:
                self.latest_data[device_id] = sensor_data

            print(
                f"[MQTT] Sensor data received from "
                f"{message.topic}: {sensor_data}"
            )
            self.evaluate_controls(sensor_data)

        except json.JSONDecodeError:
            print("[MQTT] Invalid sensor JSON")
        except Exception as error:
            print(f"[MQTT] Sensor processing error: {error}")

    def mqtt_loop(self):
        while True:
            try:
                self.mqtt_client.connect(
                    self.mqtt_broker,
                    self.mqtt_port,
                    keepalive=60,
                )
                self.mqtt_client.loop_forever()
            except Exception as error:
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {error}")
                time.sleep(5)

    def _publish_json(self, topic, payload):
        if not self.mqtt_connected:
            print(f"[MQTT] Cannot publish to {topic}: disconnected")
            return False

        try:
            info = self.mqtt_client.publish(
                topic,
                json.dumps(payload),
                qos=1,
            )

            # Never block the Paho MQTT callback/network thread here.
            return info.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as error:
            print(f"[MQTT] Publish error on {topic}: {error}")
            return False

    def publish_analysis(self, payload):
        topic = self.analysis_topic(payload["device_id"])

        if self._publish_json(topic, payload):
            with self.lock:
                self.analysis_history.append(payload)
                self.analysis_history = self.analysis_history[-500:]

            print(
                f"[MQTT] Published analysis to {topic}: {payload}"
            )

    def publish_command(
        self,
        device_id,
        sensor_type,
        command,
        reason,
    ):
        with self.lock:
            last_command = (
                self.last_command_by_device
                .get(device_id, {})
                .get(sensor_type)
            )

        if last_command == command:
            return

        payload = {
            "device_id": device_id,
            "sensor_type": sensor_type,
            "command": command,
            "reason": reason,
            "timestamp": self.now_utc_iso(),
        }
        topic = self.command_topic(device_id)

        if self._publish_json(topic, payload):
            with self.lock:
                self.command_history.append(payload)
                self.command_history = self.command_history[-200:]
                self.last_command_by_device.setdefault(
                    device_id, {}
                )[sensor_type] = command

            print(
                f"[MQTT] Published command to {topic}: {payload}"
            )

    # ------------------------------------------------------------------
    # Rule engine
    # ------------------------------------------------------------------
    def evaluate_sensor(
        self,
        device_id,
        sensor_type,
        raw_value,
        limits,
    ):
        if raw_value is None:
            return

        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            print(
                f"[RULES] Invalid {sensor_type} value "
                f"for {device_id}: {raw_value}"
            )
            return

        threshold_min = limits["min"]
        threshold_max = limits["max"]

        if value < threshold_min:
            state = "low"
        elif value > threshold_max:
            state = "high"
        else:
            state = "normal"

        command_key = self.SENSOR_RULES[sensor_type][
            f"{state}_command"
        ]
        command = self.config["commands"][command_key]
        reason = f"{sensor_type}_{state}"

        analysis_payload = {
            "device_id": device_id,
            "sensor_type": sensor_type,
            "value": value,
            "state": state,
            "threshold_min": threshold_min,
            "threshold_max": threshold_max,
            "reason": reason,
            "timestamp": self.now_utc_iso(),
        }

        # Alert Generator consumes this.
        self.publish_analysis(analysis_payload)

        # Raspberry Pi consumes this.
        self.publish_command(
            device_id,
            sensor_type,
            command,
            reason,
        )

    def evaluate_controls(self, sensor_data):
        thresholds = self.get_thresholds_snapshot()

        if thresholds is None:
            print(
                "[RULES] Thresholds are not loaded; "
                "evaluation skipped"
            )
            return

        device_id = sensor_data["device_id"]

        for sensor_type in self.SENSOR_RULES:
            self.evaluate_sensor(
                device_id,
                sensor_type,
                sensor_data.get(sensor_type),
                thresholds[sensor_type],
            )


    # ------------------------------------------------------------------
    # Catalogue device registry + live telemetry merge
    # ------------------------------------------------------------------
    def fetch_catalogue_devices(self):
        """
        Return all registered devices from Catalogue as a list.

        Supported Catalogue response forms:
        1. {"count": 4, "devices": [{...}, {...}]}
        2. {"devices": {"raspi-01": {...}, ...}}
        3. [{...}, {...}]
        """
        try:
            response = requests.get(
                f"{self.catalog_url}/devices",
                timeout=5,
            )
            response.raise_for_status()
            data = response.json()

        except requests.RequestException as error:
            raise cherrypy.HTTPError(
                503,
                f"Catalogue is unavailable: {error}",
            )

        except json.JSONDecodeError as error:
            raise cherrypy.HTTPError(
                502,
                f"Catalogue returned invalid JSON: {error}",
            )

        if isinstance(data, list):
            devices = data

        elif isinstance(data, dict):
            devices = data.get("devices", data)

            if isinstance(devices, dict):
                normalized = []

                for device_id, device_data in devices.items():
                    if isinstance(device_data, dict):
                        item = copy.deepcopy(device_data)
                        item.setdefault("id", device_id)
                    else:
                        item = {
                            "id": device_id,
                            "value": device_data,
                        }

                    normalized.append(item)

                devices = normalized

        else:
            devices = []

        if not isinstance(devices, list):
            raise cherrypy.HTTPError(
                502,
                "Catalogue returned an unsupported devices format",
            )

        return [
            copy.deepcopy(device)
            for device in devices
            if isinstance(device, dict)
            and (device.get("id") or device.get("device_id"))
        ]

    def merged_registered_devices(self):
        """
        Merge Catalogue registry information with telemetry that
        Analytics has received since its current start.
        """
        registered_devices = self.fetch_catalogue_devices()

        with self.lock:
            live_data = copy.deepcopy(self.latest_data)

        merged = {}

        for catalogue_device in registered_devices:
            device_id = (
                catalogue_device.get("id")
                or catalogue_device.get("device_id")
            )

            latest = live_data.get(device_id)
            merged[device_id] = {
                "id": device_id,
                "name": catalogue_device.get("name"),
                "type": catalogue_device.get("type"),
                "registered": True,
                "catalogue_status": catalogue_device.get("status"),
                "mqtt_topic": catalogue_device.get("mqtt_topic"),
                "command_topic": catalogue_device.get("command_topic"),
                "telemetry_received": latest is not None,
                "latest_data": latest,
                "last_seen": (
                    latest.get("_received_at")
                    if latest is not None
                    else None
                ),
            }

        # Keep unexpected publishers visible even when they are not
        # registered in Catalogue.
        for device_id, latest in live_data.items():
            if device_id not in merged:
                merged[device_id] = {
                    "id": device_id,
                    "name": None,
                    "type": None,
                    "registered": False,
                    "catalogue_status": None,
                    "mqtt_topic": None,
                    "command_topic": None,
                    "telemetry_received": True,
                    "latest_data": latest,
                    "last_seen": latest.get("_received_at"),
                }

        return merged

    # ------------------------------------------------------------------
    # REST API
    # ------------------------------------------------------------------
    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
        if not path:
            return {
                "message": "Analytics Service is running",
                "endpoints": {
                    "health": "/health",
                    "devices": "/devices",
                    "commands": "/commands",
                    "analysis": "/analysis",
                    "rules": "/rules",
                    "summary": "/summary",
                },
            }

        resource = path[0]

        if resource == "health":
            return {
                "status": (
                    "ok"
                    if self.thresholds_ready.is_set()
                    else "degraded"
                ),
                "service_id": self.service_id,
                "mqtt_connected": self.mqtt_connected,
                "thresholds_loaded": self.thresholds_ready.is_set(),
                "thresholds_updated_at": self.thresholds_updated_at,
                "timestamp": self.now_utc_iso(),
            }

        if resource == "devices":
            if len(path) > 2:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid devices path",
                )

            devices = self.merged_registered_devices()

            if len(path) == 2:
                device_id = path[1]
                device = devices.get(device_id)

                # A 404 now means the device is neither registered
                # in Catalogue nor observed by Analytics.
                if device is None:
                    raise cherrypy.HTTPError(
                        404,
                        f"Device '{device_id}' is not registered "
                        "and has not been observed",
                    )

                return device

            return {
                "count": len(devices),
                "registered_count": sum(
                    1
                    for device in devices.values()
                    if device["registered"]
                ),
                "telemetry_count": sum(
                    1
                    for device in devices.values()
                    if device["telemetry_received"]
                ),
                "devices": devices,
            }

        if resource == "commands":
            with self.lock:
                commands = copy.deepcopy(self.command_history)
            return {
                "count": len(commands),
                "commands": commands,
            }

        if resource == "analysis":
            if len(path) > 2:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid analysis path",
                )

            with self.lock:
                analysis = copy.deepcopy(self.analysis_history)

            # GET /analysis/<device_id>
            if len(path) == 2:
                device_id = path[1]
                device_analysis = [
                    item
                    for item in analysis
                    if item.get("device_id") == device_id
                ]

                if not device_analysis:
                    raise cherrypy.HTTPError(
                        404,
                        f"No analysis found for device '{device_id}'",
                    )

                return {
                    "device_id": device_id,
                    "count": len(device_analysis),
                    "analysis": device_analysis,
                }

            # GET /analysis
            return {
                "count": len(analysis),
                "analysis": analysis,
            }

        if resource == "rules":
            return {
                "thresholds": self.get_thresholds_snapshot(),
                "thresholds_source": f"{self.catalog_url}/config",
                "commands": copy.deepcopy(
                    self.config["commands"]
                ),
            }

        if resource == "summary":
            devices = self.merged_registered_devices()

            with self.lock:
                commands_count = len(self.command_history)
                analysis_count = len(self.analysis_history)

            return {
                "registered_devices_count": sum(
                    1
                    for device in devices.values()
                    if device["registered"]
                ),
                "telemetry_devices_count": sum(
                    1
                    for device in devices.values()
                    if device["telemetry_received"]
                ),
                "commands_count": commands_count,
                "analysis_count": analysis_count,
                "mqtt_connected": self.mqtt_connected,
                "thresholds_loaded": (
                    self.thresholds_ready.is_set()
                ),
                "last_update": self.now_utc_iso(),
            }

        raise cherrypy.HTTPError(404, "Endpoint not found")

    def start(self):
        threading.Thread(
            target=self.registration_task,
            daemon=True,
            name="registration-thread",
        ).start()
        threading.Thread(
            target=self.threshold_refresh_task,
            daemon=True,
            name="threshold-thread",
        ).start()
        threading.Thread(
            target=self.mqtt_loop,
            daemon=True,
            name="mqtt-thread",
        ).start()

    def stop(self):
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    service = AnalyticsControlService()

    print("[START] Analytics Service starting...")
    print(f"[INFO] Sensor topic: {service.sensor_topic()}")
    print(
        "[INFO] Analysis topic base: "
        f'{service.config["mqtt"]["analysis_topic_base"]}'
    )
    print(
        f"[INFO] Threshold source: "
        f"{service.catalog_url}/config"
    )

    service.start()

    cherrypy.tree.mount(
        service,
        "/",
        {
            "/": {
                "request.dispatch": (
                    cherrypy.dispatch.MethodDispatcher()
                ),
                "tools.sessions.on": True,
            }
        },
    )
    cherrypy.config.update(
        {
            "server.socket_host": service.service_host,
            "server.socket_port": service.service_port,
            "log.screen": True,
        }
    )
    cherrypy.engine.subscribe("stop", service.stop)
    cherrypy.engine.start()
    cherrypy.engine.block()