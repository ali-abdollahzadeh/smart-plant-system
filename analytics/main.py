import copy
import json
import math
import os
import threading
import time
from datetime import datetime, timezone

import cherrypy
import paho.mqtt.client as mqtt
import requests


class AnalyticsService:
    exposed = True

    SENSOR_FIELDS = {
        "temperature": "field1",
        "soil_moisture": "field2",
        "humidity": "field3",
    }

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

    def __init__(self, config):
        self.config = config

        self.service = config["service"]
        self.catalogue = config["catalogue"]
        self.thingspeak = config["thingspeak_adapter"]
        self.mqtt_config = config["mqtt"]
        self.analytics_config = config["analytics"]
        self.commands = config["commands"]
        self.llm_config = config.get("llm", {})

        self.lock = threading.RLock()

        self.latest_data = {}
        self.command_history = []
        self.analysis_history = []
        self.last_command_by_device = {}
        self.last_historical_analysis = {}

        self.thresholds = None
        self.thresholds_updated_at = None
        self.thresholds_ready = threading.Event()

        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(
            client_id=self.mqtt_config["client_id"],
            clean_session=True,
        )
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.validate_config()

    # --------------------------------------------------
    # Configuration / helpers
    # --------------------------------------------------
    def validate_config(self):
        required_topics = (
            "sensor_topic",
            "command_topic_base",
            "analysis_topic_base",
        )

        for key in required_topics:
            if not self.mqtt_config.get(key):
                raise ValueError(f"Missing MQTT configuration: {key}")

        for rule in self.SENSOR_RULES.values():
            for command_key in rule.values():
                if command_key not in self.commands:
                    raise ValueError(
                        f"Missing command configuration: {command_key}"
                    )

    def now_utc_iso(self):
        return (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
        )

    def sensor_topic(self):
        return self.mqtt_config["sensor_topic"]

    def command_topic(self, device_id):
        return (
            f'{self.mqtt_config["command_topic_base"]}/'
            f"{device_id}"
        )

    def analysis_topic(self, device_id):
        return (
            f'{self.mqtt_config["analysis_topic_base"]}/'
            f"{device_id}"
        )

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def registration_payload(self):
        return {
            "id": self.service["id"],
            "name": self.service["name"],
            "type": self.service["type"],
            "endpoint": (
                f'http://{self.service["id"]}:'
                f'{self.service["port"]}'
            ),
            "status": "active",
        }

    def register_service(self):
        try:
            response = requests.post(
                f'{self.catalogue["url"]}/services',
                json=self.registration_payload(),
                timeout=5,
            )

            if response.status_code in (200, 201):
                print("[CATALOGUE] Analytics registered")
                return True

            print(
                "[CATALOGUE] Registration failed: "
                f"{response.status_code} {response.text}"
            )

        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_startup_task(self):
        retry_delay = self.service["registration_retry_delay"]

        while not self.register_service():
            print(
                "[CATALOGUE] Retrying registration in "
                f"{retry_delay} seconds..."
            )
            time.sleep(retry_delay)

    # --------------------------------------------------
    # Catalogue thresholds
    # --------------------------------------------------
    def extract_thresholds(self, data):
        config = data.get("config", data)
        thresholds = config.get("thresholds")

        if not isinstance(thresholds, dict):
            raise ValueError("Catalogue has no thresholds object")

        validated = {}

        for sensor_type in self.SENSOR_RULES:
            limits = thresholds.get(sensor_type)

            if not isinstance(limits, dict):
                raise ValueError(
                    f"Missing thresholds for {sensor_type}"
                )

            minimum = float(limits["min"])
            maximum = float(limits["max"])

            if minimum > maximum:
                raise ValueError(
                    f"Minimum exceeds maximum for {sensor_type}"
                )

            validated[sensor_type] = {
                "min": minimum,
                "max": maximum,
            }

        return validated

    def load_thresholds_from_catalogue(self):
        try:
            response = requests.get(
                f'{self.catalogue["url"]}/config',
                timeout=5,
            )
            response.raise_for_status()

            new_thresholds = self.extract_thresholds(
                response.json()
            )

            with self.lock:
                changed = new_thresholds != self.thresholds
                self.thresholds = new_thresholds
                self.thresholds_updated_at = self.now_utc_iso()
                self.thresholds_ready.set()

            if changed:
                print(
                    "[CATALOGUE] Thresholds loaded/updated: "
                    f"{new_thresholds}"
                )

            return True

        except (requests.RequestException, ValueError) as error:
            print(f"[CATALOGUE] Threshold load error: {error}")
            return False

    def threshold_refresh_task(self):
        refresh_interval = self.catalogue[
            "threshold_refresh_interval"
        ]
        retry_delay = self.service[
            "registration_retry_delay"
        ]

        while True:
            loaded = self.load_thresholds_from_catalogue()
            time.sleep(
                refresh_interval if loaded else retry_delay
            )

    def get_thresholds_snapshot(self):
        with self.lock:
            return copy.deepcopy(self.thresholds)

    # --------------------------------------------------
    # MQTT
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
                qos=self.mqtt_config["qos"],
            )

            print(
                "[MQTT] Connected to "
                f'{self.mqtt_config["broker"]}:'
                f'{self.mqtt_config["port"]}'
            )
            print(
                f"[MQTT] Subscribed to {self.sensor_topic()}"
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
    ):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(
        self,
        client,
        userdata,
        message
    ):
        try:
            sensor_data = json.loads(
                message.payload.decode("utf-8")
            )

            device_id = sensor_data.get("device_id")

            if not device_id:
                print("[MQTT] Missing device_id")
                return

            sensor_data["_mqtt_topic"] = message.topic
            sensor_data["_received_at"] = self.now_utc_iso()

            with self.lock:
                self.latest_data[device_id] = sensor_data

            print(
                f"[MQTT] Received from {message.topic}: "
                f"{sensor_data}"
            )

            # Real-time threshold control.
            self.evaluate_controls(sensor_data)

            # Historical analytics is intentionally separate from
            # simple threshold control.
            self.maybe_run_historical_analysis(device_id)

        except json.JSONDecodeError:
            print("[MQTT] Invalid sensor JSON")

        except Exception as error:
            print(
                f"[MQTT] Sensor processing error: {error}"
            )

    def mqtt_loop(self):
        while True:
            try:
                self.mqtt_client.connect(
                    self.mqtt_config["broker"],
                    self.mqtt_config["port"],
                    keepalive=60,
                )
                self.mqtt_client.loop_forever()

            except Exception as error:
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {error}")
                time.sleep(5)

    def publish_json(self, topic, payload):
        if not self.mqtt_connected:
            return False

        try:
            info = self.mqtt_client.publish(
                topic,
                json.dumps(payload),
                qos=self.mqtt_config["qos"],
            )
            return info.rc == mqtt.MQTT_ERR_SUCCESS

        except Exception as error:
            print(f"[MQTT] Publish error: {error}")
            return False

    # --------------------------------------------------
    # Real-time threshold control
    # --------------------------------------------------
    def publish_command(
        self,
        device_id,
        sensor_type,
        command,
        reason
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

        if self.publish_json(
            self.command_topic(device_id),
            payload
        ):
            with self.lock:
                self.command_history.append(payload)
                self.command_history = (
                    self.command_history[-200:]
                )
                self.last_command_by_device.setdefault(
                    device_id,
                    {}
                )[sensor_type] = command

            print(
                "[MQTT] Published command: "
                f"{payload}"
            )

    def evaluate_sensor(
        self,
        device_id,
        sensor_type,
        raw_value,
        limits
    ):
        if raw_value is None:
            return

        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return

        if value < limits["min"]:
            state = "low"
        elif value > limits["max"]:
            state = "high"
        else:
            state = "normal"


        ###### NEW ORRER FINDED: publish analysis event for Alert Generator ######
        analysis_event = {
            "device_id": device_id,
            "sensor_type": sensor_type,
            "state": state,
            "value": value,
            "threshold_min": limits["min"],
            "threshold_max": limits["max"],
            "timestamp": self.now_utc_iso()
        }

        self.publish_json(
            self.analysis_topic(device_id),
            analysis_event
        )


        command_key = self.SENSOR_RULES[
            sensor_type
        ][f"{state}_command"]

        self.publish_command(
            device_id,
            sensor_type,
            self.commands[command_key],
            f"{sensor_type}_{state}",
        )


    def evaluate_controls(self, sensor_data):
        thresholds = self.get_thresholds_snapshot()

        if thresholds is None:
            return

        device_id = sensor_data["device_id"]

        for sensor_type in self.SENSOR_RULES:
            self.evaluate_sensor(
                device_id,
                sensor_type,
                sensor_data.get(sensor_type),
                thresholds[sensor_type],
            )

    # --------------------------------------------------
    # Historical data analytics
    # --------------------------------------------------
    def get_history(self, device_id):
        response = requests.get(
            (
                f'{self.thingspeak["url"]}'
                f'{self.thingspeak["history_endpoint"]}'
            ),
            params={
                "device_id": device_id,
                "results": self.thingspeak[
                    "history_results"
                ],
            },
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("feeds", [])

    def extract_history_values(
        self,
        feeds,
        field_name
    ):
        values = []

        for feed in feeds:
            raw_value = feed.get(field_name)

            if raw_value in (None, ""):
                continue

            try:
                values.append(float(raw_value))
            except (TypeError, ValueError):
                continue

        return values

    def mean(self, values):
        return sum(values) / len(values)

    def standard_deviation(self, values):
        if len(values) < 2:
            return 0.0

        average = self.mean(values)
        variance = sum(
            (value - average) ** 2
            for value in values
        ) / len(values)

        return math.sqrt(variance)

    def linear_trend_slope(self, values):
        """
        Simple least-squares slope over sample index.
        Positive slope -> increasing trend.
        Negative slope -> decreasing trend.
        """
        size = len(values)

        if size < 2:
            return 0.0

        x_mean = (size - 1) / 2
        y_mean = self.mean(values)

        numerator = sum(
            (index - x_mean) * (value - y_mean)
            for index, value in enumerate(values)
        )
        denominator = sum(
            (index - x_mean) ** 2
            for index in range(size)
        )

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def trend_label(self, slope):
        epsilon = self.analytics_config[
            "trend_epsilon"
        ]

        if slope > epsilon:
            return "increasing"

        if slope < -epsilon:
            return "decreasing"

        return "stable"

    def historical_sensor_analysis(
        self,
        sensor_type,
        values
    ):
        minimum_points = self.analytics_config[
            "minimum_history_points"
        ]

        if len(values) < minimum_points:
            return {
                "history_points": len(values),
                "status": "insufficient_history",
            }

        average = self.mean(values)
        std_dev = self.standard_deviation(values)
        slope = self.linear_trend_slope(values)
        latest = values[-1]

        z_score = 0.0
        if std_dev > 0:
            z_score = (latest - average) / std_dev

        return {
            "history_points": len(values),
            "latest": round(latest, 3),
            "mean": round(average, 3),
            "standard_deviation": round(std_dev, 3),
            "minimum": round(min(values), 3),
            "maximum": round(max(values), 3),
            "trend_slope": round(slope, 4),
            "trend": self.trend_label(slope),
            "z_score": round(z_score, 3),
            "anomaly": (
                abs(z_score)
                >= self.analytics_config[
                    "zscore_warning"
                ]
            ),
        }

    def run_historical_analysis(self, device_id):
        try:
            feeds = self.get_history(device_id)

            sensors = {}

            for sensor_type, field_name in (
                self.SENSOR_FIELDS.items()
            ):
                values = self.extract_history_values(
                    feeds,
                    field_name
                )
                sensors[sensor_type] = (
                    self.historical_sensor_analysis(
                        sensor_type,
                        values
                    )
                )

            payload = {
                "device_id": device_id,
                "analysis_type": "historical_statistics",
                "history_source": "thingspeak",
                "requested_results": self.thingspeak[
                    "history_results"
                ],
                "sensors": sensors,
                "timestamp": self.now_utc_iso(),
            }

            # Add LLM interpretation after the historical statistics.
            payload["llm"] = self.interpret_with_llm(payload)

            with self.lock:
                self.analysis_history.append(payload)
                self.analysis_history = (
                    self.analysis_history[-500:]
                )
                self.last_historical_analysis[
                    device_id
                ] = time.time()

            self.publish_json(
                self.analysis_topic(device_id),
                payload
            )

            print(
                "[ANALYTICS] Historical analysis "
                f"for {device_id}: {payload}"
            )

            return payload

        except requests.RequestException as error:
            print(
                "[ANALYTICS] Historical data error "
                f"for {device_id}: {error}"
            )
            return None

    def maybe_run_historical_analysis(
        self,
        device_id
    ):
        interval = self.thingspeak[
            "analysis_interval"
        ]

        with self.lock:
            last_time = self.last_historical_analysis.get(
                device_id,
                0
            )

        if time.time() - last_time < interval:
            return

        # Run outside the MQTT callback so REST/history access
        # does not block MQTT message processing.
        threading.Thread(
            target=self.run_historical_analysis,
            args=(device_id,),
            daemon=True,
            name=f"history-{device_id}",
        ).start()

        with self.lock:
            # Reserve the interval immediately to avoid spawning
            # several history threads for consecutive messages.
            self.last_historical_analysis[
                device_id
            ] = time.time()

    # --------------------------------------------------
    # LLM interpretation
    # --------------------------------------------------
    def interpret_with_llm(self, historical_payload):
        """
        Interpret the deterministic historical statistics with
        the local Ollama model. The LLM never replaces the
        numerical analytics.
        """
        if not self.llm_config.get("enabled", False):
            return {
                "enabled": False,
                "interpretation": None
            }

        llm_input = {
            "device_id": historical_payload["device_id"],
            "analysis_type": historical_payload["analysis_type"],
            "history_source": historical_payload["history_source"],
            "sensors": historical_payload["sensors"]
        }

        prompt = (
            self.llm_config["instructions"]
            + "\n\nHistorical analytics:\n"
            + json.dumps(llm_input, indent=2)
        )

        try:
            response = requests.post(
                f'{self.llm_config["url"]}/api/generate',
                json={
                    "model": self.llm_config["model"],
                    "prompt": prompt,
                    "stream": False
                },
                timeout=self.llm_config["timeout"]
            )
            response.raise_for_status()

            result = response.json()
            interpretation = result.get("response", "").strip()

            return {
                "enabled": True,
                "provider": "ollama",
                "model": self.llm_config["model"],
                "interpretation": interpretation,
                "timestamp": self.now_utc_iso()
            }

        except (
            requests.RequestException,
            ValueError,
            json.JSONDecodeError
        ) as error:
            print(f"[LLM] Ollama interpretation error: {error}")

            return {
                "enabled": True,
                "provider": "ollama",
                "model": self.llm_config.get("model"),
                "interpretation": None,
                "error": str(error),
                "timestamp": self.now_utc_iso()
            }

    # --------------------------------------------------
    # REST
    # --------------------------------------------------
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
                    "analysis_device": "/analysis/<device_id>",
                    "rules": "/rules",
                },
            }

        resource = path[0]

        if resource == "health":
            return {
                "status": "ok",
                "mqtt_connected": self.mqtt_connected,
                "thresholds_loaded": (
                    self.thresholds_ready.is_set()
                ),
                "timestamp": self.now_utc_iso(),
            }

        if resource == "devices":
            with self.lock:
                devices = copy.deepcopy(
                    self.latest_data
                )

            return {
                "count": len(devices),
                "devices": devices,
            }

        if resource == "commands":
            with self.lock:
                commands = copy.deepcopy(
                    self.command_history
                )

            return {
                "count": len(commands),
                "commands": commands,
            }

        if resource == "analysis":
            with self.lock:
                analysis = copy.deepcopy(
                    self.analysis_history
                )

            if len(path) == 2:
                device_id = path[1]
                analysis = [
                    item
                    for item in analysis
                    if item.get("device_id")
                    == device_id
                ]

            return {
                "count": len(analysis),
                "analysis": analysis,
            }

        if resource == "rules":
            return {
                "thresholds": (
                    self.get_thresholds_snapshot()
                ),
                "commands": copy.deepcopy(
                    self.commands
                ),
            }

        raise cherrypy.HTTPError(
            404,
            "Endpoint not found"
        )

    # --------------------------------------------------
    # Lifecycle
    # --------------------------------------------------
    def start(self):
        threading.Thread(
            target=self.registration_startup_task,
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
    config_file = os.environ.get(
        "CONFIG_FILE",
        "/app/config.json"
    )

    with open(
        config_file,
        "r",
        encoding="utf-8"
    ) as file:
        config = json.load(file)

    service = AnalyticsService(config)
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

    cherrypy.config.update({
        "server.socket_host":
            config["service"]["host"],
        "server.socket_port":
            config["service"]["port"],
        "log.screen": True,
    })

    cherrypy.engine.subscribe(
        "stop",
        service.stop
    )
    cherrypy.engine.start()
    cherrypy.engine.block()