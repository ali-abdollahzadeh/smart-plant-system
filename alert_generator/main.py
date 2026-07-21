import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import cherrypy
import paho.mqtt.client as mqtt
import requests

class AlertRuleEngine:
    """Generate alerts when sensor values are outside configured limits."""

    def __init__(self, thresholds):
        self.thresholds = thresholds

    def now_utc_iso(self):
        return (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
        )

    def create_alert(
        self,
        device_id,
        alert_type,
        value,
        threshold
    ):
        return {
            "device_id": device_id,
            "alert": alert_type,
            "value": value,
            "threshold": threshold,
            "timestamp": self.now_utc_iso()
        }

    def check_sensor(
        self,
        sensor_data,
        sensor_type
    ):
        device_id = sensor_data.get("device_id")
        value = sensor_data.get(sensor_type)
        limits = self.thresholds.get(sensor_type, {})

        if value is None:
            return []

        try:
            numeric_value = float(value)
            minimum = float(limits["min"])
            maximum = float(limits["max"])
        except (TypeError, ValueError, KeyError):
            print(
                f"[RULES] Invalid configuration or value "
                f"for {sensor_type}: {value}"
            )
            return []

        if numeric_value < minimum:
            return [
                self.create_alert(
                    device_id=device_id,
                    alert_type=f"{sensor_type}_low",
                    value=numeric_value,
                    threshold=minimum
                )
            ]

        if numeric_value > maximum:
            return [
                self.create_alert(
                    device_id=device_id,
                    alert_type=f"{sensor_type}_high",
                    value=numeric_value,
                    threshold=maximum
                )
            ]

        return []

    def generate_alerts(self, sensor_data):
        alerts = []

        for sensor_type in (
            "temperature",
            "soil_moisture",
            "humidity"
        ):
            alerts.extend(
                self.check_sensor(
                    sensor_data,
                    sensor_type
                )
            )

        return alerts


class AlertGeneratorService(object):
    exposed = True

    def __init__(self):
        # --------------------------------------------------
        # Runtime configuration
        # --------------------------------------------------
        self.service_id = os.environ.get(
            "SERVICE_ID",
            "alert-generator"
        )
        self.service_name = os.environ.get(
            "SERVICE_NAME",
            "Alert Generator"
        )
        self.service_type = os.environ.get(
            "SERVICE_TYPE",
            "alert_generator"
        )
        self.service_host = os.environ.get(
            "SERVICE_HOST",
            "0.0.0.0"
        )
        self.service_port = int(
            os.environ.get("SERVICE_PORT", 8091)
        )
        self.catalog_url = os.environ.get(
            "CATALOG_URL",
            "http://catalogue:8000"
        )
        self.registration_retry_delay = int(
            os.environ.get("REGISTRATION_RETRY_DELAY", 5)
        )

        self.mqtt_broker = os.environ.get(
            "MQTT_BROKER",
            "mosquitto"
        )
        self.mqtt_port = int(
            os.environ.get("MQTT_PORT", 1883)
        )

        self.thingspeak_adapter_url = os.environ.get(
            "THINGSPEAK_ADAPTER_URL",
            "http://thingspeak-adapter:8080"
        )
        self.config_file = os.environ.get(
            "CONFIG_FILE",
            "/app/config.json"
        )

        # --------------------------------------------------
        # Load configuration
        # --------------------------------------------------
        with open(self.config_file, "r", encoding="utf-8") as file:
            self.config = json.load(file)

        # --------------------------------------------------
        # Shared service state
        # --------------------------------------------------
        self.lock = threading.RLock()
        self.latest_data: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []

        # Rule engine remains a separate application module.
        self.rule_engine = AlertRuleEngine(
            self.config["thresholds"]
        )

        # --------------------------------------------------
        # MQTT client
        # --------------------------------------------------
        self.mqtt_connected = False

        self.mqtt_client = mqtt.Client(
            client_id=self.service_id,
            clean_session=True
        )

        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
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

    def alert_topic_base(self):
        return self.config["mqtt"]["alert_topic_base"]

    def alert_topic(self, device_id):
        return f"{self.alert_topic_base()}/{device_id}"

    def default_report_results(self):
        return self.config["report"]["default_results"]

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def registration_payload(self):
        return {
            "id": self.service_id,
            "name": self.service_name,
            "type": self.service_type,
            "endpoint": (
                f"http://{self.service_id}:{self.service_port}"
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
                        "[CATALOGUE] Service already registered; "
                        "information refreshed"
                    )
                else:
                    print(
                        f"[CATALOGUE] Service registered: {payload}"
                    )

                return True

            print(
                "[CATALOGUE] Registration failed: "
                f"{response.status_code} {response.text}"
            )

        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_startup_task(self):
        # Retry only until registration succeeds.
        while not self.register_service():
            print(
                "[CATALOGUE] Retrying registration in "
                f"{self.registration_retry_delay} seconds..."
            )
            time.sleep(self.registration_retry_delay)

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
                f"[MQTT] Connected to "
                f"{self.mqtt_broker}:{self.mqtt_port}"
            )
            print(
                f"[MQTT] Subscribed to "
                f"{self.sensor_topic()}"
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
            payload = message.payload.decode("utf-8")
            sensor_data = json.loads(payload)

            sensor_data["_mqtt_topic"] = message.topic
            sensor_data["_received_at"] = self.now_utc_iso()

            device_id = sensor_data.get("device_id")

            if not device_id:
                print("[MQTT] Missing device_id in payload")
                return

            with self.lock:
                self.latest_data[device_id] = sensor_data

            print(
                f"[MQTT] Received from {message.topic}: "
                f"{sensor_data}"
            )

            generated_alerts = (
                self.rule_engine.generate_alerts(sensor_data)
            )

            for alert in generated_alerts:
                self.publish_alert(alert)

        except json.JSONDecodeError:
            print("[MQTT] Invalid JSON payload received")

        except Exception as error:
            print(f"[MQTT] Processing error: {error}")

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
                print(f"[MQTT] Connection error: {error}")
                time.sleep(5)

    # --------------------------------------------------
    # Alert storage and publication
    # --------------------------------------------------
    def add_alert(self, alert):
        with self.lock:
            self.alerts.append(alert)

            if len(self.alerts) > 200:
                self.alerts = self.alerts[-200:]

    def publish_alert(self, alert):
        device_id = alert.get("device_id")

        if not device_id:
            print("[MQTT] Alert missing device_id")
            return

        if not self.mqtt_connected:
            print(
                "[MQTT] Cannot publish alert because "
                "MQTT is not connected"
            )
            return

        topic = self.alert_topic(device_id)

        try:
            information = self.mqtt_client.publish(
                topic,
                json.dumps(alert),
                qos=2
            )

            information.wait_for_publish()

            if information.rc == mqtt.MQTT_ERR_SUCCESS:
                self.add_alert(alert)
                print(
                    f"[MQTT] Published alert to {topic}: "
                    f"{alert}"
                )
            else:
                print(
                    f"[MQTT] Alert publish failed with "
                    f"rc={information.rc}"
                )

        except Exception as error:
            print(f"[MQTT] Publish alert error: {error}")

    # --------------------------------------------------
    # Report generation
    # --------------------------------------------------
    def get_history(self, device_id, results):
        response = requests.get(
            f"{self.thingspeak_adapter_url}/history",
            params={
                "device_id": device_id,
                "results": results
            },
            timeout=10
        )

        response.raise_for_status()
        return response.json()

    def average(self, values):
        if not values:
            return 0.0

        return round(sum(values) / len(values), 2)

    def generate_report(
        self,
        device_id,
        results=None
    ):
        if results is None:
            results = self.default_report_results()

        with self.lock:
            latest_data = self.latest_data.get(device_id)

            if latest_data is not None:
                latest_data = dict(latest_data)

        if latest_data is None:
            raise ValueError(
                f"Device '{device_id}' not found in local state"
            )

        history_data = self.get_history(
            device_id,
            results
        )
        feeds = history_data.get("feeds", [])

        temperatures = []
        soil_moistures = []
        humidities = []

        for feed in feeds:
            if feed.get("field1") not in (None, ""):
                temperatures.append(
                    float(feed["field1"])
                )

            if feed.get("field2") not in (None, ""):
                soil_moistures.append(
                    float(feed["field2"])
                )

            if feed.get("field3") not in (None, ""):
                humidities.append(
                    float(feed["field3"])
                )

        return {
            "device_id": device_id,
            "latest_data": latest_data,
            "history_count": len(feeds),
            "averages": {
                "temperature": (
                    self.average(temperatures)
                ),
                "soil_moisture": (
                    self.average(soil_moistures)
                ),
                "humidity": (
                    self.average(humidities)
                )
            },
            "message": "Report generated successfully"
        }

    # --------------------------------------------------
    # REST API
    # --------------------------------------------------
    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
        # GET /
        if len(path) == 0:
            return {
                "message": "Alert Generator is running",
                "endpoints": {
                    "health": "/health",
                    "devices": "/devices",
                    "device_by_id": (
                        "/devices/<device_id>"
                    ),
                    "alerts": "/alerts",
                    "report": (
                        "/report?device_id=raspi-01"
                    )
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
                "status": "ok",
                "timestamp": self.now_utc_iso(),
                "service_id": self.service_id,
                "mqtt_connected": self.mqtt_connected
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
                    device = self.latest_data.get(device_id)

                    if device is None:
                        raise cherrypy.HTTPError(
                            404,
                            f"Device '{device_id}' not found"
                        )

                    return dict(device)

                devices = dict(self.latest_data)

            return {
                "count": len(devices),
                "devices": devices
            }

        # GET /alerts
        if resource == "alerts":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid alerts path"
                )

            with self.lock:
                alerts = list(self.alerts)

            return {
                "count": len(alerts),
                "alerts": alerts
            }

        # GET /report?device_id=raspi-01&results=20
        if resource == "report":
            if len(path) != 1:
                raise cherrypy.HTTPError(
                    404,
                    "Invalid report path"
                )

            device_id = query.get("device_id")
            results = query.get("results")

            if not device_id:
                raise cherrypy.HTTPError(
                    400,
                    "device_id is required"
                )

            try:
                if results is not None:
                    results = int(results)

                    if results <= 0:
                        raise ValueError(
                            "results must be greater than zero"
                        )

                return self.generate_report(
                    device_id=device_id,
                    results=results
                )

            except ValueError as error:
                message = str(error)

                if "not found" in message:
                    raise cherrypy.HTTPError(
                        404,
                        message
                    )

                raise cherrypy.HTTPError(
                    400,
                    message
                )

            except requests.RequestException as error:
                raise cherrypy.HTTPError(
                    500,
                    "Failed to fetch history from "
                    f"ThingSpeak Adapter: {error}"
                )

            except Exception as error:
                raise cherrypy.HTTPError(
                    500,
                    f"Report generation error: {error}"
                )

        raise cherrypy.HTTPError(
            404,
            "Endpoint not found"
        )

    # --------------------------------------------------
    # Service lifecycle
    # --------------------------------------------------
    def start(self):
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

    def stop(self):
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    service = AlertGeneratorService()

    print("[START] Alert Generator starting...")
    print(f"[INFO] Service ID: {service.service_id}")
    print(
        f"[INFO] MQTT Broker: "
        f"{service.mqtt_broker}:{service.mqtt_port}"
    )
    print(
        f"[INFO] Sensor Topic: "
        f"{service.sensor_topic()}"
    )

    service.start()

    configuration = {
        "/": {
            "request.dispatch":
                cherrypy.dispatch.MethodDispatcher(),
            "tools.sessions.on": True
        }
    }

    cherrypy.tree.mount(
        service,
        "/",
        configuration
    )

    cherrypy.config.update({
        "server.socket_host": service.service_host,
        "server.socket_port": service.service_port,
        "log.screen": True
    })

    cherrypy.engine.subscribe(
        "stop",
        service.stop
    )

    cherrypy.engine.start()
    cherrypy.engine.block()