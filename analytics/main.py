import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import cherrypy
import requests
import paho.mqtt.client as mqtt


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_runtime_config() -> Dict[str, Any]:
    return {
        "service_id": os.environ.get("SERVICE_ID", "analytics-control"),
        "service_name": os.environ.get("SERVICE_NAME", "Analytics Control Service"),
        "service_type": os.environ.get("SERVICE_TYPE", "analytics_control"),
        "service_host": os.environ.get("SERVICE_HOST", "0.0.0.0"),
        "service_port": int(os.environ.get("SERVICE_PORT", 8090)),
        "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
        "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
        "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
        "config_file": os.environ.get("CONFIG_FILE", "/app/config.json"),
    }


RUNTIME = load_runtime_config()
APP_CONFIG = load_json(RUNTIME["config_file"])


# --------------------------------------------------
# Shared state
# --------------------------------------------------
class SharedState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest_data: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []
        self.last_registration_time: float = 0.0


STATE = SharedState()


# --------------------------------------------------
# Analytics / Control Service
# --------------------------------------------------
class AnalyticsControlService:
    def __init__(self) -> None:
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # -----------------------------
    # Config helpers
    # -----------------------------
    def sensor_topic(self) -> str:
        return APP_CONFIG["mqtt"]["sensor_topic"]

    def alert_topic_base(self) -> str:
        return APP_CONFIG["mqtt"]["alert_topic_base"]

    def command_topic_base(self) -> str:
        return APP_CONFIG["mqtt"]["command_topic_base"]

    def thresholds(self) -> Dict[str, Any]:
        return APP_CONFIG["thresholds"]

    def commands(self) -> Dict[str, str]:
        return APP_CONFIG["commands"]

    # -----------------------------
    # Catalogue registration
    # -----------------------------
    def register_service(self) -> None:
        payload = {
            "id": RUNTIME["service_id"],
            "name": RUNTIME["service_name"],
            "type": RUNTIME["service_type"],
            "endpoint": f"http://{RUNTIME['service_id']}:{RUNTIME['service_port']}",
            "status": "active"
        }

        try:
            response = requests.post(
                f"{RUNTIME['catalog_url']}/services",
                json=payload,
                timeout=5
            )

            if response.status_code in (200, 201):
                with STATE.lock:
                    STATE.last_registration_time = time.time()
                print(f"[CATALOGUE] Service registered: {payload}")
            else:
                print(f"[CATALOGUE] Registration failed: {response.status_code} {response.text}")

        except requests.RequestException as e:
            print(f"[CATALOGUE] Registration error: {e}")

    def registration_loop(self) -> None:
        while True:
            self.register_service()
            time.sleep(RUNTIME["register_interval"])
    # -----------------------------
    # MQTT handling
    # -----------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = self.sensor_topic()
            client.subscribe(topic)
            print(f"[MQTT] Connected to {RUNTIME['mqtt_broker']}:{RUNTIME['mqtt_port']}")
            print(f"[MQTT] Subscribed to {topic}")
        else:
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            sensor_data = json.loads(msg.payload.decode("utf-8"))
            sensor_data["_mqtt_topic"] = msg.topic
            sensor_data["_received_at"] = now_utc_iso()

            device_id = sensor_data.get("device_id")
            if not device_id:
                print("[MQTT] Missing device_id in payload")
                return

            with STATE.lock:
                STATE.latest_data[device_id] = sensor_data

            print(f"[MQTT] Received from {msg.topic}: {sensor_data}")
            self.evaluate_rules(sensor_data)

        except json.JSONDecodeError:
            print("[MQTT] Invalid JSON payload received")
        except Exception as e:
            print(f"[MQTT] Processing error: {e}")

    def mqtt_loop(self) -> None:
        while True:
            try:
                self.mqtt_client.connect(
                    RUNTIME["mqtt_broker"],
                    RUNTIME["mqtt_port"],
                    keepalive=60
                )
                self.mqtt_client.loop_forever()
            except Exception as e:
                print(f"[MQTT] Connection error: {e}")
                print("[MQTT] Retrying in 5 seconds...")
                time.sleep(5)

    # -----------------------------
    # Alerts and commands
    # -----------------------------
    def publish_json(self, topic: str, payload: Dict[str, Any]) -> None:
        try:
            self.mqtt_client.publish(topic, json.dumps(payload))
            print(f"[MQTT] Published to {topic}: {payload}")
        except Exception as e:
            print(f"[MQTT] Publish error on {topic}: {e}")

    def add_alert(self, alert: Dict[str, Any]) -> None:
        with STATE.lock:
            STATE.alerts.append(alert)

            # keep only the latest 100 alerts
            if len(STATE.alerts) > 100:
                STATE.alerts = STATE.alerts[-100:]

    def publish_alert(self, device_id: str, alert_type: str, value: Any, threshold: Any) -> None:
        alert = {
            "device_id": device_id,
            "alert": alert_type,
            "value": value,
            "threshold": threshold,
            "timestamp": now_utc_iso()
        }

        topic = f"{self.alert_topic_base()}/{device_id}"
        self.publish_json(topic, alert)
        self.add_alert(alert)

    def publish_command(self, device_id: str, command: str, reason: str) -> None:
        payload = {
            "device_id": device_id,
            "command": command,
            "reason": reason,
            "timestamp": now_utc_iso()
        }

        topic = f"{self.command_topic_base()}/{device_id}"
        self.publish_json(topic, payload)

    # -----------------------------
    # Rule engine
    # -----------------------------
    def evaluate_rules(self, sensor_data: Dict[str, Any]) -> None:
        device_id = sensor_data["device_id"]
        thresholds = self.thresholds()
        commands = self.commands()

        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")
# Soil moisture control
        if soil_moisture is not None:
            if soil_moisture < thresholds["soil_moisture_min"]:
                self.publish_alert(
                    device_id=device_id,
                    alert_type="low_soil_moisture",
                    value=soil_moisture,
                    threshold=thresholds["soil_moisture_min"]
                )
                self.publish_command(
                    device_id=device_id,
                    command=commands["low_soil_moisture"],
                    reason="soil_moisture_below_threshold"
                )
            else:
                self.publish_command(
                    device_id=device_id,
                    command=commands["normal_soil_moisture"],
                    reason="soil_moisture_back_to_normal"
                )

        # Temperature control
        if temperature is not None:
            if temperature > thresholds["temperature_max"]:
                self.publish_alert(
                    device_id=device_id,
                    alert_type="high_temperature",
                    value=temperature,
                    threshold=thresholds["temperature_max"]
                )
                self.publish_command(
                    device_id=device_id,
                    command=commands["high_temperature"],
                    reason="temperature_above_threshold"
                )
            else:
                self.publish_command(
                    device_id=device_id,
                    command=commands["normal_temperature"],
                    reason="temperature_back_to_normal"
                )

        # Humidity control
        if humidity is not None:
            if humidity < thresholds["humidity_min"]:
                self.publish_alert(
                    device_id=device_id,
                    alert_type="low_humidity",
                    value=humidity,
                    threshold=thresholds["humidity_min"]
                )
                self.publish_command(
                    device_id=device_id,
                    command=commands["low_humidity"],
                    reason="humidity_below_threshold"
                )

            elif humidity > thresholds["humidity_max"]:
                self.publish_alert(
                    device_id=device_id,
                    alert_type="high_humidity",
                    value=humidity,
                    threshold=thresholds["humidity_max"]
                )
                self.publish_command(
                    device_id=device_id,
                    command=commands["high_humidity"],
                    reason="humidity_above_threshold"
                )

    # -----------------------------
    # Summary helpers
    # -----------------------------
    def get_system_summary(self) -> Dict[str, Any]:
        with STATE.lock:
            return {
                "devices_count": len(STATE.latest_data),
                "alerts_count": len(STATE.alerts),
                "last_update": now_utc_iso()
            }


SERVICE = AnalyticsControlService()


# --------------------------------------------------
# REST API
# --------------------------------------------------
@cherrypy.expose
class RootAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "message": "Analytics Control Service is running",
            "endpoints": {
                "health": "/health",
                "devices": "/devices",
                "device_by_id": "/devices/<device_id>",
                "alerts": "/alerts",
                "rules": "/rules",
                "summary": "/summary"
            }
        }


@cherrypy.expose
class HealthAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "status": "ok",
            "timestamp": now_utc_iso(),
            "service_id": RUNTIME["service_id"]
        }
@cherrypy.expose
class DevicesAPI:
    @cherrypy.tools.json_out()
    def GET(self, device_id=None):
        with STATE.lock:
            if device_id:
                device = STATE.latest_data.get(device_id)
                if not device:
                    cherrypy.response.status = 404
                    return {"error": f"Device '{device_id}' not found"}
                return device

            return {
                "count": len(STATE.latest_data),
                "devices": STATE.latest_data
            }


@cherrypy.expose
class AlertsAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        with STATE.lock:
            return {
                "count": len(STATE.alerts),
                "alerts": STATE.alerts
            }


@cherrypy.expose
class RulesAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "thresholds": APP_CONFIG["thresholds"],
            "commands": APP_CONFIG["commands"]
        }


@cherrypy.expose
class SummaryAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        return SERVICE.get_system_summary()


def start_background_threads():
    threading.Thread(target=SERVICE.mqtt_loop, daemon=True).start()
    threading.Thread(target=SERVICE.registration_loop, daemon=True).start()


if name == "__main__":
    print("[START] Analytics Control Service starting...")
    print(f"[INFO] Service ID: {RUNTIME['service_id']}")
    print(f"[INFO] MQTT Broker: {RUNTIME['mqtt_broker']}:{RUNTIME['mqtt_port']}")
    print(f"[INFO] Sensor Topic: {APP_CONFIG['mqtt']['sensor_topic']}")

    start_background_threads()

    app = RootAPI()
    app.health = HealthAPI()
    app.devices = DevicesAPI()
    app.alerts = AlertsAPI()
    app.rules = RulesAPI()
    app.summary = SummaryAPI()

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
    }

    cherrypy.config.update({
        "server.socket_host": RUNTIME["service_host"],
        "server.socket_port": RUNTIME["service_port"],
        "log.screen": True
    })

    cherrypy.quickstart(app, "/", conf)