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
        self.command_history: List[Dict[str, Any]] = []
        self.last_command_by_device: Dict[str, Dict[str, str]] = {}
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
            self.evaluate_controls(sensor_data)

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
    # Command publishing
    # -----------------------------
    def publish_command(self, device_id: str, command: str, reason: str, sensor_type: str) -> None:
        with STATE.lock:
            last_for_device = STATE.last_command_by_device.get(device_id, {})
            last_command = last_for_device.get(sensor_type)

        # Avoid publishing the same command repeatedly
        if last_command == command:
            return

        payload = {
            "device_id": device_id,
            "command": command,
            "reason": reason,
            "sensor_type": sensor_type,
            "timestamp": now_utc_iso()
        }

        topic = f"{self.command_topic_base()}/{device_id}"

        try:
            self.mqtt_client.publish(topic, json.dumps(payload))
            print(f"[MQTT] Published command to {topic}: {payload}")

            with STATE.lock:
                STATE.command_history.append(payload)
                if len(STATE.command_history) > 200:
                    STATE.command_history = STATE.command_history[-200:]

                if device_id not in STATE.last_command_by_device:
                    STATE.last_command_by_device[device_id] = {}

                STATE.last_command_by_device[device_id][sensor_type] = command

        except Exception as e:
            print(f"[MQTT] Publish command error: {e}")

    # -----------------------------
    # Rule engine
    # -----------------------------
    def evaluate_controls(self, sensor_data: Dict[str, Any]) -> None:
        device_id = sensor_data["device_id"]
        thresholds = self.thresholds()
        commands = self.commands()

        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")

        # Temperature control
        if temperature is not None:
            temp_min = thresholds["temperature"]["min"]
            temp_max = thresholds["temperature"]["max"]

            if temperature < temp_min:
                self.publish_command(
                    device_id=device_id,
                    command=commands["temperature_low"],
                    reason="temperature_below_min_threshold",
                    sensor_type="temperature"
                )
            elif temperature > temp_max:
                self.publish_command(
                    device_id=device_id,
                    command=commands["temperature_high"],
                    reason="temperature_above_max_threshold",
                    sensor_type="temperature"
                )
            else:
                self.publish_command(
                    device_id=device_id,
                    command=commands["temperature_normal"],
                    reason="temperature_back_to_normal_range",
                    sensor_type="temperature"
                )

        # Soil moisture control
        if soil_moisture is not None:
            soil_min = thresholds["soil_moisture"]["min"]
            soil_max = thresholds["soil_moisture"]["max"]

            if soil_moisture < soil_min:
                self.publish_command(
                    device_id=device_id,
                    command=commands["soil_moisture_low"],
                    reason="soil_moisture_below_min_threshold",
                    sensor_type="soil_moisture"
                )
            elif soil_moisture > soil_max:
                self.publish_command(
                    device_id=device_id,
                    command=commands["soil_moisture_high"],
                    reason="soil_moisture_above_max_threshold",
                    sensor_type="soil_moisture"
                )
            else:
                self.publish_command(
                    device_id=device_id,
                    command=commands["soil_moisture_normal"],
                    reason="soil_moisture_back_to_normal_range",
                    sensor_type="soil_moisture"
                )

        # Humidity control
        if humidity is not None:
            hum_min = thresholds["humidity"]["min"]
            hum_max = thresholds["humidity"]["max"]

            if humidity < hum_min:
                self.publish_command(
                    device_id=device_id,
                    command=commands["humidity_low"],
                    reason="humidity_below_min_threshold",
                    sensor_type="humidity"
                )
            elif humidity > hum_max:
                self.publish_command(
                    device_id=device_id,
                    command=commands["humidity_high"],
                    reason="humidity_above_max_threshold",
                    sensor_type="humidity"
                )
            else:
                self.publish_command(
                    device_id=device_id,
                    command=commands["humidity_normal"],
                    reason="humidity_back_to_normal_range",
                    sensor_type="humidity"
                )


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
                "commands": "/commands",
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
class CommandsAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        with STATE.lock:
            return {
                "count": len(STATE.command_history),
                "commands": STATE.command_history
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
        with STATE.lock:
            return {
                "devices_count": len(STATE.latest_data),
                "commands_count": len(STATE.command_history),
                "last_update": now_utc_iso()
            }


def start_background_threads():
    threading.Thread(target=SERVICE.mqtt_loop, daemon=True).start()
    threading.Thread(target=SERVICE.registration_loop, daemon=True).start()


if __name__ == "__main__":
    print("[START] Analytics Control Service starting...")
    print(f"[INFO] Service ID: {RUNTIME['service_id']}")
    print(f"[INFO] MQTT Broker: {RUNTIME['mqtt_broker']}:{RUNTIME['mqtt_port']}")
    print(f"[INFO] Sensor Topic: {APP_CONFIG['mqtt']['sensor_topic']}")

    start_background_threads()

    app = RootAPI()
    app.health = HealthAPI()
    app.devices = DevicesAPI()
    app.commands = CommandsAPI()
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
