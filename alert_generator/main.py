import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

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
        "service_id": os.environ.get("SERVICE_ID", "alert-generator"),
        "service_name": os.environ.get("SERVICE_NAME", "Alert Generator"),
        "service_type": os.environ.get("SERVICE_TYPE", "alert_generator"),
        "service_host": os.environ.get("SERVICE_HOST", "0.0.0.0"),
        "service_port": int(os.environ.get("SERVICE_PORT", 8091)),
        "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
        "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
        "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
        "thingspeak_adapter_url": os.environ.get("THINGSPEAK_ADAPTER_URL", "http://thingspeak-adapter:8080"),
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
# Alert Generator Service
# --------------------------------------------------
class AlertGeneratorService:
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

    def thresholds(self) -> Dict[str, Any]:
        return APP_CONFIG["thresholds"]

    def default_report_results(self) -> int:
        return APP_CONFIG["report"]["default_results"]

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
    # MQTT
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

            alerts = self.generate_alerts(sensor_data)
            for alert in alerts:
                self.publish_alert(alert)

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
    # Alert logic
    # -----------------------------
    def generate_alerts(self, sensor_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        thresholds = self.thresholds()
        device_id = sensor_data["device_id"]
        alerts = []

        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")

        # Temperature alerts
        if temperature is not None:
            temp_min = thresholds["temperature"]["min"]
            temp_max = thresholds["temperature"]["max"]

            if temperature < temp_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_temperature",
                    "value": temperature,
                    "threshold": temp_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })

            elif temperature > temp_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_temperature",
                    "value": temperature,
                    "threshold": temp_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        # Soil moisture alerts
        if soil_moisture is not None:
            soil_min = thresholds["soil_moisture"]["min"]
            soil_max = thresholds["soil_moisture"]["max"]

            if soil_moisture < soil_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_soil_moisture",
                    "value": soil_moisture,
                    "threshold": soil_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })

            elif soil_moisture > soil_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_soil_moisture",
                    "value": soil_moisture,
                    "threshold": soil_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        # Humidity alerts
        if humidity is not None:
            hum_min = thresholds["humidity"]["min"]
            hum_max = thresholds["humidity"]["max"]

            if humidity < hum_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_humidity",
                    "value": humidity,
                    "threshold": hum_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })

            elif humidity > hum_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_humidity",
                    "value": humidity,
                    "threshold": hum_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        return alerts

    def publish_alert(self, alert: Dict[str, Any]) -> None:
        topic = f"{self.alert_topic_base()}/{alert['device_id']}"

        try:
            self.mqtt_client.publish(topic, json.dumps(alert))
            with STATE.lock:
                STATE.alerts.append(alert)
                if len(STATE.alerts) > 200:
                    STATE.alerts = STATE.alerts[-200:]

            print(f"[MQTT] Published alert to {topic}: {alert}")
        except Exception as e:
            print(f"[MQTT] Publish alert error: {e}")

    # -----------------------------
    # Report generation
    # -----------------------------
    def get_history(self, device_id: str, results: int) -> Dict[str, Any]:
        response = requests.get(
            f"{RUNTIME['thingspeak_adapter_url']}/history",
            params={"device_id": device_id, "results": results},
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    def generate_report(self, device_id: str, results: int = None) -> Dict[str, Any]:
        if results is None:
            results = self.default_report_results()

        with STATE.lock:
            latest_data = STATE.latest_data.get(device_id)

        if latest_data is None:
            raise ValueError(f"Device '{device_id}' not found in local state")

        history_data = self.get_history(device_id, results)
        feeds = history_data.get("feeds", [])

        temperatures = []
        soil_moistures = []
        humidities = []

        for feed in feeds:
            if feed.get("field1") not in (None, ""):
                temperatures.append(float(feed["field1"]))
            if feed.get("field2") not in (None, ""):
                soil_moistures.append(float(feed["field2"]))
            if feed.get("field3") not in (None, ""):
                humidities.append(float(feed["field3"]))

        def average(values: List[float]) -> float:
            return round(sum(values) / len(values), 2) if values else 0.0

        return {
            "device_id": device_id,
            "latest_data": latest_data,
            "history_count": len(feeds),
            "averages": {
                "temperature": average(temperatures),
                "soil_moisture": average(soil_moistures),
                "humidity": average(humidities)
            },
            "message": "Report generated successfully"
        }


SERVICE = AlertGeneratorService()


# --------------------------------------------------
# REST API
# --------------------------------------------------
@cherrypy.expose
class RootAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "message": "Alert Generator is running",
            "endpoints": {
                "health": "/health",
                "devices": "/devices",
                "device_by_id": "/devices/<device_id>",
                "alerts": "/alerts",
                "report": "/report?device_id=raspi-01"
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
class ReportAPI:
    @cherrypy.tools.json_out()
    def GET(self, device_id=None, results=None):
        if not device_id:
            cherrypy.response.status = 400
            return {"error": "device_id is required"}

        try:
            report = SERVICE.generate_report(
                device_id=device_id,
                results=int(results) if results else None
            )
            return report

        except ValueError as e:
            cherrypy.response.status = 404
            return {"error": str(e)}

        except requests.RequestException as e:
            cherrypy.response.status = 500
            return {"error": f"Failed to fetch history from ThingSpeak Adapter: {e}"}

        except Exception as e:
            cherrypy.response.status = 500
            return {"error": f"Report generation error: {e}"}


def start_background_threads():
    threading.Thread(target=SERVICE.mqtt_loop, daemon=True).start()
    threading.Thread(target=SERVICE.registration_loop, daemon=True).start()


if __name__ == "__main__":
    print("[START] Alert Generator starting...")
    print(f"[INFO] Service ID: {RUNTIME['service_id']}")
    print(f"[INFO] MQTT Broker: {RUNTIME['mqtt_broker']}:{RUNTIME['mqtt_port']}")
    print(f"[INFO] Sensor Topic: {APP_CONFIG['mqtt']['sensor_topic']}")

    start_background_threads()

    app = RootAPI()
    app.health = HealthAPI()
    app.devices = DevicesAPI()
    app.alerts = AlertsAPI()
    app.report = ReportAPI()

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
