import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import cherrypy
import requests
from shared.MyMQTT import MyMQTT
from rule_engine import AlertRuleEngine


class AppConfig:
    @staticmethod
    def now_utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def load_json(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
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


class SharedState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest_data: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []
        self.last_registration_time: float = 0.0

    def set_latest_data(self, device_id: str, sensor_data: Dict[str, Any]) -> None:
        with self.lock:
            self.latest_data[device_id] = sensor_data

    def get_latest_data(self, device_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.latest_data.get(device_id)

    def get_all_latest_data(self) -> Dict[str, Dict[str, Any]]:
        with self.lock:
            return dict(self.latest_data)

    def add_alert(self, alert: Dict[str, Any]) -> None:
        with self.lock:
            self.alerts.append(alert)
            if len(self.alerts) > 200:
                self.alerts = self.alerts[-200:]

    def get_alerts(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.alerts)

    def set_last_registration_time(self, timestamp: float) -> None:
        with self.lock:
            self.last_registration_time = timestamp


class AlertGeneratorService:
    def __init__(self) -> None:
        self.runtime = AppConfig.load_runtime_config()
        self.config = AppConfig.load_json(self.runtime["config_file"])
        self.state = SharedState()
        self.rule_engine = AlertRuleEngine(self.config["thresholds"])

        self.mqtt_client = MyMQTT(
            clientID=self.runtime["service_id"],
            broker=self.runtime["mqtt_broker"],
            port=self.runtime["mqtt_port"],
            notifier=self
        )

    # --------------------------------------------------
    # Config helpers
    # --------------------------------------------------
    def sensor_topic(self) -> str:
        return self.config["mqtt"]["sensor_topic"]

    def alert_topic_base(self) -> str:
        return self.config["mqtt"]["alert_topic_base"]

    def thresholds(self) -> Dict[str, Any]:
        return self.config["thresholds"]

    def default_report_results(self) -> int:
        return self.config["report"]["default_results"]

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def register_service(self) -> None:
        payload = {
            "id": self.runtime["service_id"],
            "name": self.runtime["service_name"],
            "type": self.runtime["service_type"],
            "endpoint": f"http://{self.runtime['service_id']}:{self.runtime['service_port']}",
            "status": "active"
        }

        try:
            response = requests.post(
                f"{self.runtime['catalog_url']}/services",
                json=payload,
                timeout=5
            )

            if response.status_code in (200, 201):
                self.state.set_last_registration_time(time.time())
                print(f"[CATALOGUE] Service registered: {payload}")
            else:
                print(f"[CATALOGUE] Registration failed: {response.status_code} {response.text}")

        except requests.RequestException as e:
            print(f"[CATALOGUE] Registration error: {e}")

    def registration_loop(self) -> None:
        while True:
            self.register_service()
            time.sleep(self.runtime["register_interval"])

    # --------------------------------------------------
    # MQTT Actions & Callback
    # --------------------------------------------------
    def notify(self, topic: str, payload: str) -> None:
        try:
            sensor_data = json.loads(payload)
            sensor_data["_mqtt_topic"] = topic
            sensor_data["_received_at"] = AppConfig.now_utc_iso()

            device_id = sensor_data.get("device_id")
            if not device_id:
                print("[MQTT] Missing device_id in payload")
                return

               # Cache the latest sensor update
            self.state.set_latest_data(device_id, sensor_data)
            print(f"[MQTT] Received from {topic}: {sensor_data}")

            # Analyze thresholds
            alerts = self.rule_engine.generate_alerts(sensor_data)
            for alert in alerts:
                self.publish_alert(alert)

        except json.JSONDecodeError:
            print("[MQTT] Invalid JSON payload received")
        except Exception as e:
            print(f"[MQTT] Processing error: {e}")

    def publish_alert(self, alert: Dict[str, Any]) -> None:
        topic = f"{self.alert_topic_base()}/{alert['device_id']}"
        try:
            self.mqtt_client.myPublish(topic, alert)
            self.state.add_alert(alert)
            print(f"[MQTT] Published alert to {topic}: {alert}")
        except Exception as e:
            print(f"[MQTT] Publish alert error: {e}")

    # --------------------------------------------------
    # Report generation
    # --------------------------------------------------
    def get_history(self, device_id: str, results: int) -> Dict[str, Any]:
        response = requests.get(
            f"{self.runtime['thingspeak_adapter_url']}/history",
            params={"device_id": device_id, "results": results},
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    def generate_report(self, device_id: str, results: Optional[int] = None) -> Dict[str, Any]:
        if results is None:
            results = self.default_report_results()

        latest_data = self.state.get_latest_data(device_id)
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

    # --------------------------------------------------
    # Background tasks
    # --------------------------------------------------
    def start_background_threads(self) -> None:
        self.mqtt_client.start()
        self.mqtt_client.mySubscribe(self.sensor_topic())
        threading.Thread(target=self.registration_loop, daemon=True).start()


class RootAPI(object):
    exposed = True

    def __init__(self, service: AlertGeneratorService) -> None:
        self.service = service
        self.health = HealthAPI(service)
        self.devices = DevicesAPI(service)
        self.alerts = AlertsAPI(service)
        self.report = ReportAPI(service)

    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
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


class HealthAPI(object):
    exposed = True

    def __init__(self, service: AlertGeneratorService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
        return {
            "status": "ok",
            "timestamp": AppConfig.now_utc_iso(),
            "service_id": self.service.runtime["service_id"]
        }


class DevicesAPI(object):
    exposed = True

    def __init__(self, service: AlertGeneratorService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, device_id=None, **query):
        if device_id:
            device = self.service.state.get_latest_data(device_id)
            if not device:
                # Professor's way to handle errors
                raise cherrypy.HTTPError(404, f"Device '{device_id}' not found")
            return device

        devices = self.service.state.get_all_latest_data()
        return {
            "count": len(devices),
            "devices": devices
        }


class AlertsAPI(object):
    exposed = True

    def __init__(self, service: AlertGeneratorService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        alerts = self.service.state.get_alerts()
        return {
            "count": len(alerts),
            "alerts": alerts
        }


class ReportAPI(object):
    exposed = True

    def __init__(self, service: AlertGeneratorService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, device_id=None, results=None, **query):
        if not device_id:
            raise cherrypy.HTTPError(400, "device_id is required")

        try:
            report = self.service.generate_report(
                device_id=device_id,
                results=int(results) if results else None
            )
            return report

        except ValueError as e:
            raise cherrypy.HTTPError(404, str(e))

        except requests.RequestException as e:
            raise cherrypy.HTTPError(500, f"Failed to fetch history from ThingSpeak Adapter: {e}")

        except Exception as e:
            raise cherrypy.HTTPError(500, f"Report generation error: {e}")

if __name__ == "__main__":
    service = AlertGeneratorService()

    print("[START] Alert Generator starting...")
    print(f"[INFO] Service ID: {service.runtime['service_id']}")
    print(f"[INFO] MQTT Broker: {service.runtime['mqtt_broker']}:{service.runtime['mqtt_port']}")
    print(f"[INFO] Sensor Topic: {service.config['mqtt']['sensor_topic']}")

    service.start_background_threads()

    app = RootAPI(service)

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher(),
            "tools.sessions.on": True
        }
    }

    cherrypy.tree.mount(app, "/", conf)

    cherrypy.config.update({
        "server.socket_host": service.runtime["service_host"],
        "server.socket_port": service.runtime["service_port"],
        "log.screen": True
    })

    cherrypy.engine.start()
    cherrypy.engine.block()