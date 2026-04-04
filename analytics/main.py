import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import cherrypy
import requests
import paho.mqtt.client as mqtt


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


class SharedState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest_data: Dict[str, Dict[str, Any]] = {}
        self.command_history: List[Dict[str, Any]] = []
        self.last_command_by_device: Dict[str, Dict[str, str]] = {}
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

    def get_command_history(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.command_history)

    def get_last_command_for_sensor(self, device_id: str, sensor_type: str) -> Optional[str]:
        with self.lock:
            return self.last_command_by_device.get(device_id, {}).get(sensor_type)

    def add_command(self, device_id: str, sensor_type: str, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.command_history.append(payload)
            if len(self.command_history) > 200:
                self.command_history = self.command_history[-200:]

            if device_id not in self.last_command_by_device:
                self.last_command_by_device[device_id] = {}

            self.last_command_by_device[device_id][sensor_type] = payload["command"]

    def set_last_registration_time(self, timestamp: float) -> None:
        with self.lock:
            self.last_registration_time = timestamp

    def get_summary(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "devices_count": len(self.latest_data),
                "commands_count": len(self.command_history),
                "last_update": AppConfig.now_utc_iso()
            }


class AnalyticsControlService:
    def __init__(self) -> None:
        self.runtime = AppConfig.load_runtime_config()
        self.config = AppConfig.load_json(self.runtime["config_file"])
        self.state = SharedState()

        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # --------------------------------------------------
    # Config helpers
    # --------------------------------------------------
    def sensor_topic(self) -> str:
        return self.config["mqtt"]["sensor_topic"]

    def command_topic_base(self) -> str:
        return self.config["mqtt"]["command_topic_base"]

    def thresholds(self) -> Dict[str, Any]:
        return self.config["thresholds"]

    def commands(self) -> Dict[str, str]:
        return self.config["commands"]

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
    # MQTT handling
    # --------------------------------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = self.sensor_topic()
            client.subscribe(topic)
            print(f"[MQTT] Connected to {self.runtime['mqtt_broker']}:{self.runtime['mqtt_port']}")
            print(f"[MQTT] Subscribed to {topic}")
        else:
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            sensor_data = json.loads(msg.payload.decode("utf-8"))
            sensor_data["_mqtt_topic"] = msg.topic
            sensor_data["_received_at"] = AppConfig.now_utc_iso()

            device_id = sensor_data.get("device_id")
            if not device_id:
                print("[MQTT] Missing device_id in payload")
                return

            self.state.set_latest_data(device_id, sensor_data)

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
                    self.runtime["mqtt_broker"],
                    self.runtime["mqtt_port"],
                    keepalive=60
                )
                self.mqtt_client.loop_forever()
            except Exception as e:
                print(f"[MQTT] Connection error: {e}")
                print("[MQTT] Retrying in 5 seconds...")
                time.sleep(5)

    # --------------------------------------------------
    # Command publishing
    # --------------------------------------------------
    def publish_command(self, device_id: str, command: str, reason: str, sensor_type: str) -> None:
        last_command = self.state.get_last_command_for_sensor(device_id, sensor_type)

        if last_command == command:
            return

        payload = {
            "device_id": device_id,
            "command": command,
            "reason": reason,
            "sensor_type": sensor_type,
            "timestamp": AppConfig.now_utc_iso()
        }

        topic = f"{self.command_topic_base()}/{device_id}"

        try:
            self.mqtt_client.publish(topic, json.dumps(payload))
            print(f"[MQTT] Published command to {topic}: {payload}")
            self.state.add_command(device_id, sensor_type, payload)

        except Exception as e:
            print(f"[MQTT] Publish command error: {e}")

    # --------------------------------------------------
    # Rule engine
    # --------------------------------------------------
    def evaluate_controls(self, sensor_data: Dict[str, Any]) -> None:
        device_id = sensor_data["device_id"]
        thresholds = self.thresholds()
        commands = self.commands()

        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")

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

    # --------------------------------------------------
    # Background tasks
    # --------------------------------------------------
    def start_background_threads(self) -> None:
        threading.Thread(target=self.mqtt_loop, daemon=True).start()
        threading.Thread(target=self.registration_loop, daemon=True).start()


class RootAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service
        self.health = HealthAPI(service)
        self.devices = DevicesAPI(service)
        self.commands = CommandsAPI(service)
        self.rules = RulesAPI(service)
        self.summary = SummaryAPI(service)

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


class HealthAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "status": "ok",
            "timestamp": AppConfig.now_utc_iso(),
            "service_id": self.service.runtime["service_id"]
        }


class DevicesAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, device_id=None):
        if device_id:
            device = self.service.state.get_latest_data(device_id)
            if not device:
                cherrypy.response.status = 404
                return {"error": f"Device '{device_id}' not found"}
            return device

        devices = self.service.state.get_all_latest_data()
        return {
            "count": len(devices),
            "devices": devices
        }


class CommandsAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        commands = self.service.state.get_command_history()
        return {
            "count": len(commands),
            "commands": commands
        }


class RulesAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "thresholds": self.service.config["thresholds"],
            "commands": self.service.config["commands"]
        }


class SummaryAPI:
    exposed = True

    def __init__(self, service: AnalyticsControlService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        return self.service.state.get_summary()


if __name__ == "__main__":
    service = AnalyticsControlService()

    print("[START] Analytics Control Service starting...")
    print(f"[INFO] Service ID: {service.runtime['service_id']}")
    print(f"[INFO] MQTT Broker: {service.runtime['mqtt_broker']}:{service.runtime['mqtt_port']}")
    print(f"[INFO] Sensor Topic: {service.config['mqtt']['sensor_topic']}")

    service.start_background_threads()

    app = RootAPI(service)

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
    }

    cherrypy.config.update({
        "server.socket_host": service.runtime["service_host"],
        "server.socket_port": service.runtime["service_port"],
        "log.screen": True
    })

    cherrypy.quickstart(app, "/", conf)