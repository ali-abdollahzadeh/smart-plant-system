import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import cherrypy
import paho.mqtt.client as mqtt
import requests


class AppConfig:
    @staticmethod
    def now_utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def load_json(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def ensure_json_file(path: str, default_data: Dict[str, Any]) -> None:
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as file:
                json.dump(default_data, file, indent=4)

    @staticmethod
    def save_json_atomic(path: str, data: Dict[str, Any]) -> None:
        temp_path = f"{path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        os.replace(temp_path, path)


class SharedState:
    def __init__(self, registry_file: str) -> None:
        self.lock = threading.RLock()
        self.latest_message: Optional[Dict[str, Any]] = None
        self.last_upload_status: Optional[Dict[str, Any]] = None
        self.registry_file = registry_file
        self.registry: Dict[str, Any] = AppConfig.load_json(registry_file)
        print(f"[REGISTRY] Loaded {len(self.registry)} device channel entries")

    def set_latest_message(self, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.latest_message = payload

    def get_latest_message(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.latest_message

    def set_last_upload_status(self, payload: Dict[str, Any]) -> None:
        with self.lock:
            self.last_upload_status = payload

    def get_last_upload_status(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.last_upload_status

    def get_registry(self) -> Dict[str, Any]:
        with self.lock:
            return dict(self.registry)

    def get_device_channel(self, device_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.registry.get(device_id)

    def save_device_channel(self, device_id: str, channel_info: Dict[str, Any]) -> None:
        with self.lock:
            self.registry[device_id] = channel_info
            AppConfig.save_json_atomic(self.registry_file, self.registry)


class ThingSpeakAdapterService:
    def __init__(self) -> None:
        self.config_file = os.environ.get("CONFIG_FILE", "/app/config.json")
        self.config = AppConfig.load_json(self.config_file)

        self.registry_file = self.config["storage"]["registry_file"]
        AppConfig.ensure_json_file(self.registry_file, {})
        self.state = SharedState(self.registry_file)

        mqtt_config = self.config["mqtt"]
        self.mqtt_client = mqtt.Client(client_id=mqtt_config["client_id"])
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # --------------------------------------------------
    # Configuration helpers
    # --------------------------------------------------
    def service_config(self) -> Dict[str, Any]:
        return self.config["service"]

    def catalogue_config(self) -> Dict[str, Any]:
        return self.config["catalogue"]

    def mqtt_config(self) -> Dict[str, Any]:
        return self.config["mqtt"]

    def thingspeak_config(self) -> Dict[str, Any]:
        return self.config["thingspeak"]

    def field_mapping(self) -> Dict[str, str]:
        return self.config["field_mapping"]

    def thingspeak_base_url(self) -> str:
        return self.thingspeak_config()["base_url"].rstrip("/")

    def user_api_key(self) -> str:
        env_name = self.thingspeak_config()["user_api_key_env"]
        api_key = os.environ.get(env_name)
        if not api_key:
            raise RuntimeError(f"Missing required environment variable: {env_name}")
        return api_key

    # --------------------------------------------------
    # Catalogue registration
    # --------------------------------------------------
    def build_registration_payload(self) -> Dict[str, Any]:
        service = self.service_config()
        return {
            "id": service["id"],
            "name": service["name"],
            "type": service["type"],
            "endpoint": service["endpoint"],
            "status": "active"
        }

    def register_service(self) -> bool:
        payload = self.build_registration_payload()
        url = f"{self.catalogue_config()['url']}/services"

        try:
            response = requests.post(
                url,
                json=payload,
                timeout=self.catalogue_config()["request_timeout"]
            )

            if response.status_code in (200, 201):
                print("[CATALOGUE] ThingSpeak Adapter registered successfully")
                return True

            print(
                "[CATALOGUE] Registration failed - "
                f"status={response.status_code}, response={response.text}"
            )
        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_startup_task(self) -> None:
        retry_delay = self.catalogue_config()["registration_retry_delay"]

        while not self.register_service():
            print(f"[CATALOGUE] Retrying registration in {retry_delay} seconds...")
            time.sleep(retry_delay)

    # --------------------------------------------------
    # Channel registry
    # --------------------------------------------------
    def get_device_channel(self, device_id: str) -> Optional[Dict[str, Any]]:
        return self.state.get_device_channel(device_id)

    def save_device_channel(self, device_id: str, channel_info: Dict[str, Any]) -> None:
        self.state.save_device_channel(device_id, channel_info)

    # --------------------------------------------------
    # ThingSpeak channel creation
    # --------------------------------------------------
    def create_channel_for_device(self, device_id: str) -> Dict[str, Any]:
        url = f"{self.thingspeak_base_url()}/channels.json"
        thingspeak = self.thingspeak_config()

        payload = {
            "api_key": self.user_api_key(),
            "name": f"{thingspeak['channel_name_prefix']}{device_id}",
            "public_flag": str(thingspeak["public_channels"]).lower()
        }

        for sensor_name, field_name in self.field_mapping().items():
            payload[field_name] = thingspeak["field_labels"][sensor_name]

        response = requests.post(
            url,
            data=payload,
            timeout=thingspeak["request_timeout"]
        )
        response.raise_for_status()
        data = response.json()

        write_key = None
        read_key = None

        for key_info in data.get("api_keys", []):
            if key_info.get("write_flag", False):
                write_key = key_info.get("api_key")
            else:
                read_key = key_info.get("api_key")

        channel_info = {
            "channel_id": data["id"],
            "name": data.get("name", f"{thingspeak['channel_name_prefix']}{device_id}"),
            "write_api_key": write_key,
            "read_api_key": read_key
        }

        self.save_device_channel(device_id, channel_info)
        print(f"[THINGSPEAK] Created channel for {device_id}: {channel_info['channel_id']}")
        return channel_info

    def ensure_channel_for_device(self, device_id: str) -> Dict[str, Any]:
        existing = self.get_device_channel(device_id)
        if existing:
            return existing
        return self.create_channel_for_device(device_id)

    # --------------------------------------------------
    # SenML parsing
    # --------------------------------------------------
    def normalize_sensor_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}

        device_id = payload.get("bn") or payload.get("device_id")
        if device_id:
            normalized["device_id"] = device_id

        if payload.get("timestamp"):
            normalized["timestamp"] = payload["timestamp"]
        elif payload.get("bt") is not None:
            try:
                normalized["timestamp"] = (
                    datetime.fromtimestamp(float(payload["bt"]), tz=timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                )
            except (TypeError, ValueError, OverflowError):
                normalized["timestamp"] = AppConfig.now_utc_iso()
        else:
            normalized["timestamp"] = AppConfig.now_utc_iso()

        measurements = payload.get("e", [])
        if isinstance(measurements, list):
            for measurement in measurements:
                if not isinstance(measurement, dict):
                    continue
                name = measurement.get("n")
                if name in self.field_mapping() and "v" in measurement:
                    normalized[name] = measurement["v"]

        # Backward compatibility for flat JSON payloads.
        for sensor_name in self.field_mapping():
            if sensor_name not in normalized and sensor_name in payload:
                normalized[sensor_name] = payload[sensor_name]

        return normalized

    # --------------------------------------------------
    # MQTT handling
    # --------------------------------------------------
    def on_connect(self, client, userdata, flags, rc):
        mqtt_config = self.mqtt_config()

        if rc == 0:
            client.subscribe(mqtt_config["topic"], qos=mqtt_config["qos"])
            print(f"[MQTT] Connected to {mqtt_config['broker']}:{mqtt_config['port']}")
            print(f"[MQTT] Subscribed to {mqtt_config['topic']}")
        else:
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_message(self, client, userdata, msg):
        try:
            raw_payload = json.loads(msg.payload.decode("utf-8"))
            sensor_data = self.normalize_sensor_payload(raw_payload)
            sensor_data["_mqtt_topic"] = msg.topic
            sensor_data["_received_at"] = AppConfig.now_utc_iso()

            self.state.set_latest_message(sensor_data)
            print(f"[MQTT] Received from {msg.topic}: {sensor_data}")
            self.process_sensor_data(sensor_data)

        except json.JSONDecodeError:
            self.set_upload_status(False, "Invalid JSON received from MQTT")
            print("[MQTT] Invalid JSON payload")
        except Exception as error:
            self.set_upload_status(False, f"Unexpected processing error: {error}")
            print(f"[MQTT] Processing error: {error}")

    def mqtt_loop(self) -> None:
        mqtt_config = self.mqtt_config()

        while True:
            try:
                self.mqtt_client.connect(
                    mqtt_config["broker"],
                    mqtt_config["port"],
                    keepalive=mqtt_config["keepalive"]
                )
                self.mqtt_client.loop_forever()
            except Exception as error:
                print(f"[MQTT] Connection error: {error}")
                retry_delay = mqtt_config["reconnect_delay"]
                print(f"[MQTT] Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    # --------------------------------------------------
    # ThingSpeak upload
    # --------------------------------------------------
    def build_update_payload(
        self,
        sensor_data: Dict[str, Any],
        channel_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        payload = {
            "api_key": channel_info["write_api_key"],
            "created_at": sensor_data.get("timestamp", AppConfig.now_utc_iso()),
            "status": sensor_data.get("device_id", "unknown-device")
        }

        for sensor_name, field_name in self.field_mapping().items():
            if sensor_name in sensor_data:
                payload[field_name] = sensor_data[sensor_name]

        return payload

    def process_sensor_data(self, sensor_data: Dict[str, Any]) -> None:
        device_id = sensor_data.get("device_id")

        if not device_id:
            self.set_upload_status(False, "Missing device_id / SenML bn")
            return

        channel_info = self.ensure_channel_for_device(device_id)

        if not channel_info.get("write_api_key"):
            self.set_upload_status(False, f"Missing write API key for device {device_id}")
            return

        url = f"{self.thingspeak_base_url()}/update.json"
        payload = self.build_update_payload(sensor_data, channel_info)

        try:
            response = requests.post(
                url,
                data=payload,
                timeout=self.thingspeak_config()["request_timeout"]
            )
            response.raise_for_status()

            try:
                result = response.json()
            except ValueError:
                result = response.text

            self.set_upload_status(
                True,
                "Upload successful",
                {
                    "device_id": device_id,
                    "channel_id": channel_info["channel_id"],
                    "response": result
                }
            )

            print(
                f"[THINGSPEAK] Uploaded data for {device_id} "
                f"to channel {channel_info['channel_id']}"
            )

        except requests.RequestException as error:
            self.set_upload_status(
                False,
                f"Upload failed: {error}",
                {
                    "device_id": device_id,
                    "channel_id": channel_info.get("channel_id")
                }
            )
            print(f"[THINGSPEAK] Upload error for {device_id}: {error}")

    def set_upload_status(
        self,
        success: bool,
        message: str,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        status = {
            "success": success,
            "message": message,
            "timestamp": AppConfig.now_utc_iso()
        }

        if extra:
            status.update(extra)

        self.state.set_last_upload_status(status)

    # --------------------------------------------------
    # Background tasks
    # --------------------------------------------------
    def start_background_threads(self) -> None:
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


class RootAPI:
    exposed = True

    def __init__(self, service: ThingSpeakAdapterService) -> None:
        self.service = service
        self.health = HealthAPI(service)
        self.latest = LatestAPI(service)
        self.registry = RegistryAPI(service)
        self.history = HistoryAPI(service)

    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "message": "ThingSpeak Adapter is running",
            "endpoints": {
                "health": "/health",
                "latest": "/latest",
                "registry": "/registry",
                "history": "/history?device_id=raspi-01&results=5"
            }
        }


class HealthAPI:
    exposed = True

    def __init__(self, service: ThingSpeakAdapterService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        return {
            "status": "ok",
            "timestamp": AppConfig.now_utc_iso(),
            "latest_message": self.service.state.get_latest_message(),
            "last_upload_status": self.service.state.get_last_upload_status()
        }


class LatestAPI:
    exposed = True

    def __init__(self, service: ThingSpeakAdapterService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        latest = self.service.state.get_latest_message()
        if latest is None:
            cherrypy.response.status = 404
            return {"error": "No MQTT data received yet"}
        return latest


class RegistryAPI:
    exposed = True

    def __init__(self, service: ThingSpeakAdapterService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        registry = self.service.state.get_registry()
        safe_registry = {}

        for device_id, info in registry.items():
            safe_registry[device_id] = {
                "channel_id": info.get("channel_id"),
                "name": info.get("name")
            }

        return safe_registry


class HistoryAPI:
    exposed = True

    def __init__(self, service: ThingSpeakAdapterService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, device_id=None, results=20):
        if not device_id:
            cherrypy.response.status = 400
            return {"error": "device_id is required"}

        try:
            results = int(results)
        except (TypeError, ValueError):
            cherrypy.response.status = 400
            return {"error": "results must be an integer"}

        max_results = self.service.thingspeak_config()["max_history_results"]
        results = max(1, min(results, max_results))

        channel_info = self.service.get_device_channel(device_id)
        if not channel_info:
            cherrypy.response.status = 404
            return {"error": f"No ThingSpeak channel found for device '{device_id}'"}

        channel_id = channel_info.get("channel_id")
        read_api_key = channel_info.get("read_api_key")

        if not channel_id:
            cherrypy.response.status = 404
            return {"error": f"Missing channel_id for device '{device_id}'"}

        url = f"{self.service.thingspeak_base_url()}/channels/{channel_id}/feeds.json"
        params = {"results": results}

        if read_api_key:
            params["api_key"] = read_api_key

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self.service.thingspeak_config()["request_timeout"]
            )
            response.raise_for_status()
            data = response.json()

            return {
                "device_id": device_id,
                "channel_id": channel_id,
                "count": len(data.get("feeds", [])),
                "channel": data.get("channel", {}),
                "feeds": data.get("feeds", [])
            }

        except requests.RequestException as error:
            cherrypy.response.status = 502
            return {"error": f"ThingSpeak history request failed: {error}"}


if __name__ == "__main__":
    service = ThingSpeakAdapterService()

    service_info = service.service_config()
    mqtt_info = service.mqtt_config()

    print("[START] ThingSpeak Adapter starting...")
    print(f"[INFO] Service ID: {service_info['id']}")
    print(f"[INFO] MQTT Broker: {mqtt_info['broker']}:{mqtt_info['port']}")
    print(f"[INFO] MQTT Topic: {mqtt_info['topic']}")

    service.start_background_threads()

    app = RootAPI(service)

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
    }

    cherrypy.config.update({
        "server.socket_host": service_info["host"],
        "server.socket_port": service_info["port"],
        "log.screen": service_info["log_screen"]
    })

    cherrypy.quickstart(app, "/", conf)