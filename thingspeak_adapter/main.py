import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import cherrypy
import requests
import paho.mqtt.client as mqtt


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict[str, Any]) -> None:
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(temp_path, path)


def load_runtime_config() -> Dict[str, Any]:
    return {
        "service_id": os.environ.get("SERVICE_ID", "thingspeak-adapter"),
        "service_name": os.environ.get("SERVICE_NAME", "ThingSpeak Adapter"),
        "service_type": os.environ.get("SERVICE_TYPE", "thingspeak_adapter"),
        "service_host": os.environ.get("SERVICE_HOST", "0.0.0.0"),
        "service_port": int(os.environ.get("SERVICE_PORT", 8080)),
        "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
        "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
        "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
        "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
        "config_file": os.environ.get("CONFIG_FILE", "config.json"),
        "registry_file": os.environ.get("REGISTRY_FILE", "channel_registry.json"),
    }

def ensure_file_exists(path: str, default_data: Dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default_data, f, indent=2)

RUNTIME = load_runtime_config()
ensure_file_exists(RUNTIME["registry_file"], {})
APP_CONFIG = load_json(RUNTIME["config_file"])


# --------------------------------------------------
# Shared state
# --------------------------------------------------
class SharedState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.latest_message: Optional[Dict[str, Any]] = None
        self.last_upload_status: Optional[Dict[str, Any]] = None
        self.last_registration_time: float = 0.0
        self.registry: Dict[str, Any] = load_json(RUNTIME["registry_file"])
        print(f"[REGISTRY] Loaded registry from {RUNTIME['registry_file']}: {self.registry}")


STATE = SharedState()


# --------------------------------------------------
# ThingSpeak Adapter
# --------------------------------------------------
class ThingSpeakAdapter:
    def __init__(self) -> None:
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    # -----------------------------
    # Configuration helpers
    # -----------------------------
    def thingspeak_base_url(self) -> str:
        return APP_CONFIG["thingspeak"]["base_url"]

    def user_api_key(self) -> str:
        return APP_CONFIG["thingspeak"]["user_api_key"]

    def mqtt_topic(self) -> str:
        return APP_CONFIG["mqtt"]["topic"]

    def default_fields(self) -> Dict[str, str]:
        return APP_CONFIG.get("default_fields", {})

    # -----------------------------
    # Registry management
    # -----------------------------
    def get_device_channel(self, device_id: str) -> Optional[Dict[str, Any]]:
        with STATE.lock:
            return STATE.registry.get(device_id)

    def save_device_channel(self, device_id: str, channel_info: Dict[str, Any]) -> None:
        with STATE.lock:
            STATE.registry[device_id] = channel_info
            save_json(RUNTIME["registry_file"], STATE.registry)

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
    # ThingSpeak channel creation
    # -----------------------------
    def create_channel_for_device(self, device_id: str) -> Dict[str, Any]:
        """
        Create a ThingSpeak channel for a new device using the User API Key.
        """
        url = f"{self.thingspeak_base_url()}/channels.json"

        payload = {
            "api_key": self.user_api_key(),
            "name": f"Plant Channel - {device_id}",
            "public_flag": str(APP_CONFIG["thingspeak"].get("public_channels", False)).lower()
        }

        fields = self.default_fields()
        field_index = 1
        for _, field_label in fields.items():
            payload[f"field{field_index}"] = field_label
            field_index += 1

        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        data = response.json()

        api_keys = data.get("api_keys", [])

        write_key = None
        read_key = None

        for key_info in api_keys:
            if key_info.get("write_flag", False):
                write_key = key_info.get("api_key")
            else:
                read_key = key_info.get("api_key")

        channel_info = {
            "channel_id": data["id"],
            "name": data.get("name", f"Plant Channel - {device_id}"),
            "write_api_key": write_key,
            "read_api_key": read_key
        }

        self.save_device_channel(device_id, channel_info)
        print(f"[THINGSPEAK] Created new channel for {device_id}: {channel_info}")

        return channel_info

    def ensure_channel_for_device(self, device_id: str) -> Dict[str, Any]:
        existing = self.get_device_channel(device_id)
        if existing:
            return existing
        return self.create_channel_for_device(device_id)

    # -----------------------------
    # MQTT handling
    # -----------------------------
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = self.mqtt_topic()
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

            with STATE.lock:
                STATE.latest_message = sensor_data

            print(f"[MQTT] Received: {sensor_data}")
            self.process_sensor_data(sensor_data)

        except json.JSONDecodeError:
            self.set_upload_status(False, "Invalid JSON received from MQTT")
            print("[MQTT] Invalid JSON payload")

        except Exception as e:
            self.set_upload_status(False, f"Unexpected processing error: {e}")
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
    # ThingSpeak upload
    # -----------------------------
    def build_update_payload(self, sensor_data: Dict[str, Any], channel_info: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "api_key": channel_info["write_api_key"],
            "created_at": sensor_data.get("timestamp", now_utc_iso()),
            "status": sensor_data.get("device_id", "unknown-device")
        }

        sensor_to_field = {
            "temperature": "field1",
            "soil_moisture": "field2",
            "humidity": "field3"
        }

        for sensor_key, field_name in sensor_to_field.items():
            if sensor_key in sensor_data:
                payload[field_name] = sensor_data[sensor_key]

        return payload

    def process_sensor_data(self, sensor_data: Dict[str, Any]) -> None:
        device_id = sensor_data.get("device_id")
        if not device_id:
            self.set_upload_status(False, "Missing device_id in MQTT payload")
            return

        channel_info = self.ensure_channel_for_device(device_id)

        if not channel_info.get("write_api_key"):
            self.set_upload_status(False, f"Missing write API key for device {device_id}")
            return

        url = f"{self.thingspeak_base_url()}/update.json"
        payload = self.build_update_payload(sensor_data, channel_info)

        try:
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status()

            try:
                result = response.json()
            except Exception:
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

        except requests.RequestException as e:
            self.set_upload_status(
                False,
                f"Upload failed: {e}",
                {
                    "device_id": device_id,
                    "channel_id": channel_info.get("channel_id")
                }
            )
            print(f"[THINGSPEAK] Upload error for {device_id}: {e}")

    def set_upload_status(self, success: bool, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        status = {
            "success": success,
            "message": message,
            "timestamp": now_utc_iso()
        }

        if extra:
            status.update(extra)

        with STATE.lock:
            STATE.last_upload_status = status


ADAPTER = ThingSpeakAdapter()


# --------------------------------------------------
# REST API
# --------------------------------------------------
@cherrypy.expose
class RootAPI:
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


@cherrypy.expose
class HealthAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        with STATE.lock:
            return {
                "status": "ok",
                "timestamp": now_utc_iso(),
                "latest_message": STATE.latest_message,
                "last_upload_status": STATE.last_upload_status
            }


@cherrypy.expose
class LatestAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        with STATE.lock:
            if STATE.latest_message is None:
                cherrypy.response.status = 404
                return {"error": "No MQTT data received yet"}
            return STATE.latest_message


@cherrypy.expose
class RegistryAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        with STATE.lock:
            return STATE.registry
@cherrypy.expose
class HistoryAPI:
    @cherrypy.tools.json_out()
    def GET(self, device_id=None, results=20):
        if not device_id:
            cherrypy.response.status = 400
            return {"error": "device_id is required"}

        channel_info = ADAPTER.get_device_channel(device_id)
        if not channel_info:
            cherrypy.response.status = 404
            return {"error": f"No ThingSpeak channel found for device '{device_id}'"}

        channel_id = channel_info.get("channel_id")
        read_api_key = channel_info.get("read_api_key")

        if not channel_id:
            cherrypy.response.status = 404
            return {"error": f"Missing channel_id for device '{device_id}'"}

        url = f"{ADAPTER.thingspeak_base_url()}/channels/{channel_id}/feeds.json"
        params = {"results": int(results)}

        if read_api_key:
            params["api_key"] = read_api_key

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            return {
                "device_id": device_id,
                "channel_id": channel_id,
                "count": len(data.get("feeds", [])),
                "channel": data.get("channel", {}),
                "feeds": data.get("feeds", [])
            }

        except requests.RequestException as e:
            cherrypy.response.status = 500
            return {"error": f"ThingSpeak history request failed: {e}"}


def start_background_threads():
    threading.Thread(target=ADAPTER.mqtt_loop, daemon=True).start()
    threading.Thread(target=ADAPTER.registration_loop, daemon=True).start()


if __name__ == "__main__":
    print("[START] ThingSpeak Adapter starting...")
    print(f"[INFO] Service ID: {RUNTIME['service_id']}")
    print(f"[INFO] MQTT Broker: {RUNTIME['mqtt_broker']}:{RUNTIME['mqtt_port']}")
    print(f"[INFO] MQTT Topic: {APP_CONFIG['mqtt']['topic']}")

    start_background_threads()

    app = RootAPI()
    app.health = HealthAPI()
    app.latest = LatestAPI()
    app.registry = RegistryAPI()
    app.history = HistoryAPI()

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