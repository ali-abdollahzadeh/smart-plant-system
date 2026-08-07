import json
import os
import threading
import time
from datetime import datetime, timezone
import cherrypy
import paho.mqtt.client as mqtt
import requests


# =============================================================================
# 1. HELPER FUNCTIONS
# =============================================================================
def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# =============================================================================
# 2. ALERT GENERATOR SERVICE (CHERRYPY + MQTT)
# =============================================================================
class AlertGeneratorService:
    exposed = True

    def __init__(
        self,
        service_id,
        service_name,
        service_type,
        service_host,
        service_port,
        catalog_url,
        thingspeak_adapter_url,
        default_results,
        client_id,
        broker,
        port,
        sub_topic,
        analysis_topic,
        pub_topic
    ):
        self.id = service_id
        self.service_name = service_name
        self.service_type = service_type
        self.service_host = service_host
        self.service_port = service_port
        self.catalog_url = catalog_url.rstrip("/")
        self.thingspeak_adapter_url = thingspeak_adapter_url.rstrip("/")
        self.default_results = default_results

        self.client_id = client_id
        self.broker = broker
        self.port = port
        self.sub_topic = sub_topic
        self.analysis_topic = analysis_topic
        self.pub_topic = pub_topic

        # State Initialization
        self.lock = threading.RLock()
        self.latest_data = {}
        self.alerts = []

        # MQTT Client Setup
        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(client_id=self.client_id, clean_session=True)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

    def average(self, values):
        return round(sum(values) / len(values), 2) if values else 0.0

    # -------------------------------------------------------------------------
    # Catalogue Registration
    # -------------------------------------------------------------------------
    def register_service(self):
        payload = {
            "id": self.id,
            "name": self.service_name,
            "type": self.service_type,
            "endpoint": f"http://{self.id}:{self.service_port}",
            "status": "active"
        }

        try:
            url = f"{self.catalog_url}/services"
            response = requests.post(url, json=payload, timeout=5)

            if response.status_code in (200, 201):
                try:
                    action = response.json().get("action")
                except ValueError:
                    action = None

                if action == "updated":
                    print("[CATALOGUE] Service information refreshed")
                else:
                    print(f"[CATALOGUE] Service registered successfully: {payload}")
                return True

            print(f"[CATALOGUE] Registration failed: {response.status_code} {response.text}")

        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")

        return False

    def registration_task(self):
        while not self.register_service():
            print(f"[CATALOGUE] Retrying registration in {self.registration_retry_delay} seconds...")
            time.sleep(self.registration_retry_delay)

    # -------------------------------------------------------------------------
    # MQTT Callbacks & Actions
    # -------------------------------------------------------------------------
    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe([(self.sub_topic, 2), (self.analysis_topic, 2)])
            print(f"[MQTT] Connected to {self.broker}:{self.port}")
            print(f"[MQTT] Subscribed to {self.sub_topic} and {self.analysis_topic}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload_str = message.payload.decode("utf-8")
            data = json.loads(payload_str)
            topic = message.topic

            # Determine message type by topic prefix
            analysis_base = self.analysis_topic.rstrip("#").rstrip("+").rstrip("/")
            if topic.startswith(analysis_base):
                self.process_analysis_message(data)
            else:
                self.process_sensor_message(topic, data)

        except json.JSONDecodeError:
            print("[MQTT] Invalid JSON payload received")
        except Exception as error:
            print(f"[MQTT] Processing error: {error}")

    def process_sensor_message(self, topic, sensor_data):
        device_id = sensor_data.get("device_id")
        if not device_id:
            print("[MQTT] Missing device_id in sensor payload")
            return

        sensor_data["_mqtt_topic"] = topic
        sensor_data["_received_at"] = now_utc_iso()

        with self.lock:
            self.latest_data[device_id] = sensor_data

        print(f"[MQTT] Received sensor telemetry from {device_id}: {sensor_data}")

    def process_analysis_message(self, analysis_data):
        device_id = analysis_data.get("device_id")
        sensor_type = analysis_data.get("sensor_type")
        state = analysis_data.get("state")
        value = analysis_data.get("value")

        if not device_id or not sensor_type or not state:
            return

        # Update latest_data device status if known
        with self.lock:
            if device_id in self.latest_data:
                self.latest_data[device_id]["status"] = "warning" if state in ("low", "high") else "normal"

        # Only generate alerts for "low" or "high" threshold violations
        if state not in ("low", "high"):
            return

        threshold = analysis_data.get("threshold_min") if state == "low" else analysis_data.get("threshold_max")
        threshold_type = "min" if state == "low" else "max"

        alert = {
            "device_id": device_id,
            "alert": f"{state}_{sensor_type}",
            "value": value,
            "threshold": threshold,
            "threshold_type": threshold_type,
            "timestamp": analysis_data.get("timestamp") or now_utc_iso()
        }

        print(f"[ANALYSIS] Triggering alert for {device_id}: {alert}")
        self.publish_alert(alert)

    def mqtt_loop(self):
        while True:
            try:
                self.mqtt_client.connect(self.broker, self.port, keepalive=60)
                self.mqtt_client.loop_forever()
            except Exception as error:
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {error}")
                time.sleep(5)

    def publish_alert(self, alert):
        device_id = alert.get("device_id")
        if not device_id:
            return

        if not self.mqtt_connected:
            print("[MQTT] Cannot publish alert: MQTT not connected")
            return

        topic = f"{self.pub_topic}/{device_id}"

        try:
            info = self.mqtt_client.publish(topic, json.dumps(alert), qos=1)

            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                with self.lock:
                    self.alerts.append(alert)
                    if len(self.alerts) > 200:
                        self.alerts = self.alerts[-200:]
                print(f"[MQTT] Published alert to {topic}: {alert}")
            else:
                print(f"[MQTT] Alert publish failed with rc={info.rc}")

        except Exception as error:
            print(f"[MQTT] Publish alert error: {error}")

    # -------------------------------------------------------------------------
    # Catalogue Merging & Devices List
    # -------------------------------------------------------------------------
    def fetch_catalogue_devices(self):
        try:
            url = f"{self.catalog_url}/devices"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                devices = data
            elif isinstance(data, dict):
                devices = data.get("devices", data)
                if isinstance(devices, dict):
                    devices = [{"id": k, **v} if isinstance(v, dict) else {"id": k} for k, v in devices.items()]
            else:
                devices = []

            return [d for d in devices if isinstance(d, dict) and (d.get("id") or d.get("device_id"))]

        except Exception as error:
            print(f"[CATALOGUE] Device list request error: {error}")
            return []

    def merged_devices(self):
        catalogue_devices = self.fetch_catalogue_devices()

        with self.lock:
            latest = {k: dict(v) for k, v in self.latest_data.items()}

        merged = {}
        for c_device in catalogue_devices:
            device_id = c_device.get("id") or c_device.get("device_id")
            telemetry = latest.get(device_id)

            if telemetry:
                merged[device_id] = telemetry
            else:
                merged[device_id] = {
                    "device_id": device_id,
                    "name": c_device.get("name"),
                    "type": c_device.get("type"),
                    "status": c_device.get("status", "registered"),
                    "temperature": None,
                    "soil_moisture": None,
                    "humidity": None,
                    "timestamp": None
                }

        for device_id, telemetry in latest.items():
            if device_id not in merged:
                merged[device_id] = telemetry

        return merged

    # -------------------------------------------------------------------------
    # Report Logic
    # -------------------------------------------------------------------------
    def generate_report(self, device_id, results=None):
        if results is None:
            results = self.default_results

        with self.lock:
            latest = self.latest_data.get(device_id)
            if latest:
                latest = dict(latest)

        if not latest:
            raise ValueError(f"Device '{device_id}' not found in local state")

        # Query ThingSpeak Adapter
        url = f"{self.thingspeak_adapter_url}/history"
        res = requests.get(url, params={"device_id": device_id, "results": results}, timeout=10)
        res.raise_for_status()

        history_data = res.json()
        feeds = history_data.get("feeds", [])

        temperatures, soil_moistures, humidities = [], [], []
        for feed in feeds:
            if feed.get("field1") not in (None, ""):
                temperatures.append(float(feed["field1"]))
            if feed.get("field2") not in (None, ""):
                soil_moistures.append(float(feed["field2"]))
            if feed.get("field3") not in (None, ""):
                humidities.append(float(feed["field3"]))

        return {
            "device_id": device_id,
            "latest_data": latest,
            "history_count": len(feeds),
            "averages": {
                "temperature": self.average(temperatures),
                "soil_moisture": self.average(soil_moistures),
                "humidity": self.average(humidities)
            },
            "message": "Report generated successfully"
        }

    # -------------------------------------------------------------------------
    # CherryPy REST Endpoints
    # -------------------------------------------------------------------------
    @cherrypy.tools.json_out()
    def GET(self, *path, **query):
        # GET /
        if len(path) == 0:
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

        resource = path[0]

        # GET /health
        if resource == "health":
            return {
                "status": "ok",
                "timestamp": now_utc_iso(),
                "service_id": self.id,
                "mqtt_connected": self.mqtt_connected
            }

        # GET /devices OR GET /devices/<device_id>
        if resource == "devices":
            devices = self.merged_devices()
            if len(path) == 2:
                device_id = path[1]
                device = devices.get(device_id)
                if not device:
                    raise cherrypy.HTTPError(404, f"Device '{device_id}' not found")
                return device

            return {"count": len(devices), "devices": devices}

        # GET /alerts
        if resource == "alerts":
            with self.lock:
                return {"count": len(self.alerts), "alerts": list(self.alerts)}

        # GET /report?device_id=...&results=...
        if resource == "report":
            device_id = query.get("device_id")
            results = query.get("results")

            if not device_id:
                raise cherrypy.HTTPError(400, "device_id is required")

            try:
                if results is not None:
                    results = int(results)
                    if results <= 0:
                        raise ValueError("results must be greater than zero")

                return self.generate_report(device_id, results)

            except ValueError as error:
                msg = str(error)
                code = 404 if "not found" in msg else 400
                raise cherrypy.HTTPError(code, msg)

            except requests.RequestException as error:
                raise cherrypy.HTTPError(500, f"Failed to fetch history: {error}")

            except Exception as error:
                raise cherrypy.HTTPError(500, f"Report error: {error}")

        raise cherrypy.HTTPError(404, "Endpoint not found")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------
    def start(self):
        threading.Thread(target=self.registration_task, daemon=True, name="registration-thread").start()
        threading.Thread(target=self.mqtt_loop, daemon=True, name="mqtt-thread").start()

    def stop(self):
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


# =============================================================================
# 4. MAIN ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    settings = json.load(open("config.json"))

    service_id = settings["service_id"]
    service_name = settings["service_name"]
    service_type = settings["service_type"]
    service_host = settings["service_host"]
    service_port = settings["service_port"]
    catalog_url = settings["catalog_url"]
    thingspeak_adapter_url = settings["thingspeak_adapter_url"]
    default_results = settings["default_results"]

    client_id = settings["mqtt_info"]["clientID"]
    broker = settings["mqtt_info"]["broker"]
    port = settings["mqtt_info"]["port"]
    sub_topic = settings["mqtt_info"]["sensor_topic"]
    analysis_topic = settings["mqtt_info"]["analysis_topic"]
    pub_topic = settings["mqtt_info"]["alert_topic_base"]

    service = AlertGeneratorService(
        service_id=service_id,
        service_name=service_name,
        service_type=service_type,
        service_host=service_host,
        service_port=service_port,
        catalog_url=catalog_url,
        thingspeak_adapter_url=thingspeak_adapter_url,
        default_results=default_results,
        client_id=client_id,
        broker=broker,
        port=port,
        sub_topic=sub_topic,
        analysis_topic=analysis_topic,
        pub_topic=pub_topic
    )
    service.start()

    print("[START] Alert Generator starting...")
    print(f"[INFO] Service ID: {service.id}")
    print(f"[INFO] MQTT Broker: {service.broker}:{service.port}")
    print(f"[INFO] Subscribed Topics: {service.sub_topic}, {service.analysis_topic}")

    service.start()

    cherrypy_config = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher(),
            "tools.sessions.on": True
        }
    }

    cherrypy.tree.mount(service, "/", cherrypy_config)
    cherrypy.config.update({
        "server.socket_host": service.service_host,
        "server.socket_port": service.service_port,
        "log.screen": True
    })

    cherrypy.engine.subscribe("stop", service.stop)
    cherrypy.engine.start()
    cherrypy.engine.block()
