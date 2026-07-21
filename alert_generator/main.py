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
# 2. RULE ENGINE CLASS
# =============================================================================
class AlertRuleEngine:

    def __init__(self, thresholds):
        self.thresholds = thresholds

    def generate_alerts(self, sensor_data):
        device_id = sensor_data.get("device_id")
        if not device_id:
            return []

        alerts = []
        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")

        # Temperature Checks
        if temperature is not None:
            temp_min = self.thresholds.get("temperature", {}).get("min")
            temp_max = self.thresholds.get("temperature", {}).get("max")

            if temp_min is not None and temperature < temp_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_temperature",
                    "value": temperature,
                    "threshold": temp_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })
            elif temp_max is not None and temperature > temp_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_temperature",
                    "value": temperature,
                    "threshold": temp_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        # Soil Moisture Checks
        if soil_moisture is not None:
            soil_min = self.thresholds.get("soil_moisture", {}).get("min")
            soil_max = self.thresholds.get("soil_moisture", {}).get("max")

            if soil_min is not None and soil_moisture < soil_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_soil_moisture",
                    "value": soil_moisture,
                    "threshold": soil_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })
            elif soil_max is not None and soil_moisture > soil_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_soil_moisture",
                    "value": soil_moisture,
                    "threshold": soil_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        # Humidity Checks
        if humidity is not None:
            hum_min = self.thresholds.get("humidity", {}).get("min")
            hum_max = self.thresholds.get("humidity", {}).get("max")

            if hum_min is not None and humidity < hum_min:
                alerts.append({
                    "device_id": device_id,
                    "alert": "low_humidity",
                    "value": humidity,
                    "threshold": hum_min,
                    "threshold_type": "min",
                    "timestamp": now_utc_iso()
                })
            elif hum_max is not None and humidity > hum_max:
                alerts.append({
                    "device_id": device_id,
                    "alert": "high_humidity",
                    "value": humidity,
                    "threshold": hum_max,
                    "threshold_type": "max",
                    "timestamp": now_utc_iso()
                })

        return alerts


# =============================================================================
# 3. ALERT GENERATOR SERVICE (CHERRYPY + MQTT)
# =============================================================================
class AlertGeneratorService:
    exposed = True

    def __init__(self, id, sub_topic, pub_topic, broker, port, config_file="/app/config.json"):
        # Explicit Professor-style parameters
        self.id = id
        self.sub_topic = sub_topic
        self.pub_topic = pub_topic
        self.broker = broker
        self.port = port
        self.config_file = config_file

        # Additional Service configurations
        self.service_name = os.environ.get("SERVICE_NAME", "Alert Generator")
        self.service_type = os.environ.get("SERVICE_TYPE", "alert_generator")
        self.service_host = os.environ.get("SERVICE_HOST", "0.0.0.0")
        self.service_port = int(os.environ.get("SERVICE_PORT", 8091))
        
        self.catalog_url = os.environ.get("CATALOG_URL", "http://catalogue:8000")
        self.registration_retry_delay = int(os.environ.get("REGISTRATION_RETRY_DELAY", 5))
        self.thingspeak_adapter_url = os.environ.get("THINGSPEAK_ADAPTER_URL", "http://thingspeak-adapter:8080")

        # Load file thresholds and defaults
        with open(self.config_file, "r", encoding="utf-8") as file:
            self.config = json.load(file)

        self.default_results = self.config.get("report", {}).get("default_results", 20)

        # State & Rule Engine Initialization
        self.lock = threading.RLock()
        self.latest_data = {}
        self.alerts = []
        self.rule_engine = AlertRuleEngine(self.config["thresholds"])

        # MQTT Client Setup
        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(client_id=self.id, clean_session=True)
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
            client.subscribe(self.sub_topic, qos=2)
            print(f"[MQTT] Connected to {self.broker}:{self.port}")
            print(f"[MQTT] Subscribed to {self.sub_topic}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload = message.payload.decode("utf-8")
            sensor_data = json.loads(payload)

            sensor_data["_mqtt_topic"] = message.topic
            sensor_data["_received_at"] = now_utc_iso()

            device_id = sensor_data.get("device_id")
            if not device_id:
                print("[MQTT] Missing device_id in payload")
                return

            with self.lock:
                self.latest_data[device_id] = sensor_data

            print(f"[MQTT] Received from {message.topic}: {sensor_data}")

            # Run Rule Engine
            generated_alerts = self.rule_engine.generate_alerts(sensor_data)
            for alert in generated_alerts:
                self.publish_alert(alert)

        except json.JSONDecodeError:
            print("[MQTT] Invalid JSON payload received")
        except Exception as error:
            print(f"[MQTT] Processing error: {error}")

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
            info = self.mqtt_client.publish(topic, json.dumps(alert), qos=2)
            info.wait_for_publish()

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
            with self.lock:
                if len(path) == 2:
                    device_id = path[1]
                    device = self.latest_data.get(device_id)
                    if not device:
                        raise cherrypy.HTTPError(404, f"Device '{device_id}' not found")
                    return dict(device)

                return {"count": len(self.latest_data), "devices": self.latest_data}

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
    service = AlertGeneratorService(
        id=os.environ.get("SERVICE_ID", "alert-generator"),
        sub_topic=os.environ.get("MQTT_SUB_TOPIC", "smartplant/sensors/#"),
        pub_topic=os.environ.get("MQTT_PUB_TOPIC", "smartplant/alerts"),
        broker=os.environ.get("MQTT_BROKER", "mosquitto"),
        port=int(os.environ.get("MQTT_PORT", 1883)),
        config_file=os.environ.get("CONFIG_FILE", "/app/config.json")
    )

    print("[START] Alert Generator starting...")
    print(f"[INFO] Service ID: {service.id}")
    print(f"[INFO] MQTT Broker: {service.broker}:{service.port}")
    print(f"[INFO] Subscribed Topic: {service.sub_topic}")
    print(f"[INFO] Alert Topic Base: {service.pub_topic}")

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