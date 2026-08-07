import json
import os
import threading
import time
import requests
import paho.mqtt.client as mqtt
from simulator import PlantSimulator


class SensorNode:

    def __init__(self, device_id, catalog_url, broker, port, sensor_topic_base, command_topic_base, publish_interval=10, sim_config=None):
        self.id = os.environ.get("DEVICE_ID", device_id)
        self.catalog_url = os.environ.get("CATALOG_URL", catalog_url).rstrip("/")
        self.broker = os.environ.get("MQTT_BROKER", broker)
        self.port = int(os.environ.get("MQTT_PORT", port))
        self.publish_interval = int(os.environ.get("PUBLISH_INTERVAL", publish_interval))

        sensor_base = os.environ.get("MQTT_TOPIC_BASE", sensor_topic_base)
        command_base = os.environ.get("MQTT_COMMAND_TOPIC_BASE", command_topic_base)
        self.sensor_topic = f"{sensor_base}/{self.id}"
        self.command_topic = f"{command_base}/{self.id}"

        self.running = True
        self.mqtt_connected = False
        self.simulator = PlantSimulator(self.id, sim_config=sim_config)

        # MQTT setup with unique client ID
        self.mqtt_client = mqtt.Client(client_id=f"rpi-{self.id}", clean_session=True)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

    def register_device(self):
        payload = {
            "id": self.id,
            "name": f"Plant Sensor Node {self.id}",
            "type": "sensor_node",
            "mqtt_topic": self.sensor_topic,
            "command_topic": self.command_topic,
            "status": "active"
        }
        try:
            url = f"{self.catalog_url}/devices"
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code in (200, 201, 409):
                print(f"[CATALOGUE] Registered device {self.id}")
                return True
        except requests.RequestException as error:
            print(f"[CATALOGUE] Registration error: {error}")
        return False

    def registration_task(self):
        while self.running and not self.register_device():
            time.sleep(5)

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.command_topic, qos=2)
            print(f"[MQTT] Connected to {self.broker}:{self.port}, subscribed to {self.command_topic}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False

    def on_mqtt_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            print(f"[MQTT] Command received on {message.topic}: {payload}")
            self.simulator.handle_command(payload)
        except Exception as error:
            print(f"[MQTT] Command error: {error}")

    def mqtt_loop(self):
        while self.running:
            try:
                self.mqtt_client.connect(self.broker, self.port, keepalive=60)
                self.mqtt_client.loop_forever()
            except Exception as error:
                self.mqtt_connected = False
                time.sleep(5)

    def publish_loop(self):
        while self.running:
            if self.mqtt_connected:
                try:
                    data = self.simulator.collect_data()
                    info = self.mqtt_client.publish(self.sensor_topic, json.dumps(data), qos=2)
                    info.wait_for_publish()
                    print(f"[MQTT] Published to {self.sensor_topic}: {data}")
                except Exception as error:
                    print(f"[MQTT] Publish error: {error}")
            time.sleep(self.publish_interval)

    def run(self):
        print(f"[START] Sensor node {self.id} running...")
        threading.Thread(target=self.registration_task, daemon=True).start()
        threading.Thread(target=self.mqtt_loop, daemon=True).start()
        threading.Thread(target=self.publish_loop, daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        self.running = False
        try:
            self.mqtt_client.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    settings = json.load(open("config.json"))

    catalog_url = settings["catalog_url"]
    device_id = settings["device_id"]
    publish_interval = settings["publish_interval"]

    broker = settings["mqtt_info"]["broker"]
    port = settings["mqtt_info"]["port"]
    sensor_topic_base = settings["mqtt_info"]["sensor_topic_base"]
    command_topic_base = settings["mqtt_info"]["command_topic_base"]
    sim_config = settings.get("simulation", {})

    node = SensorNode(
        device_id=device_id,
        catalog_url=catalog_url,
        broker=broker,
        port=port,
        sensor_topic_base=sensor_topic_base,
        command_topic_base=command_topic_base,
        publish_interval=publish_interval,
        sim_config=sim_config
    )
    node.run()