import random
import time
import json
import requests
import paho.mqtt.publish as publish
import sys
import os

class DeviceInfo:

    def simulate_temperature_sensor(self):
        return round(random.uniform(15.0, 30.0), 2) #various temperatures in Celsius

    def simulate_soil_moisture_sensor(self):
        return round(random.uniform(30.0, 70.0), 2)

    def simulate_light_sensor(self):
        return round(random.uniform(100.0, 1000.0), 2)

    def read_sensors(self):
        return {
            "temperature": self.simulate_temperature_sensor(),
            "soil_moisture": self.simulate_soil_moisture_sensor(),
            "light": self.simulate_light_sensor(),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    def send_sensor_data(self):
        while True:
            sensor_data = self.read_sensors()
            sensor_data["device_id"] = DEVICE_INFO["device_id"]
            payload = json.dumps(sensor_data)
            try:
                print("connecting to MQTT broker at", BROKER)
                publish.single(TOPIC+'/'+sensor_data["device_id"], payload, hostname=BROKER)
                print("Published sensor data:", payload," on topic ", TOPIC+'/'+sensor_data["device_id"])
            except Exception as e:
                print("Failed to publish sensor data:", e)
            time.sleep(PUBLISH_INTERVAL)