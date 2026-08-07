import json
import math
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class PlantSimulator:
    def __init__(self, device_id: str, sim_config: Dict[str, Any] = None) -> None:
        self.device_id = device_id
        self.state_lock = threading.Lock()
        
        sim_config = sim_config or {}
        self.base_temp = float(sim_config.get("base_temperature", 24.0))
        self.temp_amplitude = float(sim_config.get("temperature_amplitude", 5.5))
        self.base_humidity = float(sim_config.get("base_humidity", 60.0))
        self.hum_amplitude = float(sim_config.get("humidity_amplitude", 12.0))
        self.soil_decay_rate = float(sim_config.get("soil_decay_rate", 0.25))
        
        initial_moisture = float(sim_config.get("initial_soil_moisture", random.uniform(25.0, 60.0)))

        # Simulated environment state
        self.soil_moisture_value = initial_moisture
        self.temperature_bias = 0.0
        self.humidity_bias = 0.0
        self.last_sensor_update = time.time()
        
        # Simulated actuator/control state
        self.control_state = {
            "watering": "idle",
            "temperature_control": "idle",
            "humidity_control": "idle",
            "last_command": None,
            "last_command_time": None,
            "last_command_reason": None,
            "last_sensor_type": None
        }

    def day_fraction(self) -> float:
        now = datetime.now()
        seconds_today = now.hour * 3600 + now.minute * 60 + now.second
        return seconds_today / 86400

    def update_environment_state(self) -> None:
        now_ts = time.time()
        elapsed = now_ts - self.last_sensor_update
        self.last_sensor_update = now_ts

        elapsed_factor = elapsed / 10.0

        with self.state_lock:
            # Soil moisture naturally decays
            decay = self.soil_decay_rate * elapsed_factor

            if self.control_state["watering"] == "increase_requested":
                self.soil_moisture_value += 2.0 * elapsed_factor
            elif self.control_state["watering"] == "reduction_requested":
                self.soil_moisture_value -= 0.6 * elapsed_factor
            else:
                self.soil_moisture_value -= decay

            self.soil_moisture_value = max(10.0, min(95.0, self.soil_moisture_value))

            # Temperature bias evolves according to temperature control
            if self.control_state["temperature_control"] == "cooling":
                self.temperature_bias -= 0.25 * elapsed_factor
            elif self.control_state["temperature_control"] == "heating":
                self.temperature_bias += 0.25 * elapsed_factor
            else:
                self.temperature_bias *= 0.97

            self.temperature_bias = max(-8.0, min(8.0, self.temperature_bias))

            # Humidity bias evolves according to humidity control
            if self.control_state["humidity_control"] == "increase_requested":
                self.humidity_bias += 0.5 * elapsed_factor
            elif self.control_state["humidity_control"] == "decrease_requested":
                self.humidity_bias -= 0.5 * elapsed_factor
            else:
                self.humidity_bias *= 0.97

            self.humidity_bias = max(-20.0, min(20.0, self.humidity_bias))

    def read_temperature(self) -> float:
        frac = self.day_fraction()
        base = self.base_temp + self.temp_amplitude * math.sin(2 * math.pi * frac)
        noise = random.uniform(-0.6, 0.6)

        with self.state_lock:
            value = base + self.temperature_bias + noise

        return round(max(5.0, min(45.0, value)), 1)

    def read_soil_moisture(self) -> float:
        with self.state_lock:
            noise = random.uniform(-0.5, 0.5)
            value = self.soil_moisture_value + noise

        return round(max(0.0, min(100.0, value)), 1)

    def read_humidity(self) -> float:
        frac = self.day_fraction()
        base = self.base_humidity - self.hum_amplitude * math.sin(2 * math.pi * frac)
        noise = random.uniform(-1.5, 1.5)

        with self.state_lock:
            value = base + self.humidity_bias + noise

        return round(max(10.0, min(100.0, value)), 1)

    def collect_data(self) -> Dict[str, Any]:
        self.update_environment_state()
        temp = self.read_temperature()
        soil = self.read_soil_moisture()
        hum = self.read_humidity()
        ts_iso = now_utc_iso()
        ts_epoch = time.time()

        return {
            "bn": self.device_id,
            "bt": ts_epoch,
            "e": [
                {"n": "temperature", "v": temp, "u": "Cel"},
                {"n": "soil_moisture", "v": soil, "u": "%RH"},
                {"n": "humidity", "v": hum, "u": "%RH"}
            ],
            # Flat attributes for backward compatibility
            "device_id": self.device_id,
            "temperature": temp,
            "soil_moisture": soil,
            "humidity": hum,
            "timestamp": ts_iso
        }

    def handle_command(self, command_payload: Dict[str, Any]) -> None:
        command = command_payload.get("command")
        reason = command_payload.get("reason")
        sensor_type = command_payload.get("sensor_type")

        with self.state_lock:
            self.control_state["last_command"] = command
            self.control_state["last_command_time"] = now_utc_iso()
            self.control_state["last_command_reason"] = reason
            self.control_state["last_sensor_type"] = sensor_type

            if command == "increase_watering":
                self.control_state["watering"] = "increase_requested"
                action_message = "Watering increase requested"

            elif command == "reduce_watering":
                self.control_state["watering"] = "reduction_requested"
                action_message = "Watering reduction requested"

            elif command == "stop_watering_adjustment":
                self.control_state["watering"] = "idle"
                action_message = "Watering adjustment stopped"

            elif command == "start_cooling":
                self.control_state["temperature_control"] = "cooling"
                action_message = "Cooling activated"

            elif command == "start_heating":
                self.control_state["temperature_control"] = "heating"
                action_message = "Heating activated"

            elif command == "stop_temperature_control":
                self.control_state["temperature_control"] = "idle"
                action_message = "Temperature control stopped"

            elif command == "increase_humidity":
                self.control_state["humidity_control"] = "increase_requested"
                action_message = "Humidity increase requested"

            elif command == "decrease_humidity":
                self.control_state["humidity_control"] = "decrease_requested"
                action_message = "Humidity decrease requested"

            elif command == "stop_humidity_adjustment":
                self.control_state["humidity_control"] = "idle"
                action_message = "Humidity adjustment stopped"

            else:
                action_message = f"Unknown command received: {command}"

        print(f"[ACTION] {action_message} for {self.device_id}")
        self.print_control_state()

    def print_control_state(self) -> None:
        with self.state_lock:
            snapshot = {
                "device_id": self.device_id,
                "watering": self.control_state["watering"],
                "temperature_control": self.control_state["temperature_control"],
                "humidity_control": self.control_state["humidity_control"],
                "last_command": self.control_state["last_command"],
                "last_command_time": self.control_state["last_command_time"],
                "last_command_reason": self.control_state["last_command_reason"],
                "last_sensor_type": self.control_state["last_sensor_type"]
            }

        print("[CONTROL] Updated simulated control state:")
        print(json.dumps(snapshot, indent=2))
