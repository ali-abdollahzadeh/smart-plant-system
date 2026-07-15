from datetime import datetime, timezone
from typing import Any, Dict, List

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

class AlertRuleEngine:
    def __init__(self, thresholds: Dict[str, Any]) -> None:
        self.thresholds = thresholds

    def generate_alerts(self, sensor_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        device_id = sensor_data["device_id"]
        alerts = []

        temperature = sensor_data.get("temperature")
        soil_moisture = sensor_data.get("soil_moisture")
        humidity = sensor_data.get("humidity")

        if temperature is not None:
            temp_min = self.thresholds["temperature"]["min"]
            temp_max = self.thresholds["temperature"]["max"]

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

        if soil_moisture is not None:
            soil_min = self.thresholds["soil_moisture"]["min"]
            soil_max = self.thresholds["soil_moisture"]["max"]

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

        if humidity is not None:
            hum_min = self.thresholds["humidity"]["min"]
            hum_max = self.thresholds["humidity"]["max"]

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
