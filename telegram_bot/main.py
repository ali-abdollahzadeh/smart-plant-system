import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

import requests
import telepot
from telepot.loop import MessageLoop
from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton
import paho.mqtt.client as mqtt


class TelegramPlantBot:
    def __init__(self) -> None:
        self.runtime = self.load_runtime_config()
        self.config = self.load_json(self.runtime["config_file"])

        token = self.runtime["telegram_bot_token"]
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")

        self.bot = telepot.Bot(token)

        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.register_handlers()

    def load_json(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_runtime_config(self) -> Dict[str, Any]:
        return {
            "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
            "alert_generator_url": os.environ.get("ALERT_GENERATOR_URL", "http://alert-generator:8091"),
            "service_id": os.environ.get("SERVICE_ID", "telegram-bot"),
            "service_name": os.environ.get("SERVICE_NAME", "Telegram Bot"),
            "service_type": os.environ.get("SERVICE_TYPE", "telegram_bot"),
            "register_interval": int(os.environ.get("REGISTER_INTERVAL", 60)),
            "config_file": os.environ.get("CONFIG_FILE", "/app/config.json"),
            "mqtt_broker": os.environ.get("MQTT_BROKER", "mosquitto"),
            "mqtt_port": int(os.environ.get("MQTT_PORT", 1883)),
            "mqtt_alert_topic": os.environ.get("MQTT_ALERT_TOPIC", "smartplant/alerts/#"),
            "mqtt_command_topic_base": os.environ.get("MQTT_COMMAND_TOPIC_BASE", "smartplant/commands"),
        }

    def msg(self, key: str, default: str = "") -> str:
        return self.config.get("messages", {}).get(key, default)

    def safe_get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        resp = requests.get(url, params=params, timeout=15)
        return resp.json()

    def get_catalogue_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        data = self.safe_get_json(f"{self.runtime['catalog_url']}/users")
        for user in data.get("users", []):
            if str(user.get("telegram_id")).strip() == str(telegram_id).strip():
                return user
        return None

    def get_authorized_users(self) -> List[int]:
        data = self.safe_get_json(f"{self.runtime['catalog_url']}/users")
        users = data.get("users", [])
        return [
            int(user["telegram_id"])
            for user in users
            if str(user.get("status", "active")).strip().lower() == "active"
        ]

    def get_user_devices(self, telegram_id: int) -> List[str]:
        user = self.get_catalogue_user(telegram_id)
        if not user:
            return []
        return user.get("devices", [])

    def is_authorized_user_id(self, telegram_id: int) -> bool:
        user = self.get_catalogue_user(telegram_id)
        if not user:
            return False
        return str(user.get("status", "active")).strip().lower() == "active"

    def is_device_allowed_for_user(self, telegram_id: int, device_id: str) -> bool:
        return device_id in self.get_user_devices(telegram_id)

    def register_service(self) -> None:
        payload = {
            "id": self.runtime["service_id"],
            "name": self.runtime["service_name"],
            "type": self.runtime["service_type"],
            "endpoint": "telegram-bot",
            "status": "active"
        }
        try:
            requests.post(f"{self.runtime['catalog_url']}/services", json=payload, timeout=10)
        except Exception:
            print("Error occurred while registering service")

    def registration_loop(self) -> None:
        while True:
            self.register_service()
            time.sleep(self.runtime["register_interval"])

    def get_devices_map(self) -> Dict[str, Any]:
        data = self.safe_get_json(f"{self.runtime['catalog_url']}/devices")
        devices = data.get("devices", [])
        return {device["id"]: device for device in devices if "id" in device}

    def get_allowed_devices_map(self, telegram_id: int) -> Dict[str, Any]:
        all_devices = self.get_devices_map()
        allowed_devices = self.get_user_devices(telegram_id)
        return {
            device_id: all_devices.get(device_id, {"id": device_id, "status": "registered"})
            for device_id in allowed_devices
        }

    def get_live_device_status(self, device_id: str) -> Dict[str, Any]:
        try:
            return self.safe_get_json(f"{self.runtime['alert_generator_url']}/devices/{device_id}")
        except Exception:
            devices = self.get_devices_map()
            return devices.get(device_id, {"id": device_id, "status": "registered"})

    def command_topic(self, device_id: str) -> str:
        return f"{self.runtime['mqtt_command_topic_base']}/{device_id}"

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.runtime["mqtt_alert_topic"])
        else:
            self.mqtt_connected = False

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            self.notify_users_for_alert(payload)
        except Exception:
            print("Error processing MQTT message:", msg.payload)

    def mqtt_loop(self) -> None:
        while True:
            try:
                self.mqtt_client.connect(self.runtime["mqtt_broker"], self.runtime["mqtt_port"], keepalive=60)
                self.mqtt_client.loop_forever()
            except Exception:
                self.mqtt_connected = False
                time.sleep(5)

    def publish_command(self, device_id: str, command: str, reason: str, sensor_type: str) -> bool:
        if not self.mqtt_connected:
            return False
        payload = {
            "device_id": device_id,
            "command": command,
            "reason": reason,
            "sensor_type": sensor_type,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        topic = self.command_topic(device_id)
        try:
            info = self.mqtt_client.publish(topic, json.dumps(payload), qos=1)
            info.wait_for_publish()
            return info.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception:
            return False

    def format_alert_message(self, alert: Dict[str, Any]) -> str:
        return (
            f"🚨 Alert for {alert.get('device_id')}\n"
            f"Type: {alert.get('alert')}\n"
            f"Value: {alert.get('value')}\n"
            f"Threshold: {alert.get('threshold')}\n"
            f"Time: {alert.get('timestamp')}"
        )

    def alert_actions_keyboard(self, device_id: str, alert_type: str) -> InlineKeyboardMarkup:
        rows = []
        if "temperature" in alert_type:
            rows.append([
                InlineKeyboardButton(text="❄️ Start Cooling", callback_data=f"cmd:{device_id}:start_cooling:temperature"),
                InlineKeyboardButton(text="🔥 Start Heating", callback_data=f"cmd:{device_id}:start_heating:temperature")
            ])
        elif "humidity" in alert_type:
            rows.append([
                InlineKeyboardButton(text="💨 Increase Humidity", callback_data=f"cmd:{device_id}:increase_humidity:humidity"),
                InlineKeyboardButton(text="🌬 Decrease Humidity", callback_data=f"cmd:{device_id}:decrease_humidity:humidity")
            ])
        elif "soil_moisture" in alert_type:
            rows.append([
                InlineKeyboardButton(text="💧 Increase Watering", callback_data=f"cmd:{device_id}:increase_watering:soil_moisture"),
                InlineKeyboardButton(text="🚱 Reduce Watering", callback_data=f"cmd:{device_id}:reduce_watering:soil_moisture")
            ])
        rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="menu_back")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def notify_users_for_alert(self, alert: Dict[str, Any]) -> None:
        device_id = alert.get("device_id")
        alert_type = alert.get("alert", "")
        if not device_id:
            return
        for telegram_id in self.get_authorized_users():
            if self.is_device_allowed_for_user(telegram_id, device_id):
                try:
                    self.bot.sendMessage(telegram_id, self.format_alert_message(alert),
                                         reply_markup=self.alert_actions_keyboard(device_id, alert_type))
                except Exception:
                    print(f"Failed to send alert to user {telegram_id}")

    def format_device_status(self, device_id: str, data: Dict[str, Any]) -> str:
        return (
            f"🪴 Device: {device_id}\n"
            f"🌡 Temperature: {data.get('temperature', 'N/A')}\n"
            f"💧 Soil Moisture: {data.get('soil_moisture', 'N/A')}\n"
            f"💨 Humidity: {data.get('humidity', 'N/A')}\n"
            f"📌 Status: {data.get('status', 'N/A')}\n"
            f"⏱ Timestamp: {data.get('timestamp', 'N/A')}"
        )

    def format_alerts(self, alerts: List[Dict[str, Any]]) -> str:
        if not alerts:
            return self.msg("no_alerts", "✅ No alerts available.")
        lines = ["🚨 Recent Alerts:"]
        for alert in alerts[-5:]:
            lines.append(
                f"• {alert.get('device_id')} | {alert.get('alert')} | "
                f"value={alert.get('value')} | threshold={alert.get('threshold')}"
            )
        return "\n".join(lines)

    def format_report(self, report: Dict[str, Any]) -> str:
        averages = report.get("averages", {})
        latest = report.get("latest_data", {})
        return (
            f"📊 Report for {report.get('device_id')}\n\n"
            f"Latest Data:\n"
            f"🌡 Temperature: {latest.get('temperature', 'N/A')}\n"
            f"💧 Soil Moisture: {latest.get('soil_moisture', 'N/A')}\n"
            f"💨 Humidity: {latest.get('humidity', 'N/A')}\n\n"
            f"Averages:\n"
            f"🌡 Temperature: {averages.get('temperature', 'N/A')}\n"
            f"💧 Soil Moisture: {averages.get('soil_moisture', 'N/A')}\n"
            f"💨 Humidity: {averages.get('humidity', 'N/A')}\n\n"
            f"🗂 History Count: {report.get('history_count', 0)}"
        )

    def main_menu_keyboard(self) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton(text="🪴 Devices", callback_data="menu_devices"),
             InlineKeyboardButton(text="🚨 Alerts", callback_data="menu_alerts")],
            [InlineKeyboardButton(text="📊 Report", callback_data="menu_report_devices"),
             InlineKeyboardButton(text="⚙️ Commands", callback_data="menu_command_devices")],
            [InlineKeyboardButton(text="❓ Help", callback_data="menu_help")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def devices_keyboard_for_user(self, telegram_id: int, action_prefix: str) -> InlineKeyboardMarkup:
        devices = self.get_allowed_devices_map(telegram_id)
        rows = []
        for device_id in devices.keys():
            rows.append([InlineKeyboardButton(text=f"🪴 {device_id}", callback_data=f"{action_prefix}:{device_id}")])
        rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="menu_back")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def command_actions_keyboard(self, device_id: str) -> InlineKeyboardMarkup:
        rows = [
            [InlineKeyboardButton(text="❄️ Start Cooling", callback_data=f"cmd:{device_id}:start_cooling:temperature"),
             InlineKeyboardButton(text="🔥 Start Heating", callback_data=f"cmd:{device_id}:start_heating:temperature")],
            [InlineKeyboardButton(text="💧 Increase Watering", callback_data=f"cmd:{device_id}:increase_watering:soil_moisture"),
             InlineKeyboardButton(text="🚱 Reduce Watering", callback_data=f"cmd:{device_id}:reduce_watering:soil_moisture")],
            [InlineKeyboardButton(text="💨 Increase Humidity", callback_data=f"cmd:{device_id}:increase_humidity:humidity"),
             InlineKeyboardButton(text="🌬 Decrease Humidity", callback_data=f"cmd:{device_id}:decrease_humidity:humidity")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="menu_back")]
        ]
        return InlineKeyboardMarkup(inline_keyboard=rows)

    def register_handlers(self) -> None:
        MessageLoop(self.bot, {'chat': self.on_chat_message, 'callback_query': self.on_callback_query}).run_as_thread()

    def require_authorization_message(self, msg: Dict[str, Any]) -> bool:
        telegram_id = msg.get("from", {}).get("id")
        if not self.is_authorized_user_id(telegram_id):
            chat_id = msg.get("chat", {}).get("id")
            self.bot.sendMessage(chat_id, f"🚫 You are not authorized.\nYour Telegram ID is: {telegram_id}")
            return False
        return True

    def require_authorization_callback(self, call: Dict[str, Any]) -> bool:
        telegram_id = call.get("from", {}).get("id")
        if not self.is_authorized_user_id(telegram_id):
            self.bot.answerCallbackQuery(call.get("id"), text=f"🚫 You are not authorized.\nYour Telegram ID is: {telegram_id}", show_alert=True)
            return False
        return True

    def on_chat_message(self, msg: Dict[str, Any]) -> None:
        text = msg.get("text", "") or ""
        if text.startswith("/start"):
            self.handle_start(msg)
        elif text.startswith("/help"):
            self.handle_help(msg)
        elif text.startswith("/devices"):
            self.handle_devices(msg)
        elif text.startswith("/status"):
            self.handle_status(msg)
        elif text.startswith("/alerts"):
            self.handle_alerts(msg)
        elif text.startswith("/report"):
            self.handle_report(msg)
        else:
            self.handle_unknown(msg)


    def handle_start(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        chat_id = msg.get("chat", {}).get("id")
        self.bot.sendMessage(chat_id, self.msg("welcome", "🪴 Welcome to the Smart Plant Care System Bot.\nChoose an action below."), reply_markup=self.main_menu_keyboard())

    def handle_help(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        chat_id = msg.get("chat", {}).get("id")
        self.bot.sendMessage(chat_id, self.msg("help", "Use /start to open the menu."))

    def handle_devices(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        chat_id = msg.get("chat", {}).get("id")
        devices = self.get_allowed_devices_map(msg.get("from", {}).get("id"))
        if not devices:
            self.bot.sendMessage(chat_id, self.msg("devices_not_found", "⚠️ No devices found."))
            return
        text = "🪴 Your Devices:\n" + "\n".join(f"• {device_id}" for device_id in devices.keys())
        self.bot.sendMessage(chat_id, text)

    def handle_status(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        text = (msg.get("text") or "").strip().split()
        chat_id = msg.get("chat", {}).get("id")
        if len(text) < 2:
            self.bot.sendMessage(chat_id, self.msg("device_id_required", "🌿 Please provide a device_id.\nExample: /status raspi-01"))
            return
        device_id = text[1]
        if not self.is_device_allowed_for_user(msg.get("from", {}).get("id"), device_id):
            self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            return
        data = self.get_live_device_status(device_id)
        self.bot.sendMessage(chat_id, self.format_device_status(device_id, data))

    def handle_alerts(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        chat_id = msg.get("chat", {}).get("id")
        data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
        alerts = [alert for alert in data.get("alerts", []) if self.is_device_allowed_for_user(msg.get("from", {}).get("id"), alert.get("device_id", ""))]
        self.bot.sendMessage(chat_id, self.format_alerts(alerts))

    def handle_report(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        parts = (msg.get("text") or "").strip().split()
        chat_id = msg.get("chat", {}).get("id")
        if len(parts) < 2:
            self.bot.sendMessage(chat_id, self.msg("report_device_id_required", "🌿 Please provide a device_id.\nExample: /report raspi-01"))
            return
        device_id = parts[1]
        if not self.is_device_allowed_for_user(msg.get("from", {}).get("id"), device_id):
            self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            return
        report = self.safe_get_json(f"{self.runtime['alert_generator_url']}/report", params={"device_id": device_id})
        self.bot.sendMessage(chat_id, self.format_report(report))

    def handle_unknown(self, msg: Dict[str, Any]) -> None:
        if not self.require_authorization_message(msg):
            return
        chat_id = msg.get("chat", {}).get("id")
        self.bot.sendMessage(chat_id, self.msg("unknown_command", "🤖 Unknown command. Use /start or /help"))

    def on_callback_query(self, call: Dict[str, Any]) -> None:
        if not self.require_authorization_callback(call):
            return
        telegram_id = call.get("from", {}).get("id")
        chat_id = (call.get("message") or {}).get("chat", {}).get("id")
        data = call.get("data", "") or ""

        if data == "menu_back":
            self.bot.sendMessage(chat_id, self.msg("menu_title", "🌱 Main Menu"), reply_markup=self.main_menu_keyboard())
        elif data == "menu_help":
            self.bot.sendMessage(chat_id, self.msg("help", "Use /start to open the menu."), reply_markup=self.main_menu_keyboard())
        elif data == "menu_devices":
            devices = self.get_allowed_devices_map(telegram_id)
            if not devices:
                self.bot.sendMessage(chat_id, self.msg("devices_not_found", "⚠️ No devices found."), reply_markup=self.main_menu_keyboard())
            else:
                self.bot.sendMessage(chat_id, self.msg("choose_device", "🪴 Select one of your devices:"), reply_markup=self.devices_keyboard_for_user(telegram_id, "status"))
        elif data == "menu_report_devices":
            devices = self.get_allowed_devices_map(telegram_id)
            if not devices:
                self.bot.sendMessage(chat_id, self.msg("devices_not_found", "⚠️ No devices found."), reply_markup=self.main_menu_keyboard())
            else:
                self.bot.sendMessage(chat_id, self.msg("choose_report_device", "📊 Select one of your devices for the report:"), reply_markup=self.devices_keyboard_for_user(telegram_id, "report"))
        elif data == "menu_command_devices":
            devices = self.get_allowed_devices_map(telegram_id)
            if not devices:
                self.bot.sendMessage(chat_id, self.msg("devices_not_found", "⚠️ No devices found."), reply_markup=self.main_menu_keyboard())
            else:
                self.bot.sendMessage(chat_id, self.msg("choose_command_device", "⚙️ Select a device to control:"), reply_markup=self.devices_keyboard_for_user(telegram_id, "command_device"))
        elif data == "menu_alerts":
            data_json = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
            alerts = [alert for alert in data_json.get("alerts", []) if self.is_device_allowed_for_user(telegram_id, alert.get("device_id", ""))]
            self.bot.sendMessage(chat_id, self.format_alerts(alerts), reply_markup=self.main_menu_keyboard())
        elif data.startswith("status:"):
            device_id = data.split(":", 1)[1]
            if not self.is_device_allowed_for_user(telegram_id, device_id):
                self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            else:
                data_live = self.get_live_device_status(device_id)
                self.bot.sendMessage(chat_id, self.format_device_status(device_id, data_live), reply_markup=self.devices_keyboard_for_user(telegram_id, "status"))
        elif data.startswith("report:"):
            device_id = data.split(":", 1)[1]
            if not self.is_device_allowed_for_user(telegram_id, device_id):
                self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            else:
                report = self.safe_get_json(f"{self.runtime['alert_generator_url']}/report", params={"device_id": device_id})
                self.bot.sendMessage(chat_id, self.format_report(report), reply_markup=self.devices_keyboard_for_user(telegram_id, "report"))
        elif data.startswith("command_device:"):
            device_id = data.split(":", 1)[1]
            if not self.is_device_allowed_for_user(telegram_id, device_id):
                self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            else:
                self.bot.sendMessage(chat_id, f"{self.msg('choose_command_action', '⚙️ Choose a command for this device:')}\nDevice: {device_id}", reply_markup=self.command_actions_keyboard(device_id))
        elif data.startswith("cmd:"):
            parts = data.split(":")
            if len(parts) != 4:
                self.bot.answerCallbackQuery(call.get("id"))
                return
            _, device_id, command, sensor_type = parts
            if not self.is_device_allowed_for_user(telegram_id, device_id):
                self.bot.sendMessage(chat_id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            else:
                success = self.publish_command(device_id=device_id, command=command, reason="telegram_user_command", sensor_type=sensor_type)
                if success:
                    self.bot.sendMessage(chat_id, f"{self.msg('command_sent', '✅ Command sent successfully.')}\nDevice: {device_id}\nCommand: {command}", reply_markup=self.command_actions_keyboard(device_id))
                else:
                    self.bot.sendMessage(chat_id, f"❌ Failed to send command.\nDevice: {device_id}\nCommand: {command}", reply_markup=self.command_actions_keyboard(device_id))

        self.bot.answerCallbackQuery(call.get("id"))

    def run(self) -> None:
        threading.Thread(target=self.registration_loop, daemon=True).start()
        threading.Thread(target=self.mqtt_loop, daemon=True).start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down Telegram Plant Bot...")


if __name__ == "__main__":
    app = TelegramPlantBot()
    app.run()