import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

import requests
import telebot
import paho.mqtt.client as mqtt
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


class TelegramPlantBot:
    def __init__(self) -> None:
        self.runtime = self.load_runtime_config()
        self.config = self.load_json(self.runtime["config_file"])

        token = self.runtime["telegram_bot_token"]
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")

        self.bot = telebot.TeleBot(token)

        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        self.mqtt_client.on_message = self.on_mqtt_message

        self.register_handlers()

    # --------------------------------------------------
    # Config
    # --------------------------------------------------
    def load_json(self, path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_runtime_config(self) -> Dict[str, Any]:
        return {
            "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            "catalog_url": os.environ.get("CATALOG_URL", "http://catalogue:8000"),
            "alert_generator_url": os.environ.get("ALERT_GENERATOR_URL", "http://alert-generator:8091"),
            "analytics_url": os.environ.get("ANALYTICS_URL", "http://analytics:8090"),
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

    # --------------------------------------------------
    # REST helpers
    # --------------------------------------------------
    def safe_get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    # --------------------------------------------------
    # Catalogue authorization
    # --------------------------------------------------
    def get_catalogue_user(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        try:
            data = self.safe_get_json(f"{self.runtime['catalog_url']}/users")
            users = data.get("users", [])

            for user in users:
                if str(user.get("telegram_id")).strip() == str(telegram_id).strip():
                    return user

            print(f"[CATALOGUE] Telegram user {telegram_id} not found")
            return None

        except Exception as e:
            print(f"[CATALOGUE] Failed to fetch users from Catalogue: {e}")
            return None

    def get_authorized_users(self) -> List[int]:
        try:
            data = self.safe_get_json(f"{self.runtime['catalog_url']}/users")
            users = data.get("users", [])

            return [
                int(user["telegram_id"])
                for user in users
                if str(user.get("status", "active")).strip().lower() == "active"
            ]

        except Exception as e:
            print(f"[CATALOGUE] Failed to fetch authorized users: {e}")
            return []

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

    def require_authorization_message(self, message) -> bool:
        telegram_id = message.from_user.id

        if not self.is_authorized_user_id(telegram_id):
            self.bot.reply_to(
                message,
                f"🚫 You are not authorized.\nYour Telegram ID is: {telegram_id}"
            )
            print(f"[SECURITY] Unauthorized access attempt from Telegram ID: {telegram_id}")
            return False

        return True

    def require_authorization_callback(self, call) -> bool:
        telegram_id = call.from_user.id

        if not self.is_authorized_user_id(telegram_id):
            self.bot.answer_callback_query(
                call.id,
                f"🚫 You are not authorized.\nYour Telegram ID is: {telegram_id}",
                show_alert=True
            )
            print(f"[SECURITY] Unauthorized callback attempt from Telegram ID: {telegram_id}")
            return False

        return True

    # --------------------------------------------------
    # Catalogue service registration
    # --------------------------------------------------
    def register_service(self) -> None:
        payload = {
            "id": self.runtime["service_id"],
            "name": self.runtime["service_name"],
            "type": self.runtime["service_type"],
            "endpoint": "telegram-bot",
            "status": "active"
        }

        try:
            response = requests.post(
                f"{self.runtime['catalog_url']}/services",
                json=payload,
                timeout=10
            )

            if response.status_code in (200, 201):
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
    # Devices
    # --------------------------------------------------
    def get_devices_map(self) -> Dict[str, Any]:
        data = self.safe_get_json(f"{self.runtime['catalog_url']}/devices")
        devices = data.get("devices", [])

        return {
            device["id"]: device
            for device in devices
            if "id" in device
        }

    def get_allowed_devices_map(self, telegram_id: int) -> Dict[str, Any]:
        all_devices = self.get_devices_map()
        allowed_devices = self.get_user_devices(telegram_id)

        return {
            device_id: all_devices.get(device_id, {"id": device_id, "status": "registered"})
            for device_id in allowed_devices
        }

    def get_live_device_status(self, device_id: str) -> Dict[str, Any]:
        try:
            return self.safe_get_json(
                f"{self.runtime['alert_generator_url']}/devices/{device_id}"
            )
        except Exception:
            catalogue_devices = self.get_devices_map()
            return catalogue_devices.get(device_id, {"id": device_id, "status": "registered"})

    # --------------------------------------------------
    # MQTT
    # --------------------------------------------------
    def command_topic(self, device_id: str) -> str:
        return f"{self.runtime['mqtt_command_topic_base']}/{device_id}"

    def on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.mqtt_connected = True
            client.subscribe(self.runtime["mqtt_alert_topic"])
            print(f"[MQTT] Connected to {self.runtime['mqtt_broker']}:{self.runtime['mqtt_port']}")
            print(f"[MQTT] Subscribed to {self.runtime['mqtt_alert_topic']}")
        else:
            self.mqtt_connected = False
            print(f"[MQTT] Connection failed with rc={rc}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        print(f"[MQTT] Disconnected with rc={rc}")

    def on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            print(f"[MQTT] Alert received: {payload}")
            self.notify_users_for_alert(payload)
        except Exception as e:
            print(f"[MQTT] Alert processing error: {e}")

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
                self.mqtt_connected = False
                print(f"[MQTT] Connection error: {e}")
                time.sleep(5)

    def publish_command(self, device_id: str, command: str, reason: str, sensor_type: str) -> bool:
        if not self.mqtt_connected:
            print("[MQTT] Cannot publish command because MQTT is not connected.")
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

            if info.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"[MQTT] Published command to {topic}: {payload}")
                return True

            print(f"[MQTT] Failed to publish command to {topic}, rc={info.rc}")
            return False

        except Exception as e:
            print(f"[MQTT] Publish command error: {e}")
            return False

    # --------------------------------------------------
    # Alerts
    # --------------------------------------------------
    def format_alert_message(self, alert: Dict[str, Any]) -> str:
        return (
            f"🚨 Alert for {alert.get('device_id')}\n"
            f"Type: {alert.get('alert')}\n"
            f"Value: {alert.get('value')}\n"
            f"Threshold: {alert.get('threshold')}\n"
            f"Time: {alert.get('timestamp')}"
        )

    def alert_actions_keyboard(self, device_id: str, alert_type: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)

        if "temperature" in alert_type:
            kb.add(
                InlineKeyboardButton("❄️ Start Cooling", callback_data=f"cmd:{device_id}:start_cooling:temperature"),
                InlineKeyboardButton("🔥 Start Heating", callback_data=f"cmd:{device_id}:start_heating:temperature")
            )

        elif "humidity" in alert_type:
            kb.add(
                InlineKeyboardButton("💨 Increase Humidity", callback_data=f"cmd:{device_id}:increase_humidity:humidity"),
                InlineKeyboardButton("🌬 Decrease Humidity", callback_data=f"cmd:{device_id}:decrease_humidity:humidity")
            )

        elif "soil_moisture" in alert_type:
            kb.add(
                InlineKeyboardButton("💧 Increase Watering", callback_data=f"cmd:{device_id}:increase_watering:soil_moisture"),
                InlineKeyboardButton("🚱 Reduce Watering", callback_data=f"cmd:{device_id}:reduce_watering:soil_moisture")
            )

        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="menu_back"))
        return kb

    def notify_users_for_alert(self, alert: Dict[str, Any]) -> None:
        device_id = alert.get("device_id")
        alert_type = alert.get("alert", "")

        if not device_id:
            return

        for telegram_id in self.get_authorized_users():
            if self.is_device_allowed_for_user(telegram_id, device_id):
                try:
                    self.bot.send_message(
                        telegram_id,
                        self.format_alert_message(alert),
                        reply_markup=self.alert_actions_keyboard(device_id, alert_type)
                    )
                    print(f"[BOT] Alert sent to {telegram_id} for {device_id}")

                except Exception as e:
                    print(f"[BOT] Failed to notify user {telegram_id} for device {device_id}: {e}")

    # --------------------------------------------------
    # Formatters
    # --------------------------------------------------
    def format_device_status(self, device_id: str, data: Dict[str, Any]) -> str:
        return (
            f"🪴 Device: {device_id}\n"
            f"🌡 Temperature: {data.get('temperature', 'N/A')}\n"
            f"💧 Soil Moisture: {data.get('soil_moisture', 'N/A')}\n"
            f"💨 Humidity: {data.get('humidity', 'N/A')}\n"
            #f"📌 Status: {data.get('status', 'N/A')}\n"
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

    # --------------------------------------------------
    # Keyboards
    # --------------------------------------------------
    def main_menu_keyboard(self) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🪴 Devices", callback_data="menu_devices"),
            InlineKeyboardButton("🚨 Alerts", callback_data="menu_alerts")
        )
        kb.add(
            InlineKeyboardButton("📊 Report", callback_data="menu_report_devices"),
            InlineKeyboardButton("⚙️ Commands", callback_data="menu_command_devices")
        )
        kb.add(InlineKeyboardButton("❓ Help", callback_data="menu_help"))
        return kb

    def devices_keyboard_for_user(self, telegram_id: int, action_prefix: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)
        devices = self.get_allowed_devices_map(telegram_id)

        for device_id in devices.keys():
            kb.add(
                InlineKeyboardButton(
                    f"🪴 {device_id}",
                    callback_data=f"{action_prefix}:{device_id}"
                )
            )

        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="menu_back"))
        return kb

    def command_actions_keyboard(self, device_id: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)

        kb.add(
            InlineKeyboardButton("❄️ Start Cooling", callback_data=f"cmd:{device_id}:start_cooling:temperature"),
            InlineKeyboardButton("🔥 Start Heating", callback_data=f"cmd:{device_id}:start_heating:temperature")
        )
        kb.add(
            InlineKeyboardButton("💧 Increase Watering", callback_data=f"cmd:{device_id}:increase_watering:soil_moisture"),
            InlineKeyboardButton("🚱 Reduce Watering", callback_data=f"cmd:{device_id}:reduce_watering:soil_moisture")
        )
        kb.add(
            InlineKeyboardButton("💨 Increase Humidity", callback_data=f"cmd:{device_id}:increase_humidity:humidity"),
            InlineKeyboardButton("🌬 Decrease Humidity", callback_data=f"cmd:{device_id}:decrease_humidity:humidity")
        )
        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="menu_back"))

        return kb

    # --------------------------------------------------
    # Handlers
    # --------------------------------------------------
    def register_handlers(self) -> None:
        @self.bot.message_handler(commands=["start"])
        def start_handler(message):
            self.handle_start(message)

        @self.bot.message_handler(commands=["help"])
        def help_handler(message):
            self.handle_help(message)

        @self.bot.message_handler(commands=["devices"])
        def devices_handler(message):
            self.handle_devices(message)

        @self.bot.message_handler(commands=["status"])
        def status_handler(message):
            self.handle_status(message)

        @self.bot.message_handler(commands=["alerts"])
        def alerts_handler(message):
            self.handle_alerts(message)

        @self.bot.message_handler(commands=["report"])
        def report_handler(message):
            self.handle_report(message)

        @self.bot.callback_query_handler(func=lambda call: True)
        def callback_handler(call):
            self.handle_callbacks(call)

        @self.bot.message_handler(func=lambda message: True)
        def unknown_handler(message):
            self.handle_unknown(message)

    # --------------------------------------------------
    # Message handlers
    # --------------------------------------------------
    def handle_start(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        self.bot.send_message(
            message.chat.id,
            self.msg("welcome", "🪴 Welcome to the Smart Plant Care System Bot.\nChoose an action below."),
            reply_markup=self.main_menu_keyboard()
        )

    def handle_help(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        self.bot.reply_to(
            message,
            self.msg("help", "Use /start to open the menu.")
        )

    def handle_devices(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        try:
            devices = self.get_allowed_devices_map(message.from_user.id)

            if not devices:
                self.bot.reply_to(message, self.msg("devices_not_found", "⚠️ No devices found."))
                return

            text = "🪴 Your Devices:\n" + "\n".join(f"• {device_id}" for device_id in devices.keys())
            self.bot.reply_to(message, text)

        except Exception as e:
            self.bot.reply_to(message, f"Failed to fetch devices: {e}")

    def handle_status(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        parts = message.text.strip().split()
        if len(parts) < 2:
            self.bot.reply_to(
                message,
                self.msg("device_id_required", "🌿 Please provide a device_id.\nExample: /status raspi-01")
            )
            return

        device_id = parts[1]

        if not self.is_device_allowed_for_user(message.from_user.id, device_id):
            self.bot.reply_to(message, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            return

        data = self.get_live_device_status(device_id)
        self.bot.reply_to(message, self.format_device_status(device_id, data))

    def handle_alerts(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        try:
            data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
            alerts = [
                alert for alert in data.get("alerts", [])
                if self.is_device_allowed_for_user(message.from_user.id, alert.get("device_id", ""))
            ]

            self.bot.reply_to(message, self.format_alerts(alerts))

        except Exception as e:
            self.bot.reply_to(message, f"Failed to fetch alerts: {e}")

    def handle_report(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        parts = message.text.strip().split()
        if len(parts) < 2:
            self.bot.reply_to(
                message,
                self.msg("report_device_id_required", "🌿 Please provide a device_id.\nExample: /report raspi-01")
            )
            return

        device_id = parts[1]

        if not self.is_device_allowed_for_user(message.from_user.id, device_id):
            self.bot.reply_to(message, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
            return

        try:
            report = self.safe_get_json(
                f"{self.runtime['alert_generator_url']}/report",
                params={"device_id": device_id}
            )
            self.bot.reply_to(message, self.format_report(report))

        except Exception as e:
            self.bot.reply_to(message, f"Failed to fetch report: {e}")

    # --------------------------------------------------
    # Callback handlers
    # --------------------------------------------------
    def handle_callbacks(self, call) -> None:
        if not self.require_authorization_callback(call):
            return

        try:
            telegram_id = call.from_user.id

            if call.data == "menu_back":
                self.bot.send_message(
                    call.message.chat.id,
                    self.msg("menu_title", "🌱 Main Menu"),
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data == "menu_help":
                self.bot.send_message(
                    call.message.chat.id,
                    self.msg("help", "Use /start to open the menu."),
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data == "menu_devices":
                devices = self.get_allowed_devices_map(telegram_id)

                if not devices:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("devices_not_found", "⚠️ No devices found."),
                        reply_markup=self.main_menu_keyboard()
                    )
                else:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("choose_device", "🪴 Select one of your devices:"),
                        reply_markup=self.devices_keyboard_for_user(telegram_id, "status")
                    )

            elif call.data == "menu_report_devices":
                devices = self.get_allowed_devices_map(telegram_id)

                if not devices:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("devices_not_found", "⚠️ No devices found."),
                        reply_markup=self.main_menu_keyboard()
                    )
                else:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("choose_report_device", "📊 Select one of your devices for the report:"),
                        reply_markup=self.devices_keyboard_for_user(telegram_id, "report")
                    )

            elif call.data == "menu_command_devices":
                devices = self.get_allowed_devices_map(telegram_id)

                if not devices:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("devices_not_found", "⚠️ No devices found."),
                        reply_markup=self.main_menu_keyboard()
                    )
                else:
                    self.bot.send_message(
                        call.message.chat.id,
                        self.msg("choose_command_device", "⚙️ Select a device to control:"),
                        reply_markup=self.devices_keyboard_for_user(telegram_id, "command_device")
                    )

            elif call.data == "menu_alerts":
                data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
                alerts = [
                    alert for alert in data.get("alerts", [])
                    if self.is_device_allowed_for_user(telegram_id, alert.get("device_id", ""))
                ]

                self.bot.send_message(
                    call.message.chat.id,
                    self.format_alerts(alerts),
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data.startswith("status:"):
                device_id = call.data.split(":", 1)[1]

                if not self.is_device_allowed_for_user(telegram_id, device_id):
                    self.bot.send_message(call.message.chat.id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
                else:
                    data = self.get_live_device_status(device_id)
                    self.bot.send_message(
                        call.message.chat.id,
                        self.format_device_status(device_id, data),
                        reply_markup=self.devices_keyboard_for_user(telegram_id, "status")
                    )

            elif call.data.startswith("report:"):
                device_id = call.data.split(":", 1)[1]

                if not self.is_device_allowed_for_user(telegram_id, device_id):
                    self.bot.send_message(call.message.chat.id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
                else:
                    report = self.safe_get_json(
                        f"{self.runtime['alert_generator_url']}/report",
                        params={"device_id": device_id}
                    )
                    self.bot.send_message(
                        call.message.chat.id,
                        self.format_report(report),
                        reply_markup=self.devices_keyboard_for_user(telegram_id, "report")
                    )

            elif call.data.startswith("command_device:"):
                device_id = call.data.split(":", 1)[1]

                if not self.is_device_allowed_for_user(telegram_id, device_id):
                    self.bot.send_message(call.message.chat.id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
                else:
                    self.bot.send_message(
                        call.message.chat.id,
                        f"{self.msg('choose_command_action', '⚙️ Choose a command for this device:')}\nDevice: {device_id}",
                        reply_markup=self.command_actions_keyboard(device_id)
                    )

            elif call.data.startswith("cmd:"):
                parts = call.data.split(":")
                if len(parts) != 4:
                    raise ValueError("Invalid command callback format")

                _, device_id, command, sensor_type = parts

                if not self.is_device_allowed_for_user(telegram_id, device_id):
                    self.bot.send_message(call.message.chat.id, self.msg("device_not_allowed", "🚫 You are not allowed to access this device."))
                else:
                    success = self.publish_command(
                        device_id=device_id,
                        command=command,
                        reason="telegram_user_command",
                        sensor_type=sensor_type
                    )

                    if success:
                        self.bot.send_message(
                            call.message.chat.id,
                            f"{self.msg('command_sent', '✅ Command sent successfully.')}\nDevice: {device_id}\nCommand: {command}",
                            reply_markup=self.command_actions_keyboard(device_id)
                        )
                    else:
                        self.bot.send_message(
                            call.message.chat.id,
                            f"❌ Failed to send command.\nDevice: {device_id}\nCommand: {command}",
                            reply_markup=self.command_actions_keyboard(device_id)
                        )

            self.bot.answer_callback_query(call.id)

        except Exception as e:
            print(f"[BOT] Callback error: {e}")
            self.bot.answer_callback_query(
                call.id,
                self.msg("operation_failed", "⚠️ Operation failed."),
                show_alert=True
            )

    def handle_unknown(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        self.bot.reply_to(
            message,
            self.msg("unknown_command", "🤖 Unknown command. Use /start or /help")
        )

    # --------------------------------------------------
    # Run
    # --------------------------------------------------
    def run(self) -> None:
        print("[START] Telegram Bot starting...")
        print("[INFO] Authorization source: Catalogue /users")

        threading.Thread(target=self.registration_loop, daemon=True).start()
        threading.Thread(target=self.mqtt_loop, daemon=True).start()

        self.bot.infinity_polling(timeout=30, long_polling_timeout=20)


if __name__ == "__main__":
    app = TelegramPlantBot()
    app.run()