import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


class TelegramPlantBot:
    def __init__(self) -> None:
        self.runtime = self.load_runtime_config()
        self.config = self.load_json(self.runtime["config_file"])

        token = self.runtime["telegram_bot_token"]
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")

        self.bot = telebot.TeleBot(token)
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
        }

    # --------------------------------------------------
    # Authorization
    # --------------------------------------------------
    def get_authorized_users(self) -> List[int]:
        return self.config.get("authorized_users", [])

    def is_authorized_user_id(self, user_id: int) -> bool:
        return user_id in self.get_authorized_users()

    def require_authorization_message(self, message) -> bool:
        if not self.is_authorized_user_id(message.from_user.id):
            unauthorized_message = self.config.get("messages", {}).get(
                "unauthorized",
                "You are not authorized to use this bot."
            )
            self.bot.reply_to(message, unauthorized_message)
            print(f"[SECURITY] Unauthorized access attempt from Telegram ID: {message.from_user.id}")
            return False
        return True

    def require_authorization_callback(self, call) -> bool:
        if not self.is_authorized_user_id(call.from_user.id):
            unauthorized_message = self.config.get("messages", {}).get(
                "unauthorized",
                "You are not authorized to use this bot."
            )
            self.bot.answer_callback_query(call.id, unauthorized_message, show_alert=True)
            print(f"[SECURITY] Unauthorized callback attempt from Telegram ID: {call.from_user.id}")
            return False
        return True

    # --------------------------------------------------
    # Catalogue registration
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
    # API helpers
    # --------------------------------------------------
    def safe_get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_devices_map(self) -> Dict[str, Any]:
        data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/devices")
        return data.get("devices", {})

    # --------------------------------------------------
    # Formatters
    # --------------------------------------------------
    def format_device_status(self, device_id: str, data: Dict[str, Any]) -> str:
        return (
            f"🪴 Device: {device_id}\n"
            f"🌡 Temperature: {data.get('temperature', 'N/A')}\n"
            f"💧 Soil Moisture: {data.get('soil_moisture', 'N/A')}\n"
            f"💨 Humidity: {data.get('humidity', 'N/A')}\n"
            f"⏱ Timestamp: {data.get('timestamp', 'N/A')}"
        )

    def format_alerts(self, alerts: List[Dict[str, Any]]) -> str:
        if not alerts:
            return self.config["messages"]["no_alerts"]

        lines = ["🚨 Recent Alerts:"]
        for alert in alerts[-5:]:
            lines.append(
                f"• {alert.get('device_id')} | {alert.get('alert')} | "
                f"value={alert.get('value')} | threshold={alert.get('threshold')}"
            )
        return "\n".join(lines)

    def format_commands(self, commands: List[Dict[str, Any]]) -> str:
        if not commands:
            return self.config["messages"]["no_commands"]

        lines = ["⚙️ Recent Commands:"]
        for command in commands[-5:]:
            lines.append(
                f"• {command.get('device_id')} | {command.get('command')} | "
                f"{command.get('reason')}"
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
    # UI
    # --------------------------------------------------
    def main_menu_keyboard(self) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("🪴 Devices", callback_data="menu_devices"),
            InlineKeyboardButton("🚨 Alerts", callback_data="menu_alerts")
        )
        kb.add(
            InlineKeyboardButton("📊 Report", callback_data="menu_report_devices"),
            InlineKeyboardButton("⚙️ Commands", callback_data="menu_commands")
        )
        kb.add(
            InlineKeyboardButton("❓ Help", callback_data="menu_help")
        )
        return kb

    def devices_keyboard(self, action_prefix: str) -> InlineKeyboardMarkup:
        kb = InlineKeyboardMarkup(row_width=1)

        try:
            devices = self.get_devices_map()
            for device_id in devices.keys():
                kb.add(
                    InlineKeyboardButton(
                        f"🪴 {device_id}",
                        callback_data=f"{action_prefix}:{device_id}"
                    )
                )
        except Exception:
            pass

        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="menu_back"))
        return kb

    # --------------------------------------------------
    # Bot handlers
    # --------------------------------------------------
    def register_handlers(self) -> None:
        @self.bot.message_handler(commands=["start"])
        def handle_start(message):
            self.handle_start(message)

        @self.bot.message_handler(commands=["help"])
        def handle_help(message):
            self.handle_help(message)

        @self.bot.message_handler(commands=["devices"])
        def handle_devices(message):
            self.handle_devices(message)

        @self.bot.message_handler(commands=["status"])
        def handle_status(message):
            self.handle_status(message)

        @self.bot.message_handler(commands=["alerts"])
        def handle_alerts(message):
            self.handle_alerts(message)

        @self.bot.message_handler(commands=["report"])
        def handle_report(message):
            self.handle_report(message)

        @self.bot.message_handler(commands=["commands"])
        def handle_commands(message):
            self.handle_commands(message)

        @self.bot.callback_query_handler(func=lambda call: True)
        def handle_callbacks(call):
            self.handle_callbacks(call)

        @self.bot.message_handler(func=lambda message: True)
        def handle_unknown(message):
            self.handle_unknown(message)

    def handle_start(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        ui_cfg = self.config.get("ui", {})
        if ui_cfg.get("send_sticker_on_start") and ui_cfg.get("plant_sticker_file_id"):
            try:
                self.bot.send_sticker(message.chat.id, ui_cfg["plant_sticker_file_id"])
            except Exception as e:
                print(f"[BOT] Failed to send sticker: {e}")

        self.bot.send_message(
            message.chat.id,
            self.config["messages"]["welcome"],
            reply_markup=self.main_menu_keyboard()
        )

    def handle_help(self, message) -> None:
        if not self.require_authorization_message(message):
            return
        self.bot.reply_to(message, self.config["messages"]["help"])

    def handle_devices(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        try:
            devices = self.get_devices_map()
            if not devices:
                self.bot.reply_to(message, "No devices found.")
                return

            text = "🪴 Devices:\n" + "\n".join(f"• {d}" for d in devices.keys())
            self.bot.reply_to(message, text)

        except requests.RequestException as e:
            self.bot.reply_to(message, f"Failed to fetch devices: {e}")

    def handle_status(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        parts = message.text.strip().split()
        if len(parts) < 2:
            self.bot.reply_to(message, self.config["messages"]["device_id_required"])
            return

        device_id = parts[1]

        try:
            data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/devices/{device_id}")
            self.bot.reply_to(message, self.format_device_status(device_id, data))
        except requests.RequestException as e:
            self.bot.reply_to(message, f"Failed to fetch device status: {e}")

    def handle_alerts(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        try:
            data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
            self.bot.reply_to(message, self.format_alerts(data.get("alerts", [])))
        except requests.RequestException as e:
            self.bot.reply_to(message, f"Failed to fetch alerts: {e}")

    def handle_report(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        parts = message.text.strip().split()
        if len(parts) < 2:
            self.bot.reply_to(message, self.config["messages"]["report_device_id_required"])
            return

        device_id = parts[1]

        try:
            report = self.safe_get_json(
                f"{self.runtime['alert_generator_url']}/report",
                params={"device_id": device_id}
            )
            self.bot.reply_to(message, self.format_report(report))
        except requests.RequestException as e:
            self.bot.reply_to(message, f"Failed to fetch report: {e}")

    def handle_commands(self, message) -> None:
        if not self.require_authorization_message(message):
            return

        try:
            data = self.safe_get_json(f"{self.runtime['analytics_url']}/commands")
            self.bot.reply_to(message, self.format_commands(data.get("commands", [])))
        except requests.RequestException as e:
            self.bot.reply_to(message, f"Failed to fetch command history: {e}")

    def handle_callbacks(self, call) -> None:
        if not self.require_authorization_callback(call):
            return

        try:
            if call.data == "menu_back":
                self.bot.edit_message_text(
                    self.config["messages"]["menu_title"],
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data == "menu_help":
                self.bot.edit_message_text(
                    self.config["messages"]["help"],
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data == "menu_devices":
                self.bot.edit_message_text(
                    self.config["messages"]["choose_device"],
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.devices_keyboard("status")
                )

            elif call.data == "menu_report_devices":
                self.bot.edit_message_text(
                    "📊 Choose a device for the report:",
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.devices_keyboard("report")
                )

            elif call.data == "menu_alerts":
                data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/alerts")
                self.bot.edit_message_text(
                    self.format_alerts(data.get("alerts", [])),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data == "menu_commands":
                data = self.safe_get_json(f"{self.runtime['analytics_url']}/commands")
                self.bot.edit_message_text(
                    self.format_commands(data.get("commands", [])),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.main_menu_keyboard()
                )

            elif call.data.startswith("status:"):
                device_id = call.data.split(":", 1)[1]
                data = self.safe_get_json(f"{self.runtime['alert_generator_url']}/devices/{device_id}")
                self.bot.edit_message_text(
                    self.format_device_status(device_id, data),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.devices_keyboard("status")
                )

            elif call.data.startswith("report:"):
                device_id = call.data.split(":", 1)[1]
                report = self.safe_get_json(
                    f"{self.runtime['alert_generator_url']}/report",
                    params={"device_id": device_id}
                )
                self.bot.edit_message_text(
                    self.format_report(report),
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    reply_markup=self.devices_keyboard("report")
                )

            self.bot.answer_callback_query(call.id)

        except Exception as e:
            print(f"[BOT] Callback error: {e}")
            self.bot.answer_callback_query(call.id, "Operation failed.")

    def handle_unknown(self, message) -> None:
        if not self.require_authorization_message(message):
            return
        self.bot.reply_to(message, self.config["messages"]["unknown_command"])

    # --------------------------------------------------
    # Run
    # --------------------------------------------------
    def run(self) -> None:
        print("[START] Telegram Bot starting...")
        print(f"[INFO] Authorized users: {self.get_authorized_users()}")

        threading.Thread(target=self.registration_loop, daemon=True).start()
        self.bot.infinity_polling(timeout=30, long_polling_timeout=20)


if __name__ == "__main__":
    app = TelegramPlantBot()
    app.run()