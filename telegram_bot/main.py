import json
import os
import threading
import time
from typing import Any, Dict, List, Optional

import requests
import telebot


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_runtime_config() -> Dict[str, Any]:
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


RUNTIME = load_runtime_config()
APP_CONFIG = load_json(RUNTIME["config_file"])


if not RUNTIME["telegram_bot_token"]:
    raise ValueError("TELEGRAM_BOT_TOKEN is required")


bot = telebot.TeleBot(RUNTIME["telegram_bot_token"])


# --------------------------------------------------
# Authorization
# --------------------------------------------------
def get_authorized_users() -> List[int]:
    return APP_CONFIG.get("authorized_users", [])


def is_authorized(message) -> bool:
    user_id = message.from_user.id
    return user_id in get_authorized_users()


def require_authorization(message) -> bool:
    if not is_authorized(message):
        bot.reply_to(message, APP_CONFIG["messages"]["unauthorized"])
        print(f"[SECURITY] Unauthorized access attempt from Telegram ID: {message.from_user.id}")
        return False
    return True


# --------------------------------------------------
# Service registration
# --------------------------------------------------
def register_service() -> None:
    payload = {
        "id": RUNTIME["service_id"],
        "name": RUNTIME["service_name"],
        "type": RUNTIME["service_type"],
        "endpoint": "telegram-bot",
        "status": "active"
    }

    try:
        response = requests.post(
            f"{RUNTIME['catalog_url']}/services",
            json=payload,
            timeout=10
        )

        if response.status_code in (200, 201):
            print(f"[CATALOGUE] Service registered: {payload}")
        else:
            print(f"[CATALOGUE] Registration failed: {response.status_code} {response.text}")

    except requests.RequestException as e:
        print(f"[CATALOGUE] Registration error: {e}")


def registration_loop() -> None:
    while True:
        register_service()
        time.sleep(RUNTIME["register_interval"])


# --------------------------------------------------
# API helpers
# --------------------------------------------------
def safe_get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    return response.json()


def format_device_status(device_id: str, data: Dict[str, Any]) -> str:
    return (
        f"Device: {device_id}\n"
        f"Temperature: {data.get('temperature', 'N/A')}\n"
        f"Soil Moisture: {data.get('soil_moisture', 'N/A')}\n"
        f"Humidity: {data.get('humidity', 'N/A')}\n"
        f"Timestamp: {data.get('timestamp', 'N/A')}"
    )


def format_alerts(alerts: List[Dict[str, Any]]) -> str:
    if not alerts:
        return APP_CONFIG["messages"]["no_alerts"]

    lines = []
    for alert in alerts[-5:]:
        lines.append(
            f"- {alert.get('device_id')} | {alert.get('alert')} | "
            f"value={alert.get('value')} | threshold={alert.get('threshold')} | "
            f"time={alert.get('timestamp')}"
        )
    return "\n".join(lines)


def format_commands(commands: List[Dict[str, Any]]) -> str:
    if not commands:
        return APP_CONFIG["messages"]["no_commands"]

    lines = []
    for command in commands[-5:]:
        lines.append(
            f"- {command.get('device_id')} | {command.get('command')} | "
            f"reason={command.get('reason')} | sensor={command.get('sensor_type')} | "
            f"time={command.get('timestamp')}"
        )
    return "\n".join(lines)


def format_report(report: Dict[str, Any]) -> str:
    averages = report.get("averages", {})
    latest = report.get("latest_data", {})

    return (
        f"Report for {report.get('device_id')}\n\n"
        f"Latest Data:\n"
        f"Temperature: {latest.get('temperature', 'N/A')}\n"
        f"Soil Moisture: {latest.get('soil_moisture', 'N/A')}\n"
        f"Humidity: {latest.get('humidity', 'N/A')}\n\n"
        f"Averages:\n"
        f"Temperature: {averages.get('temperature', 'N/A')}\n"
        f"Soil Moisture: {averages.get('soil_moisture', 'N/A')}\n"
        f"Humidity: {averages.get('humidity', 'N/A')}\n\n"
        f"History Count: {report.get('history_count', 0)}"
    )


# --------------------------------------------------
# Telegram commands
# --------------------------------------------------
@bot.message_handler(commands=["start"])
def handle_start(message):
    if not require_authorization(message):
        return
    bot.reply_to(message, APP_CONFIG["messages"]["welcome"])


@bot.message_handler(commands=["help"])
def handle_help(message):
    if not require_authorization(message):
        return
    bot.reply_to(message, APP_CONFIG["messages"]["help"])


@bot.message_handler(commands=["devices"])
def handle_devices(message):
    if not require_authorization(message):
        return

    try:
        data = safe_get_json(f"{RUNTIME['alert_generator_url']}/devices")
        devices = data.get("devices", {})

        if not devices:
            bot.reply_to(message, "No devices found.")
            return

        device_ids = list(devices.keys())
        bot.reply_to(message, "Devices:\n" + "\n".join(f"- {d}" for d in device_ids))

    except requests.RequestException as e:
        bot.reply_to(message, f"Failed to fetch devices: {e}")


@bot.message_handler(commands=["status"])
def handle_status(message):
    if not require_authorization(message):
        return

    parts = message.text.strip().split()

    if len(parts) < 2:
        bot.reply_to(message, APP_CONFIG["messages"]["device_id_required"])
        return

    device_id = parts[1]

    try:
        data = safe_get_json(f"{RUNTIME['alert_generator_url']}/devices/{device_id}")
        bot.reply_to(message, format_device_status(device_id, data))

    except requests.RequestException as e:
        bot.reply_to(message, f"Failed to fetch device status: {e}")


@bot.message_handler(commands=["alerts"])
def handle_alerts(message):
    if not require_authorization(message):
        return

    try:
        data = safe_get_json(f"{RUNTIME['alert_generator_url']}/alerts")
        alerts = data.get("alerts", [])
        bot.reply_to(message, format_alerts(alerts))

    except requests.RequestException as e:
        bot.reply_to(message, f"Failed to fetch alerts: {e}")


@bot.message_handler(commands=["report"])
def handle_report(message):
    if not require_authorization(message):
        return

    parts = message.text.strip().split()

    if len(parts) < 2:
        bot.reply_to(message, APP_CONFIG["messages"]["report_device_id_required"])
        return

    device_id = parts[1]

    try:
        report = safe_get_json(
            f"{RUNTIME['alert_generator_url']}/report",
            params={"device_id": device_id}
        )
        bot.reply_to(message, format_report(report))

    except requests.RequestException as e:
        bot.reply_to(message, f"Failed to fetch report: {e}")


@bot.message_handler(commands=["commands"])
def handle_commands(message):
    if not require_authorization(message):
        return

    try:
        data = safe_get_json(f"{RUNTIME['analytics_url']}/commands")
        commands = data.get("commands", [])
        bot.reply_to(message, format_commands(commands))

    except requests.RequestException as e:
        bot.reply_to(message, f"Failed to fetch command history: {e}")


@bot.message_handler(func=lambda message: True)
def handle_unknown(message):
    if not require_authorization(message):
        return
    bot.reply_to(message, "Unknown command. Use /help")


# --------------------------------------------------
# Run
# --------------------------------------------------
if __name__ == "__main__":
    print("[START] Telegram Bot starting...")
    print(f"[INFO] Authorized users: {get_authorized_users()}")

    threading.Thread(target=registration_loop, daemon=True).start()
    bot.infinity_polling(timeout=30, long_polling_timeout=20)