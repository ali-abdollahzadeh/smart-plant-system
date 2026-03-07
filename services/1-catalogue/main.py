import cherrypy
import json
import os
import threading
from datetime import datetime, timezone

CATALOG_FILE = os.environ.get("CATALOG_FILE", "catalog.json")
HOST = os.environ.get("CATALOG_HOST", "0.0.0.0")
PORT = int(os.environ.get("CATALOG_PORT", 8000))


# ----------------- Thread-safe storage -----------------
catalog_lock = threading.Lock()


# ----------------- Defaults -----------------
DEFAULT_CATALOG = {
    "devices": [],
    "services": [],
    "config": {
        "project_name": "Smart Plant Care System",
        "mqtt_broker": "mosquitto",
        "mqtt_port": 1883,
        "sampling_interval": 60,
        "moisture_threshold": 30,
        "temperature_threshold": 35,
        "humidity_threshold": 70
    }
}


# ----------------- Helpers -----------------
def now_utc_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_catalog_file():
    if not os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CATALOG, f, indent=4)


def load_catalog():
    with catalog_lock:
        ensure_catalog_file()
        try:
            with open(CATALOG_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            # Recover safely if file is corrupted/unreadable
            data = DEFAULT_CATALOG.copy()
            save_catalog(data)

        # Guarantee required top-level keys exist
        data.setdefault("devices", [])
        data.setdefault("services", [])
        data.setdefault("config", {})
        return data


def save_catalog(data):
    with catalog_lock:
        temp_file = f"{CATALOG_FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, CATALOG_FILE)


def find_by_id(items, item_id):
    for item in items:
        if item.get("id") == item_id:
            return item
    return None


def upsert_by_id(items, new_item):
    for i, item in enumerate(items):
        if item.get("id") == new_item.get("id"):
            items[i] = new_item
            return "updated"
    items.append(new_item)
    return "created"


def error_response(status_code, message):
    cherrypy.response.status = status_code
    return {"error": message}


def validate_device(device):
    required = ["id", "name", "type"]
    for field in required:
        if field not in device or not str(device[field]).strip():
            return f"Missing required field: {field}"

    # At least one communication reference should exist
    if not device.get("endpoint") and not device.get("mqtt_topic"):
        return "Device must include at least one of: endpoint, mqtt_topic"

    return None


def validate_service(service):
    required = ["id", "name", "type", "endpoint"]
    for field in required:
        if field not in service or not str(service[field]).strip():
            return f"Missing required field: {field}"
    return None


# ----------------- REST Resources -----------------
@cherrypy.expose
class DevicesAPI:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self, device_id=None):
        data = load_catalog()

        if device_id:
            device = find_by_id(data["devices"], device_id)
            if not device:
                return error_response(404, f"Device '{device_id}' not found")
            return device

        return {
            "count": len(data["devices"]),
            "devices": data["devices"]
        }

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        device = cherrypy.request.json
        validation_error = validate_device(device)
        if validation_error:
            return error_response(400, validation_error)

        data = load_catalog()

        # Add/update metadata automatically
        existing = find_by_id(data["devices"], device["id"])
        created_at = existing.get("created_at") if existing else now_utc_iso()

        device["created_at"] = created_at
        device["last_update"] = now_utc_iso()
        device["status"] = device.get("status", "active")

        action = upsert_by_id(data["devices"], device)
        save_catalog(data)

        cherrypy.response.status = 201 if action == "created" else 200
        return {
            "message": f"Device {action} successfully",
            "device": device
        }

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, device_id=None):
        if not device_id:
            return error_response(400, "Device ID is required in the URL")

        updated_fields = cherrypy.request.json
        data = load_catalog()
        device = find_by_id(data["devices"], device_id)

        if not device:
            return error_response(404, f"Device '{device_id}' not found")

        # Merge old + new, then validate
        merged = {**device, **updated_fields}
        validation_error = validate_device(merged)
        if validation_error:
            return error_response(400, validation_error)

        merged["id"] = device_id
        merged["created_at"] = device.get("created_at", now_utc_iso())
        merged["last_update"] = now_utc_iso()

        upsert_by_id(data["devices"], merged)
        save_catalog(data)

        return {
            "message": "Device updated successfully",
            "device": merged
        }

    @cherrypy.tools.json_out()
    def DELETE(self, device_id=None):
        if not device_id:
            return error_response(400, "Device ID is required in the URL")

        data = load_catalog()
        device = find_by_id(data["devices"], device_id)

        if not device:
            return error_response(404, f"Device '{device_id}' not found")

        data["devices"] = [d for d in data["devices"] if d.get("id") != device_id]
        save_catalog(data)

        return {
            "message": "Device deleted successfully",
            "device_id": device_id
        }


@cherrypy.expose
class ServicesAPI:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self, service_id=None):
        data = load_catalog()

        if service_id:
            service = find_by_id(data["services"], service_id)
            if not service:
                return error_response(404, f"Service '{service_id}' not found")
            return service

        return {
            "count": len(data["services"]),
            "services": data["services"]
        }

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        service = cherrypy.request.json
        validation_error = validate_service(service)
        if validation_error:
            return error_response(400, validation_error)

        data = load_catalog()

        existing = find_by_id(data["services"], service["id"])
        created_at = existing.get("created_at") if existing else now_utc_iso()

        service["created_at"] = created_at
        service["last_update"] = now_utc_iso()
        service["status"] = service.get("status", "active")

        action = upsert_by_id(data["services"], service)
        save_catalog(data)

        cherrypy.response.status = 201 if action == "created" else 200
        return {
            "message": f"Service {action} successfully",
            "service": service
        }

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, service_id=None):
        if not service_id:
            return error_response(400, "Service ID is required in the URL")

        updated_fields = cherrypy.request.json
        data = load_catalog()
        service = find_by_id(data["services"], service_id)

        if not service:
            return error_response(404, f"Service '{service_id}' not found")

        merged = {**service, **updated_fields}
        validation_error = validate_service(merged)
        if validation_error:
            return error_response(400, validation_error)

        merged["id"] = service_id
        merged["created_at"] = service.get("created_at", now_utc_iso())
        merged["last_update"] = now_utc_iso()

        upsert_by_id(data["services"], merged)
        save_catalog(data)

        return {
            "message": "Service updated successfully",
            "service": merged
        }

    @cherrypy.tools.json_out()
    def DELETE(self, service_id=None):
        if not service_id:
            return error_response(400, "Service ID is required in the URL")

        data = load_catalog()
        service = find_by_id(data["services"], service_id)

        if not service:
            return error_response(404, f"Service '{service_id}' not found")

        data["services"] = [s for s in data["services"] if s.get("id") != service_id]
        save_catalog(data)

        return {
            "message": "Service deleted successfully",
            "service_id": service_id
        }


@cherrypy.expose
class ConfigAPI:
    exposed = True

    @cherrypy.tools.json_out()
    def GET(self):
        data = load_catalog()
        return data["config"]

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self):
        new_config = cherrypy.request.json
        if not isinstance(new_config, dict):
            return error_response(400, "Config must be a JSON object")

        data = load_catalog()
        data["config"].update(new_config)
        save_catalog(data)

        return {
            "message": "Configuration updated successfully",
            "config": data["config"]
        }


@cherrypy.expose
class RootAPI:
    @cherrypy.tools.json_out()
    def GET(self):
        data = load_catalog()
        return {
            "message": "Welcome to Smart Plant Care System Catalogue",
            "endpoints": {
                "devices": "/devices",
                "device_by_id": "/devices/<device_id>",
                "services": "/services",
                "service_by_id": "/services/<service_id>",
                "config": "/config"
            },
            "summary": {
                "devices": len(data["devices"]),
                "services": len(data["services"])
            }
        }


# ----------------- Custom dispatcher -----------------
class CatalogueDispatcher(object):
    exposed = True

    def __init__(self):
        self.root = RootAPI()
        self.devices_api = DevicesAPI()
        self.services_api = ServicesAPI()
        self.config_api = ConfigAPI()

    @cherrypy.tools.json_out()
    def GET(self, *vpath, **params):
        return self._dispatch("GET", *vpath)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self, *vpath, **params):
        return self._dispatch("POST", *vpath)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, *vpath, **params):
        return self._dispatch("PUT", *vpath)

    @cherrypy.tools.json_out()
    def DELETE(self, *vpath, **params):
        return self._dispatch("DELETE", *vpath)

    def _dispatch(self, method, *vpath):
        # /
        if len(vpath) == 0 or vpath == ("",):
            return getattr(self.root, method)()

        # /devices or /devices/<id>
        if vpath[0] == "devices":
            device_id = vpath[1] if len(vpath) > 1 else None
            return getattr(self.devices_api, method)(device_id=device_id) if method in ["GET", "PUT", "DELETE"] else getattr(self.devices_api, method)()

        # /services or /services/<id>
        if vpath[0] == "services":
            service_id = vpath[1] if len(vpath) > 1 else None
            return getattr(self.services_api, method)(service_id=service_id) if method in ["GET", "PUT", "DELETE"] else getattr(self.services_api, method)()

        # /config
        if vpath[0] == "config":
            if len(vpath) > 1:
                return error_response(404, "Invalid config path")
            return getattr(self.config_api, method)()

        return error_response(404, "Endpoint not found")


# ----------------- Server setup -----------------
if __name__ == "__main__":
    cherrypy.config.update({
        "server.socket_host": HOST,
        "server.socket_port": PORT,
        "log.screen": True
    })

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
    }

    app = CatalogueDispatcher()
    cherrypy.quickstart(app, "/", conf)