import cherrypy
import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class CatalogueConfig:
    def __init__(self) -> None:
        self.catalog_file = os.environ.get("CATALOG_FILE", "catalog.json")
        self.host = os.environ.get("CATALOG_HOST", "0.0.0.0")
        self.port = int(os.environ.get("CATALOG_PORT", 8000))

        self.default_catalog = {
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

    @staticmethod
    def now_utc_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class CatalogueStorage:
    def __init__(self, config: CatalogueConfig) -> None:
        self.config = config
        self.lock = threading.RLock()

    def ensure_catalog_file(self) -> None:
        if not os.path.exists(self.config.catalog_file):
            with open(self.config.catalog_file, "w", encoding="utf-8") as f:
                json.dump(self.config.default_catalog, f, indent=4)

    def load_catalog(self) -> Dict[str, Any]:
        with self.lock:
            self.ensure_catalog_file()
            try:
                with open(self.config.catalog_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                data = {
                    "devices": list(self.config.default_catalog["devices"]),
                    "services": list(self.config.default_catalog["services"]),
                    "config": dict(self.config.default_catalog["config"])
                }
                self.save_catalog(data)

            data.setdefault("devices", [])
            data.setdefault("services", [])
            data.setdefault("config", {})
            return data

    def save_catalog(self, data: Dict[str, Any]) -> None:
        with self.lock:
            temp_file = f"{self.config.catalog_file}.tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            os.replace(temp_file, self.config.catalog_file)


class CatalogueService:
    def __init__(self) -> None:
        self.config = CatalogueConfig()
        self.storage = CatalogueStorage(self.config)

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------
    def find_by_id(self, items: List[Dict[str, Any]], item_id: str) -> Optional[Dict[str, Any]]:
        for item in items:
            if item.get("id") == item_id:
                return item
        return None

    def upsert_by_id(self, items: List[Dict[str, Any]], new_item: Dict[str, Any]) -> str:
        for i, item in enumerate(items):
            if item.get("id") == new_item.get("id"):
                items[i] = new_item
                return "updated"
        items.append(new_item)
        return "created"

    def error_response(self, status_code: int, message: str) -> Dict[str, Any]:
        cherrypy.response.status = status_code
        return {"error": message}

    # --------------------------------------------------
    # Validation
    # --------------------------------------------------
    def validate_device(self, device: Dict[str, Any]) -> Optional[str]:
        required = ["id", "name", "type"]
        for field in required:
            if field not in device or not str(device[field]).strip():
                return f"Missing required field: {field}"

        if not device.get("endpoint") and not device.get("mqtt_topic"):
            return "Device must include at least one of: endpoint, mqtt_topic"

        return None

    def validate_service(self, service: Dict[str, Any]) -> Optional[str]:
        required = ["id", "name", "type", "endpoint"]
        for field in required:
            if field not in service or not str(service[field]).strip():
                return f"Missing required field: {field}"
        return None

    # --------------------------------------------------
    # Devices
    # --------------------------------------------------
    def get_devices(self, device_id: Optional[str] = None) -> Dict[str, Any]:
        data = self.storage.load_catalog()

        if device_id:
            device = self.find_by_id(data["devices"], device_id)
            if not device:
                return self.error_response(404, f"Device '{device_id}' not found")
            return device

        return {
            "count": len(data["devices"]),
            "devices": data["devices"]
        }

    def create_or_update_device(self, device: Dict[str, Any]) -> Dict[str, Any]:
        validation_error = self.validate_device(device)
        if validation_error:
            return self.error_response(400, validation_error)

        data = self.storage.load_catalog()
        existing = self.find_by_id(data["devices"], device["id"])
        created_at = existing.get("created_at") if existing else self.config.now_utc_iso()

        device["created_at"] = created_at
        device["last_update"] = self.config.now_utc_iso()
        device["status"] = device.get("status", "active")

        action = self.upsert_by_id(data["devices"], device)
        self.storage.save_catalog(data)

        cherrypy.response.status = 201 if action == "created" else 200
        return {
            "message": f"Device {action} successfully",
            "device": device
        }

    def update_device(self, device_id: Optional[str], updated_fields: Dict[str, Any]) -> Dict[str, Any]:
        if not device_id:
            return self.error_response(400, "Device ID is required in the URL")

        data = self.storage.load_catalog()
        device = self.find_by_id(data["devices"], device_id)

        if not device:
            return self.error_response(404, f"Device '{device_id}' not found")

        merged = {**device, **updated_fields}
        validation_error = self.validate_device(merged)
        if validation_error:
            return self.error_response(400, validation_error)

        merged["id"] = device_id
        merged["created_at"] = device.get("created_at", self.config.now_utc_iso())
        merged["last_update"] = self.config.now_utc_iso()

        self.upsert_by_id(data["devices"], merged)
        self.storage.save_catalog(data)

        return {
            "message": "Device updated successfully",
            "device": merged
        }

    def delete_device(self, device_id: Optional[str]) -> Dict[str, Any]:
        if not device_id:
            return self.error_response(400, "Device ID is required in the URL")

        data = self.storage.load_catalog()
        device = self.find_by_id(data["devices"], device_id)

        if not device:
            return self.error_response(404, f"Device '{device_id}' not found")

        data["devices"] = [d for d in data["devices"] if d.get("id") != device_id]
        self.storage.save_catalog(data)

        return {
            "message": "Device deleted successfully",
            "device_id": device_id
        }

    # --------------------------------------------------
    # Services
    # --------------------------------------------------
    def get_services(self, service_id: Optional[str] = None) -> Dict[str, Any]:
        data = self.storage.load_catalog()

        if service_id:
            service = self.find_by_id(data["services"], service_id)
            if not service:
                return self.error_response(404, f"Service '{service_id}' not found")
            return service

        return {
            "count": len(data["services"]),
            "services": data["services"]
        }

    def create_or_update_service(self, service: Dict[str, Any]) -> Dict[str, Any]:
        validation_error = self.validate_service(service)
        if validation_error:
            return self.error_response(400, validation_error)

        data = self.storage.load_catalog()
        existing = self.find_by_id(data["services"], service["id"])
        created_at = existing.get("created_at") if existing else self.config.now_utc_iso()

        service["created_at"] = created_at
        service["last_update"] = self.config.now_utc_iso()
        service["status"] = service.get("status", "active")

        action = self.upsert_by_id(data["services"], service)
        self.storage.save_catalog(data)

        cherrypy.response.status = 201 if action == "created" else 200
        return {
            "message": f"Service {action} successfully",
            "service": service
        }

    def update_service(self, service_id: Optional[str], updated_fields: Dict[str, Any]) -> Dict[str, Any]:
        if not service_id:
            return self.error_response(400, "Service ID is required in the URL")

        data = self.storage.load_catalog()
        service = self.find_by_id(data["services"], service_id)

        if not service:
            return self.error_response(404, f"Service '{service_id}' not found")

        merged = {**service, **updated_fields}
        validation_error = self.validate_service(merged)
        if validation_error:
            return self.error_response(400, validation_error)

        merged["id"] = service_id
        merged["created_at"] = service.get("created_at", self.config.now_utc_iso())
        merged["last_update"] = self.config.now_utc_iso()

        self.upsert_by_id(data["services"], merged)
        self.storage.save_catalog(data)

        return {
            "message": "Service updated successfully",
            "service": merged
        }

    def delete_service(self, service_id: Optional[str]) -> Dict[str, Any]:
        if not service_id:
            return self.error_response(400, "Service ID is required in the URL")

        data = self.storage.load_catalog()
        service = self.find_by_id(data["services"], service_id)

        if not service:
            return self.error_response(404, f"Service '{service_id}' not found")

        data["services"] = [s for s in data["services"] if s.get("id") != service_id]
        self.storage.save_catalog(data)

        return {
            "message": "Service deleted successfully",
            "service_id": service_id
        }

    # --------------------------------------------------
    # Config
    # --------------------------------------------------
    def get_config(self) -> Dict[str, Any]:
        data = self.storage.load_catalog()
        return data["config"]

    def update_config(self, new_config: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(new_config, dict):
            return self.error_response(400, "Config must be a JSON object")

        data = self.storage.load_catalog()
        data["config"].update(new_config)
        self.storage.save_catalog(data)

        return {
            "message": "Configuration updated successfully",
            "config": data["config"]
        }

    # --------------------------------------------------
    # Root
    # --------------------------------------------------
    def get_root(self) -> Dict[str, Any]:
        data = self.storage.load_catalog()
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


class RootAPI:
    exposed = True

    def __init__(self, service: CatalogueService) -> None:
        self.service = service
        self.devices = DevicesAPI(service)
        self.services = ServicesAPI(service)
        self.config = ConfigAPI(service)

    @cherrypy.tools.json_out()
    def GET(self):
        return self.service.get_root()


class DevicesAPI:
    exposed = True

    def __init__(self, service: CatalogueService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, device_id=None):
        return self.service.get_devices(device_id=device_id)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        return self.service.create_or_update_device(cherrypy.request.json)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, device_id=None):
        return self.service.update_device(device_id=device_id, updated_fields=cherrypy.request.json)

    @cherrypy.tools.json_out()
    def DELETE(self, device_id=None):
        return self.service.delete_device(device_id=device_id)


class ServicesAPI:
    exposed = True

    def __init__(self, service: CatalogueService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self, service_id=None):
        return self.service.get_services(service_id=service_id)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        return self.service.create_or_update_service(cherrypy.request.json)

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, service_id=None):
        return self.service.update_service(service_id=service_id, updated_fields=cherrypy.request.json)

    @cherrypy.tools.json_out()
    def DELETE(self, service_id=None):
        return self.service.delete_service(service_id=service_id)


class ConfigAPI:
    exposed = True

    def __init__(self, service: CatalogueService) -> None:
        self.service = service

    @cherrypy.tools.json_out()
    def GET(self):
        return self.service.get_config()

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self):
        return self.service.update_config(cherrypy.request.json)


class CatalogueDispatcher:
    exposed = True

    def __init__(self, service: CatalogueService) -> None:
        self.service = service
        self.root = RootAPI(service)
        self.devices_api = DevicesAPI(service)
        self.services_api = ServicesAPI(service)
        self.config_api = ConfigAPI(service)

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

    def _dispatch(self, method: str, *vpath):
        if len(vpath) == 0 or vpath == ("",):
            return getattr(self.root, method)()

        if vpath[0] == "devices":
            device_id = vpath[1] if len(vpath) > 1 else None
            if method in ["GET", "PUT", "DELETE"]:
                return getattr(self.devices_api, method)(device_id=device_id)
            return getattr(self.devices_api, method)()

        if vpath[0] == "services":
            service_id = vpath[1] if len(vpath) > 1 else None
            if method in ["GET", "PUT", "DELETE"]:
                return getattr(self.services_api, method)(service_id=service_id)
            return getattr(self.services_api, method)()

        if vpath[0] == "config":
            if len(vpath) > 1:
                return self.service.error_response(404, "Invalid config path")
            return getattr(self.config_api, method)()

        return self.service.error_response(404, "Endpoint not found")


if __name__ == "__main__":
    service = CatalogueService()

    cherrypy.config.update({
        "server.socket_host": service.config.host,
        "server.socket_port": service.config.port,
        "log.screen": True
    })

    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
    }

    app = CatalogueDispatcher(service)
    cherrypy.quickstart(app, "/", conf)