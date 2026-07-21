import cherrypy
import json
import os
import threading
from datetime import datetime, timezone


class CatalogueWebService:
    exposed = True

    def __init__(self, catalogue_file):
        self.catalogue_file = catalogue_file
        self.lock = threading.RLock()
        self.create_catalogue_file()

    # --------------------------------------------------
    # File management
    # --------------------------------------------------
    def create_catalogue_file(self):
        """Create the catalogue file only when it does not exist."""
        if not os.path.exists(self.catalogue_file):
            default_catalogue = {
                "users": [],
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

            file = open(self.catalogue_file, "w", encoding="utf-8")
            json.dump(default_catalogue, file, indent=4)
            file.close()

    def load_catalogue(self):
        """Read and return the complete catalogue."""
        try:
            file = open(self.catalogue_file, "r", encoding="utf-8")
            catalogue = json.load(file)
            file.close()
            return catalogue
        except (OSError, json.JSONDecodeError):
            return None

    def save_catalogue(self, catalogue):
        """Write the complete catalogue to the JSON file."""
        file = open(self.catalogue_file, "w", encoding="utf-8")
        json.dump(catalogue, file, indent=4)
        file.close()

    # --------------------------------------------------
    # Helper methods
    # --------------------------------------------------
    def current_time(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    def error_response(self, status, message):
        cherrypy.response.status = status
        return {"error": message}

    def find_by_id(self, items, item_id):
        for item in items:
            if item.get("id") == item_id:
                return item
        return None

    def find_position_by_id(self, items, item_id):
        for position, item in enumerate(items):
            if item.get("id") == item_id:
                return position
        return None

    def validate_user(self, user):
        required_fields = ["id", "telegram_id", "name", "devices"]

        for field in required_fields:
            if field not in user:
                return f"Missing required field: {field}"

        if not isinstance(user["devices"], list):
            return "devices must be a list"

        return None

    def validate_device(self, device):
        required_fields = [
            "id",
            "name",
            "type",
            "mqtt_topic",
            "command_topic"
        ]

        for field in required_fields:
            if field not in device or str(device[field]).strip() == "":
                return f"Missing required field: {field}"

        return None

    def validate_service(self, service):
        required_fields = ["id", "name", "type", "endpoint"]

        for field in required_fields:
            if field not in service or str(service[field]).strip() == "":
                return f"Missing required field: {field}"

        return None

    def check_catalogue(self):
        catalogue = self.load_catalogue()

        if catalogue is None:
            return None, self.error_response(
                500,
                "The catalogue file cannot be read"
            )

        catalogue.setdefault("users", [])
        catalogue.setdefault("devices", [])
        catalogue.setdefault("services", [])
        catalogue.setdefault("config", {})

        return catalogue, None

    # --------------------------------------------------
    # GET
    # --------------------------------------------------
    @cherrypy.tools.json_out()
    def GET(self, *uri, **params):
        catalogue, error = self.check_catalogue()
        if error:
            return error

        # GET /
        if len(uri) == 0:
            return {
                "message": "Welcome to Smart Plant Care System Catalogue",
                "endpoints": {
                    "users": "/users",
                    "devices": "/devices",
                    "services": "/services",
                    "config": "/config"
                },
                "summary": {
                    "users": len(catalogue["users"]),
                    "devices": len(catalogue["devices"]),
                    "services": len(catalogue["services"])
                }
            }

        resource = uri[0]

        # GET /users
        # GET /users/<user_id>
        # GET /users?telegram_id=<telegram_id>
        if resource == "users":
            if len(uri) > 2:
                return self.error_response(404, "Invalid users path")

            if "telegram_id" in params:
                try:
                    requested_telegram_id = int(params["telegram_id"])
                except (TypeError, ValueError):
                    return self.error_response(
                        400,
                        "telegram_id must be an integer"
                    )

                for user in catalogue["users"]:
                    if int(user.get("telegram_id", -1)) == requested_telegram_id:
                        return user

                return self.error_response(
                    404,
                    f"Telegram user '{requested_telegram_id}' not found"
                )

            if len(uri) == 2:
                user = self.find_by_id(catalogue["users"], uri[1])

                if user is None:
                    return self.error_response(
                        404,
                        f"User '{uri[1]}' not found"
                    )

                return user

            return {
                "count": len(catalogue["users"]),
                "users": catalogue["users"]
            }

        # GET /devices
        # GET /devices/<device_id>
        if resource == "devices":
            if len(uri) > 2:
                return self.error_response(404, "Invalid devices path")

            if len(uri) == 2:
                device = self.find_by_id(catalogue["devices"], uri[1])

                if device is None:
                    return self.error_response(
                        404,
                        f"Device '{uri[1]}' not found"
                    )

                return device

            return {
                "count": len(catalogue["devices"]),
                "devices": catalogue["devices"]
            }

        # GET /services
        # GET /services/<service_id>
        if resource == "services":
            if len(uri) > 2:
                return self.error_response(404, "Invalid services path")

            if len(uri) == 2:
                service = self.find_by_id(catalogue["services"], uri[1])

                if service is None:
                    return self.error_response(
                        404,
                        f"Service '{uri[1]}' not found"
                    )

                return service

            return {
                "count": len(catalogue["services"]),
                "services": catalogue["services"]
            }

        # GET /config
        if resource == "config":
            if len(uri) != 1:
                return self.error_response(404, "Invalid config path")

            return catalogue["config"]

        return self.error_response(404, "Endpoint not found")

    # --------------------------------------------------
    # POST
    # --------------------------------------------------
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self, *uri, **params):
        if len(uri) != 1:
            return self.error_response(404, "Invalid endpoint")

        catalogue, error = self.check_catalogue()
        if error:
            return error

        resource = uri[0]
        new_item = cherrypy.request.json

        if not isinstance(new_item, dict):
            return self.error_response(400, "The body must be a JSON object")

        # POST /users
        if resource == "users":
            validation_error = self.validate_user(new_item)
            if validation_error:
                return self.error_response(400, validation_error)

            if self.find_by_id(catalogue["users"], new_item["id"]) is not None:
                return self.error_response(
                    409,
                    f"User '{new_item['id']}' already exists"
                )

            new_item.setdefault("role", "user")
            new_item.setdefault("status", "active")
            catalogue["users"].append(new_item)
            self.save_catalogue(catalogue)

            cherrypy.response.status = 201
            return {
                "message": "User created successfully",
                "user": new_item
            }

        # POST /devices
        if resource == "devices":
            validation_error = self.validate_device(new_item)
            if validation_error:
                return self.error_response(400, validation_error)

            if self.find_by_id(catalogue["devices"], new_item["id"]) is not None:
                return self.error_response(
                    409,
                    f"Device '{new_item['id']}' already exists"
                )

            now = self.current_time()
            new_item.setdefault("status", "active")
            new_item.setdefault("created_at", now)
            new_item.setdefault("last_update", now)

            catalogue["devices"].append(new_item)
            self.save_catalogue(catalogue)

            cherrypy.response.status = 201
            return {
                "message": "Device created successfully",
                "device": new_item
            }

        # POST /services
        #
        # Service registration is idempotent:
        # - a new ID is appended once;
        # - an existing ID is updated instead of being appended again.
        if resource == "services":
            validation_error = self.validate_service(new_item)
            if validation_error:
                return self.error_response(400, validation_error)

            with self.lock:
                catalogue, error = self.check_catalogue()
                if error:
                    return error

                position = self.find_position_by_id(
                    catalogue["services"],
                    new_item["id"]
                )

                now = self.current_time()
                new_item.setdefault("status", "active")

                if position is None:
                    new_item.setdefault("created_at", now)
                    new_item["last_update"] = now
                    catalogue["services"].append(new_item)
                    self.save_catalogue(catalogue)

                    cherrypy.response.status = 201
                    return {
                        "message": "Service registered successfully",
                        "action": "created",
                        "service": new_item
                    }

                existing_service = catalogue["services"][position]
                updated_service = existing_service.copy()
                updated_service.update(new_item)
                updated_service["id"] = existing_service["id"]
                updated_service["created_at"] = existing_service.get(
                    "created_at",
                    now
                )
                updated_service["last_update"] = now

                catalogue["services"][position] = updated_service
                self.save_catalogue(catalogue)

                cherrypy.response.status = 200
                return {
                    "message": "Service registration refreshed",
                    "action": "updated",
                    "service": updated_service
                }

        return self.error_response(
            405,
            f"POST is not allowed for '{resource}'"
        )

    # --------------------------------------------------
    # PUT
    # --------------------------------------------------
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self, *uri, **params):
        catalogue, error = self.check_catalogue()
        if error:
            return error

        updated_fields = cherrypy.request.json

        if not isinstance(updated_fields, dict):
            return self.error_response(400, "The body must be a JSON object")

        if len(uri) == 0:
            return self.error_response(400, "Resource path is required")

        resource = uri[0]

        # PUT /config
        if resource == "config":
            if len(uri) != 1:
                return self.error_response(404, "Invalid config path")

            catalogue["config"].update(updated_fields)
            self.save_catalogue(catalogue)

            return {
                "message": "Configuration updated successfully",
                "config": catalogue["config"]
            }

        if len(uri) != 2:
            return self.error_response(
                400,
                "The resource ID is required in the URL"
            )

        item_id = uri[1]

        # PUT /users/<user_id>
        if resource == "users":
            position = self.find_position_by_id(catalogue["users"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"User '{item_id}' not found"
                )

            user = catalogue["users"][position].copy()
            user.update(updated_fields)
            user["id"] = item_id

            validation_error = self.validate_user(user)
            if validation_error:
                return self.error_response(400, validation_error)

            user.setdefault("role", "user")
            user.setdefault("status", "active")
            catalogue["users"][position] = user
            self.save_catalogue(catalogue)

            return {
                "message": "User updated successfully",
                "user": user
            }

        # PUT /devices/<device_id>
        if resource == "devices":
            position = self.find_position_by_id(catalogue["devices"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"Device '{item_id}' not found"
                )

            device = catalogue["devices"][position].copy()
            device.update(updated_fields)
            device["id"] = item_id

            validation_error = self.validate_device(device)
            if validation_error:
                return self.error_response(400, validation_error)

            device["last_update"] = self.current_time()
            catalogue["devices"][position] = device
            self.save_catalogue(catalogue)

            return {
                "message": "Device updated successfully",
                "device": device
            }

        # PUT /services/<service_id>
        if resource == "services":
            position = self.find_position_by_id(catalogue["services"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"Service '{item_id}' not found"
                )

            service = catalogue["services"][position].copy()
            service.update(updated_fields)
            service["id"] = item_id

            validation_error = self.validate_service(service)
            if validation_error:
                return self.error_response(400, validation_error)

            service["last_update"] = self.current_time()
            catalogue["services"][position] = service
            self.save_catalogue(catalogue)

            return {
                "message": "Service updated successfully",
                "service": service
            }

        return self.error_response(
            405,
            f"PUT is not allowed for '{resource}'"
        )

    # --------------------------------------------------
    # DELETE
    # --------------------------------------------------
    @cherrypy.tools.json_out()
    def DELETE(self, *uri, **params):
        if len(uri) != 2:
            return self.error_response(
                400,
                "The resource ID is required in the URL"
            )

        catalogue, error = self.check_catalogue()
        if error:
            return error

        resource = uri[0]
        item_id = uri[1]

        # DELETE /users/<user_id>
        if resource == "users":
            position = self.find_position_by_id(catalogue["users"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"User '{item_id}' not found"
                )

            deleted_user = catalogue["users"].pop(position)
            self.save_catalogue(catalogue)

            return {
                "message": "User deleted successfully",
                "user": deleted_user
            }

        # DELETE /devices/<device_id>
        if resource == "devices":
            position = self.find_position_by_id(catalogue["devices"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"Device '{item_id}' not found"
                )

            deleted_device = catalogue["devices"].pop(position)

            # Remove the deleted device from every user.
            for user in catalogue["users"]:
                if item_id in user.get("devices", []):
                    user["devices"].remove(item_id)

            self.save_catalogue(catalogue)

            return {
                "message": "Device deleted successfully",
                "device": deleted_device
            }

        # DELETE /services/<service_id>
        if resource == "services":
            position = self.find_position_by_id(catalogue["services"], item_id)

            if position is None:
                return self.error_response(
                    404,
                    f"Service '{item_id}' not found"
                )

            deleted_service = catalogue["services"].pop(position)
            self.save_catalogue(catalogue)

            return {
                "message": "Service deleted successfully",
                "service": deleted_service
            }

        return self.error_response(
            405,
            f"DELETE is not allowed for '{resource}'"
        )


if __name__ == "__main__":
    catalogue_file = os.environ.get("CATALOG_FILE", "catalog.json")
    host = os.environ.get("CATALOG_HOST", "0.0.0.0")
    port = int(os.environ.get("CATALOG_PORT", 8000))

    configuration = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher(),
            "tools.sessions.on": True
        }
    }

    cherrypy.config.update({
        "server.socket_host": host,
        "server.socket_port": port,
        "log.screen": True
    })

    web_service = CatalogueWebService(catalogue_file)

    cherrypy.tree.mount(web_service, "/", configuration)
    cherrypy.engine.start()
    cherrypy.engine.block()