import cherrypy
import json
import os

CATALOG_FILE = "catalog.json"

# ----------------- Helper Functions -----------------
def load_catalog():sadsafsdf
    # Create an initial structure to store device information and configurations if the file does not exist
    if not os.path.exists(CATALOG_FILE):
        return {
            "devices": [], 
            "services": [], 
            "config": {}  # Empty config so it can be adjusted dynamically later
        }
    with open(CATALOG_FILE, "r") as f:
        return json.load(f)

def save_catalog(data):
    with open(CATALOG_FILE, "w") as f:
        json.dump(data, f, indent=4)

# ----------------- CherryPy RESTful Classes -----------------

@cherrypy.expose
class DeviceRegistry:
    """Device management class (e.g., Raspberry Pi)"""
    
    @cherrypy.tools.json_out()
    def GET(self):
        # Discover registered devices
        return load_catalog()["devices"]

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        # Register a new device
        device = cherrypy.request.json
        data = load_catalog()
        # Prevent duplicates: remove existing device with the same ID before appending
        data["devices"] = [d for d in data["devices"] if d.get("id") != device.get("id")]
        data["devices"].append(device)
        save_catalog(data)
        return {"message": "Device registered successfully", "device": device}

@cherrypy.expose
class ServiceRegistry:
    """Service management class (e.g., Data Analytics module)"""
    
    @cherrypy.tools.json_out()
    def GET(self):
        # Discover registered services
        return load_catalog()["services"]

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def POST(self):
        # Register a new service
        service = cherrypy.request.json
        data = load_catalog()
        # Prevent duplicates: remove existing service with the same ID before appending
        data["services"] = [s for s in data["services"] if s.get("id") != service.get("id")]
        data["services"].append(service)
        save_catalog(data)
        return {"message": "Service registered successfully", "service": service}

@cherrypy.expose
class ConfigManager:
    """System configuration management class"""
    
    @cherrypy.tools.json_out()
    def GET(self):
        # Retrieve current configuration
        return load_catalog()["config"]

    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def PUT(self):
        # Update system configuration
        new_config = cherrypy.request.json
        data = load_catalog()
        data["config"] = new_config
        save_catalog(data)
        return {"message": "Configuration updated successfully", "config": data["config"]}

class CatalogueService:
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        return {"message": "Welcome to Smart Plant Care System Catalogue"}

# ----------------- Server Setup -----------------
if __name__ == '__main__':
    # Configuration to enable REST methods (GET, POST, PUT)
    conf = {
        '/': {
            'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
        }
    }
    
    # Map classes to URL paths
    app = CatalogueService()
    app.devices = DeviceRegistry()
    app.services = ServiceRegistry()
    app.config = ConfigManager()

    # Set host and port
    cherrypy.config.update({
        'server.socket_host': '0.0.0.0', 
        'server.socket_port': 8000
    })
    
    # Run the application
    cherrypy.quickstart(app, '/', conf)