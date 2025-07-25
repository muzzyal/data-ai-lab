"""
Main entry point for the batch ingestion service.
"""

import os

from .app import create_app
from .config_loader.loader import config_loader

# Validate configuration on startup
config_loader.validate_config()

# Create Flask app
app = create_app()

if __name__ == "__main__":
    config = config_loader.get_config()
    app.run(
        host=config.get("flask_host", "0.0.0.0"),  # nosec B104 - Required for containerised deployment
        port=config.get("flask_port", 8080),
        debug=config.get("flask_debug", False),
    )
