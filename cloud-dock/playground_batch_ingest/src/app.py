"""
Flask application factory for batch ingestion service.
"""

import logging
import os
from typing import Any, Dict

from flask import Flask, jsonify

from playground_batch_ingest.src.config_loader.loader import config_loader
from playground_batch_ingest.src.routes.batch_routes import batch_bp


def create_app(config_override: Dict[str, Any] = None) -> Flask:
    """
    Create and configure Flask application.

    Args:
        config_override: Optional configuration overrides

    Returns:
        Configured Flask application instance
    """
    app = Flask(__name__)

    # Load configuration
    config = config_loader.get_config()
    if config_override:
        config.update(config_override)

    # Configure Flask
    app.config.update({"DEBUG": config.get("flask_debug", False), "TESTING": False})

    # Configure logging
    setup_logging(config.get("log_level", "INFO"))

    # Register blueprints
    app.register_blueprint(batch_bp)

    # Global error handlers
    register_error_handlers(app)

    # Global routes
    register_global_routes(app, config)

    logger = logging.getLogger(__name__)
    logger.info("Batch ingestion service initialised")

    return app


def setup_logging(log_level: str = "INFO") -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Set specific logger levels
    logging.getLogger("google.cloud").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def register_error_handlers(app: Flask) -> None:
    """Register global error handlers."""

    @app.errorhandler(400)
    def bad_request(error):
        return jsonify({"error": "Bad request", "message": str(error)}), 400

    @app.errorhandler(404)
    def not_found(error):
        return jsonify({"error": "Not found", "message": "Resource not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(error):
        return jsonify({"error": "Method not allowed", "message": str(error)}), 405

    @app.errorhandler(500)
    def internal_error(error):
        logger = logging.getLogger(__name__)
        logger.error(f"Internal server error: {error}")
        return jsonify({"error": "Internal server error", "message": "An unexpected error occurred"}), 500

    @app.errorhandler(Exception)
    def handle_exception(error):
        logger = logging.getLogger(__name__)
        logger.error(f"Unhandled exception: {error}", exc_info=True)
        return jsonify({"error": "Internal server error", "message": "An unexpected error occurred"}), 500


def register_global_routes(app: Flask, config: Dict[str, Any]) -> None:
    """Register global application routes."""

    @app.route("/")
    def index():
        return jsonify(
            {
                "service": "batch_ingestion",
                "version": "0.1.0",
                "status": "running",
                "environment": config.get("environment", "unknown"),
            }
        )

    @app.route("/health")
    def health():
        return jsonify(
            {
                "service": "batch_ingestion",
                "status": "healthy",
                "version": "0.1.0",
            }
        )

    @app.route("/config")
    def get_config():
        """Get non-sensitive configuration for debugging."""
        safe_config = {
            "environment": config.get("environment"),
            "pubsub_topic": config.get("pubsub_topic"),
            "dlq_topic": config.get("dlq_topic"),
            "use_real_pubsub": config.get("use_real_pubsub"),
            "batch_size": config.get("batch_size"),
            "max_workers": config.get("max_workers"),
            "supported_file_types": config.get("supported_file_types"),
            "max_file_size_mb": config.get("max_file_size_mb"),
        }
        return jsonify(safe_config)
