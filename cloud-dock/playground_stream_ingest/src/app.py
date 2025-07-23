import logging
import os

from flask import Flask, jsonify
from playground_stream_ingest.src.config_loader.loader import get_secret_key
from werkzeug.middleware.proxy_fix import ProxyFix

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_app():
    """Application factory pattern for Flask app creation."""
    app = Flask(__name__)

    # Configuration
    app.config["SECRET_KEY"], scc, err = get_secret_key()

    if not scc:
        logger.error(f"Failed to retrieve secret key: {err}")
        raise ValueError("Failed to retrieve secret key from Secret Manager")

    app.config["DEBUG"] = True
    app.config["TESTING"] = False

    # Proxy fix for production deployment
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    # Register blueprints
    from playground_stream_ingest.src.routes.transaction_routes import transaction_bp

    app.register_blueprint(transaction_bp)

    # Health check endpoint
    @app.route("/health")
    def health_check():
        return {"status": "healthy", "service": "transaction-ingestion"}, 200

    # Root route - API info
    @app.route("/")
    def index():
        return jsonify(
            {
                "service": "transaction-ingestion",
                "version": "1.0.0",
                "description": "Cloud Run service for webhook transaction ingestion",
                "endpoints": {
                    "health": "/health",
                    "ingest": "/api/transactions (POST)",
                    "validate": "/api/transactions/validate (POST)",
                    "status": "/api/status",
                },
            }
        )

    logger.info("Flask application created successfully.")
    return app


# Create the app instance
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)  # nosec
