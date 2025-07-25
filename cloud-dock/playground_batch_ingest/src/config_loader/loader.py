"""
Configuration loader for the batch ingestion service.
Handles environment variables for Cloud Run deployment.
"""

import os
import tempfile
from typing import Any, Dict


class ConfigLoader:
    """Configuration loader with environment variable support for Cloud Run."""

    def __init__(self) -> None:
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.environment = os.getenv("ENVIRONMENT", "development")

    def get_config(self) -> Dict[str, Any]:
        """Get complete configuration dictionary."""
        return {
            # Google Cloud
            "project_id": self.project_id,
            "environment": self.environment,
            # Pub/Sub Configuration
            "pubsub_topic": os.getenv("PUBSUB_TOPIC_NAME", "batch-processed-data"),
            "subscription_name": os.getenv("SUBSCRIPTION_NAME", "gcs-file-events"),
            "use_real_pubsub": os.getenv("USE_REAL_PUBSUB", "true").lower() == "true",
            # GCS Configuration
            "temp_download_path": os.getenv("TEMP_DOWNLOAD_PATH", os.path.join(tempfile.gettempdir(), "batch_files")),
            "max_file_size_mb": int(os.getenv("MAX_FILE_SIZE_MB", "100")),
            # Processing Configuration
            "batch_size": int(os.getenv("BATCH_SIZE", "1000")),
            "max_workers": int(os.getenv("MAX_WORKERS", "4")),
            "processing_timeout": int(os.getenv("PROCESSING_TIMEOUT", "300")),
            # Data Configuration
            "supported_file_types": os.getenv("SUPPORTED_FILE_TYPES", "csv").split(","),
            "default_encoding": os.getenv("DEFAULT_ENCODING", "utf-8"),
            # Dead Letter Queue
            "dlq_topic": os.getenv("DLQ_TOPIC_NAME", "batch-processing-dlq"),
            "max_retry_attempts": int(os.getenv("MAX_RETRY_ATTEMPTS", "3")),
            # Monitoring
            "enable_monitoring": os.getenv("ENABLE_MONITORING", "true").lower() == "true",
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
            # Flask Configuration
            "flask_host": os.getenv("FLASK_HOST", "0.0.0.0"),  # nosec B104 - Required for containerised deployment
            "flask_port": int(os.getenv("PORT", "8080")),
            "flask_debug": os.getenv("FLASK_DEBUG", "false").lower() == "true",
        }

    def validate_config(self) -> bool:
        """Validate required configuration values."""
        config = self.get_config()

        required_fields = [
            "project_id",
            "pubsub_topic",
            "subscription_name",
        ]

        missing_fields = [f for f in required_fields if not config.get(f)]

        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")

        return True


# Global configuration instance
config_loader = ConfigLoader()
