"""
Tests for configuration loader.
"""

import sys

print("PYTHON EXECUTABLE:", sys.executable)
import os
from unittest.mock import MagicMock, patch

import pytest

from playground_batch_ingest.src.config_loader.loader import ConfigLoader, config_loader


@pytest.fixture
def clean_env():
    """Clean environment variables for testing."""
    env_vars = [
        "GOOGLE_CLOUD_PROJECT",
        "ENVIRONMENT",
        "PUBSUB_TOPIC",
        "SUBSCRIPTION_NAME",
        "USE_REAL_PUBSUB",
        "TEMP_DOWNLOAD_PATH",
        "MAX_FILE_SIZE_MB",
        "BATCH_SIZE",
        "MAX_WORKERS",
        "PROCESSING_TIMEOUT",
        "SUPPORTED_FILE_TYPES",
        "DEFAULT_ENCODING",
        "DLQ_TOPIC",
        "MAX_RETRY_ATTEMPTS",
        "ENABLE_MONITORING",
        "LOG_LEVEL",
        "FLASK_HOST",
        "PORT",
        "FLASK_DEBUG",
    ]

    # Store original values
    original_values = {}
    for var in env_vars:
        original_values[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    yield

    # Restore original values
    for var, value in original_values.items():
        if value is not None:
            os.environ[var] = value
        elif var in os.environ:
            del os.environ[var]


def test_config_loader_initialisation():
    """Test ConfigLoader initialisation."""
    with patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "test-project", "ENVIRONMENT": "test"}):
        loader = ConfigLoader()

        assert loader.project_id == "test-project"
        assert loader.environment == "test"


def test_config_loader_default_environment():
    """Test ConfigLoader with default environment."""
    with patch.dict(os.environ, {}, clear=True):
        loader = ConfigLoader()

        assert loader.environment == "development"


def test_get_config_defaults(clean_env):
    """Test get_config with default values."""
    loader = ConfigLoader()
    config = loader.get_config()

    # Test default values
    assert config["environment"] == "development"
    assert config["pubsub_topic"] == "batch-processed-data"
    assert config["subscription_name"] == "gcs-file-events"
    assert config["use_real_pubsub"] is True
    assert config["temp_download_path"] == "/tmp/batch_files"
    assert config["max_file_size_mb"] == 100
    assert config["batch_size"] == 1000
    assert config["max_workers"] == 4
    assert config["processing_timeout"] == 300
    assert config["supported_file_types"] == ["csv"]
    assert config["default_encoding"] == "utf-8"
    assert config["dlq_topic"] == "batch-processing-dlq"
    assert config["max_retry_attempts"] == 3
    assert config["enable_monitoring"] is True
    assert config["log_level"] == "INFO"
    assert config["flask_host"] == "0.0.0.0"
    assert config["flask_port"] == 8080
    assert config["flask_debug"] is False


def test_get_config_with_env_vars():
    """Test get_config with environment variables set."""
    env_vars = {
        "GOOGLE_CLOUD_PROJECT": "prod-project",
        "ENVIRONMENT": "production",
        "PUBSUB_TOPIC_NAME": "prod-topic",
        "SUBSCRIPTION_NAME": "prod-subscription",
        "USE_REAL_PUBSUB": "false",
        "TEMP_DOWNLOAD_PATH": "/app/temp",
        "MAX_FILE_SIZE_MB": "50",
        "BATCH_SIZE": "500",
        "MAX_WORKERS": "8",
        "PROCESSING_TIMEOUT": "600",
        "SUPPORTED_FILE_TYPES": "csv,json,xml",
        "DEFAULT_ENCODING": "utf-16",
        "DLQ_TOPIC_NAME": "prod-dlq",
        "MAX_RETRY_ATTEMPTS": "5",
        "ENABLE_MONITORING": "false",
        "LOG_LEVEL": "DEBUG",
        "FLASK_HOST": "127.0.0.1",
        "PORT": "9000",
        "FLASK_DEBUG": "true",
    }

    with patch.dict(os.environ, env_vars):
        loader = ConfigLoader()
        config = loader.get_config()

        assert config["project_id"] == "prod-project"
        assert config["environment"] == "production"
        assert config["pubsub_topic"] == "prod-topic"
        assert config["subscription_name"] == "prod-subscription"
        assert config["use_real_pubsub"] is False
        assert config["temp_download_path"] == "/app/temp"
        assert config["max_file_size_mb"] == 50
        assert config["batch_size"] == 500
        assert config["max_workers"] == 8
        assert config["processing_timeout"] == 600
        assert config["supported_file_types"] == ["csv", "json", "xml"]
        assert config["default_encoding"] == "utf-16"
        assert config["dlq_topic"] == "prod-dlq"
        assert config["max_retry_attempts"] == 5
        assert config["enable_monitoring"] is False
        assert config["log_level"] == "DEBUG"
        assert config["flask_host"] == "127.0.0.1"
        assert config["flask_port"] == 9000
        assert config["flask_debug"] is True


def test_validate_config_success():
    """Test successful config validation."""
    with patch.dict(
        os.environ,
        {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "PUBSUB_TOPIC": "test-topic",
            "SUBSCRIPTION_NAME": "test-subscription",
        },
    ):
        loader = ConfigLoader()
        result = loader.validate_config()

        assert result is True


def test_validate_config_missing_fields():
    """Test config validation with missing required fields."""
    with patch.dict(os.environ, {}, clear=True):
        loader = ConfigLoader()

        with pytest.raises(ValueError) as exc_info:
            loader.validate_config()

        assert "Missing required configuration fields" in str(exc_info.value)
        assert "project_id" in str(exc_info.value)


def test_debug():
    print("hello")
    assert True


def test_validate_config_partial_missing():
    """Test config validation with some missing fields."""
    with patch.dict(os.environ, {}, clear=True):
        loader = ConfigLoader()
        with pytest.raises(ValueError) as exc_info:
            loader.validate_config()
        error_msg = str(exc_info.value)
        assert "Missing required configuration fields" in error_msg
        assert "project_id" in error_msg


def test_global_config_loader_instance():
    """Test the global config_loader instance."""
    assert isinstance(config_loader, ConfigLoader)


def test_config_type_conversions():
    """Test that configuration values are properly converted to correct types."""
    env_vars = {
        "MAX_FILE_SIZE_MB": "50",
        "BATCH_SIZE": "1000",
        "MAX_WORKERS": "4",
        "PROCESSING_TIMEOUT": "300",
        "PORT": "8080",
        "MAX_RETRY_ATTEMPTS": "3",
        "USE_REAL_PUBSUB": "true",
        "ENABLE_MONITORING": "false",
        "FLASK_DEBUG": "true",
    }

    with patch.dict(os.environ, env_vars):
        loader = ConfigLoader()
        config = loader.get_config()

        # Test integer conversions
        assert isinstance(config["max_file_size_mb"], int)
        assert isinstance(config["batch_size"], int)
        assert isinstance(config["max_workers"], int)
        assert isinstance(config["processing_timeout"], int)
        assert isinstance(config["flask_port"], int)
        assert isinstance(config["max_retry_attempts"], int)

        # Test boolean conversions
        assert isinstance(config["use_real_pubsub"], bool)
        assert isinstance(config["enable_monitoring"], bool)
        assert isinstance(config["flask_debug"], bool)

        assert config["use_real_pubsub"] is True
        assert config["enable_monitoring"] is False
        assert config["flask_debug"] is True


def test_supported_file_types_parsing():
    """Test parsing of supported file types."""
    # Test single type
    with patch.dict(os.environ, {"SUPPORTED_FILE_TYPES": "csv"}):
        loader = ConfigLoader()
        config = loader.get_config()
        assert config["supported_file_types"] == ["csv"]

    # Test multiple types
    with patch.dict(os.environ, {"SUPPORTED_FILE_TYPES": "csv,json,xml"}):
        loader = ConfigLoader()
        config = loader.get_config()
        assert config["supported_file_types"] == ["csv", "json", "xml"]

    # Test with spaces
    with patch.dict(os.environ, {"SUPPORTED_FILE_TYPES": "csv, json, xml"}):
        loader = ConfigLoader()
        config = loader.get_config()
        assert config["supported_file_types"] == ["csv", " json", " xml"]
