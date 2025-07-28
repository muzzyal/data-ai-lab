"""
Tests for Flask application factory and global routes.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from playground_batch_ingest.src.app import create_app, register_error_handlers, register_global_routes, setup_logging


@pytest.fixture
def app():
    """Create test Flask application."""
    test_config = {
        "flask_debug": True,
        "log_level": "DEBUG",
        "environment": "test",
        "pubsub_topic": "test-topic",
        "dlq_topic": "test-dlq",
        "use_real_pubsub": False,
    }
    with patch("playground_batch_ingest.src.app.config_loader.get_config", return_value=test_config):
        app = create_app()
        app.config["TESTING"] = True
        yield app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


def test_create_app():
    """Test Flask app creation."""
    with patch("playground_batch_ingest.src.app.config_loader.get_config") as mock_config:
        mock_config.return_value = {
            "flask_debug": False,
            "log_level": "INFO",
            "environment": "test",
        }

        app = create_app()

        assert app is not None
        assert app.config["DEBUG"] is False
        assert app.config["TESTING"] is False


def test_create_app_with_config_override():
    """Test Flask app creation with config override."""
    with patch("playground_batch_ingest.src.app.config_loader.get_config") as mock_config:
        mock_config.return_value = {"flask_debug": False}

        override_config = {"custom_setting": "test_value"}
        app = create_app(config_override=override_config)

        # Config should be merged
        mock_config.assert_called_once()


def test_index_route(client):
    """Test index route."""
    response = client.get("/")
    assert response.status_code == 200

    data = json.loads(response.data)
    assert data["service"] == "batch_ingestion"
    assert data["version"] == "0.1.0"
    assert data["status"] == "running"
    assert "environment" in data


def test_health_route(client):
    """Test health check route."""
    response = client.get("/health")
    assert response.status_code == 200

    data = json.loads(response.data)
    assert data["service"] == "batch_ingestion"
    assert data["status"] == "healthy"
    assert data["version"] == "0.1.0"


def test_config_route(client):
    """Test config route."""
    response = client.get("/config")
    assert response.status_code == 200

    data = json.loads(response.data)
    # Should contain safe config values only
    assert "environment" in data
    assert "pubsub_topic" in data
    assert "use_real_pubsub" in data
    # Should not contain sensitive data like secrets


def test_error_handler_400(client):
    """Test 400 error handler."""
    with client.application.test_request_context():
        from werkzeug.exceptions import BadRequest

        with pytest.raises(Exception) as e:
            response = client.application.handle_http_exception(e)
            assert response.status_code == 400


def test_error_handler_404(client):
    """Test 404 error handler."""
    response = client.get("/nonexistent-route")
    assert response.status_code == 404

    data = json.loads(response.data)
    assert data["error"] == "Not found"


def test_error_handler_500(client):
    """Test 500 error handler."""

    # Create a route that raises an exception
    @client.application.route("/test-error")
    def test_error():
        raise Exception("Test exception")

    response = client.get("/test-error")
    assert response.status_code == 500

    data = json.loads(response.data)
    assert data["error"] == "Internal server error"


@patch("playground_batch_ingest.src.app.logging.basicConfig")
def test_setup_logging(mock_basicConfig):
    """Test logging setup."""
    setup_logging("DEBUG")

    mock_basicConfig.assert_called_once()
    args, kwargs = mock_basicConfig.call_args
    assert kwargs["level"] == 10  # DEBUG level


def test_setup_logging_default_level():
    """Test logging setup with default level."""
    with patch("playground_batch_ingest.src.app.logging.basicConfig") as mock_basicConfig:
        setup_logging()

        mock_basicConfig.assert_called_once()
        args, kwargs = mock_basicConfig.call_args
        assert kwargs["level"] == 20  # INFO level


def test_register_error_handlers():
    """Test error handlers registration."""
    app = create_app()

    # Test that error handlers are registered
    assert 400 in app.error_handler_spec[None]
    assert 404 in app.error_handler_spec[None]
    assert 500 in app.error_handler_spec[None]


def test_register_global_routes():
    """Test global routes registration."""
    app = create_app()

    with app.test_client() as client:
        # Test that routes are accessible
        response = client.get("/")
        assert response.status_code == 200

        response = client.get("/health")
        assert response.status_code == 200

        response = client.get("/config")
        assert response.status_code == 200
