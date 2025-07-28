import os
import time
from unittest.mock import MagicMock, patch

import pytest
from playground_stream_ingest.src.config_loader.loader import get_secret_key, retrieve_environment_variables


def test_retrieve_environment_variables():
    """Test that environment variables are retrieved correctly."""
    project_id, topic_name, dlq_topic_name, secret_id = retrieve_environment_variables()

    assert project_id == os.environ["GOOGLE_CLOUD_PROJECT"]
    assert topic_name == os.environ["PUBSUB_TOPIC_NAME"]
    assert dlq_topic_name == os.environ["DLQ_TOPIC_NAME"]
    assert secret_id == os.environ["SECRET_ID"]


def test_retrieve_environment_variables_missing():
    """Test that missing environment variables raise an error."""
    os.environ.pop("GOOGLE_CLOUD_PROJECT", None)
    os.environ.pop("PUBSUB_TOPIC_NAME", None)
    os.environ.pop("DLQ_TOPIC_NAME", None)

    with pytest.raises(
        ValueError, match="Missing required environment variables for Pub/Sub or Secret Manager configuration."
    ):
        retrieve_environment_variables()

    # Restore environment variables
    os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
    os.environ["PUBSUB_TOPIC_NAME"] = "test-topic"
    os.environ["DLQ_TOPIC_NAME"] = "test-dlq-topic"


def test_get_secret_key_success():
    # Mock the environment variable retrieval
    with patch("playground_stream_ingest.src.config_loader.loader.retrieve_environment_variables") as mock_env:
        mock_env.return_value = ("test-project", "test-topic", "test-dlq", "test-secret-id")

        # Mock the Secret Manager client and its response
        with patch(
            "playground_stream_ingest.src.config_loader.loader.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_response = MagicMock()
            mock_response.payload.data.decode.return_value = "mock-secret-value"
            mock_client.access_secret_version.return_value = mock_response
            mock_client_cls.return_value = mock_client

            secret, success, error = get_secret_key()

            assert secret == "mock-secret-value"
            assert success is True
            assert error == ""


def test_get_secret_key_failure():
    with patch("playground_stream_ingest.src.config_loader.loader.retrieve_environment_variables") as mock_env:
        mock_env.return_value = ("test-project", "test-topic", "test-dlq", "test-secret-id")

        with patch(
            "playground_stream_ingest.src.config_loader.loader.secretmanager.SecretManagerServiceClient"
        ) as mock_client_cls:
            mock_client = MagicMock()
            mock_client.access_secret_version.side_effect = Exception("Boom!")
            mock_client_cls.return_value = mock_client

            secret, success, error = get_secret_key()

            assert secret == ""
            assert success is False
            assert "Failed to retrieve secret" in error
