import pytest
import sys
import os
from unittest.mock import patch

secret_id = "test-secret-id"


def mock_fetch_secret_success():
    secret_key = secret_id.encode("utf-8")
    return secret_key, True, ""


@pytest.fixture(autouse=True, scope="session")
def mock_secret_retrieval():
    with patch("playground_stream_ingest.src.config_loader.loader.get_secret_key", mock_fetch_secret_success):
        yield


@pytest.fixture(autouse=True, scope="session")
def mock_env_retrieval():
    os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
    os.environ["PUBSUB_TOPIC_NAME"] = "test-topic"
    os.environ["DLQ_TOPIC_NAME"] = "test-dlq-topic"
    os.environ["SECRET_ID"] = secret_id
    yield


@pytest.fixture
def app(mock_env_retrieval, mock_secret_retrieval):
    from playground_stream_ingest.src.app import create_app

    app = create_app()
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    return app


@pytest.fixture
def app(mock_env_retrieval, mock_secret_retrieval):
    """Create and configure a test instance of the Flask app."""
    from playground_stream_ingest.src.app import create_app

    app = create_app()
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    return app


@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    return app.test_client()


@pytest.fixture
def runner(app):
    """Create a test runner for the Flask app's CLI commands."""
    return app.test_cli_runner()


@pytest.fixture
def sample_transaction():
    """Provide a sample valid transaction for testing."""
    return {
        "transaction_id": "txn_test_123456",
        "customer_id": "cust_test_789",
        "amount": 99.99,
        "currency": "USD",
        "transaction_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
        "merchant_id": "merch_test_555",
        "description": "Test transaction",
        "payment_method": {"type": "credit_card", "last_four": "1234", "provider": "Visa"},
        "location": {"country": "US", "city": "New York", "postal_code": "10001"},
    }


@pytest.fixture
def invalid_transaction():
    """Provide an invalid transaction for testing validation failures."""
    return {
        "transaction_id": "",  # Invalid: empty string
        "customer_id": "cust_test_789",
        "amount": -50.00,  # Invalid: negative amount
        "currency": "INVALID",  # Invalid: not in enum
        "transaction_type": "invalid_type",  # Invalid: not in enum
        "timestamp": "invalid-timestamp",  # Invalid: not ISO format
        "payment_method": {"type": "invalid_payment_type"},  # Invalid: not in enum
    }


@pytest.fixture
def minimal_transaction():
    """Provide a minimal valid transaction with only required fields."""
    return {
        "transaction_id": "txn_minimal_123",
        "customer_id": "cust_minimal_456",
        "amount": 25.50,
        "currency": "USD",
        "transaction_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
        "payment_method": {"type": "credit_card"},
    }
