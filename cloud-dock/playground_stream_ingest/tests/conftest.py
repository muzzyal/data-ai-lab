import pytest
import sys
import os

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.app import create_app


@pytest.fixture
def app():
    """Create and configure a test instance of the Flask app."""
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
