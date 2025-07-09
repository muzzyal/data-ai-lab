import pytest
import tempfile
import pandas as pd
import os


@pytest.fixture(autouse=True)
def set_gcp_project_env():
    os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"


@pytest.fixture
def sample_transaction_csv():
    """Create a sample transaction CSV file for testing."""
    data = {
        "transaction_id": ["txn_001", "txn_002", "txn_003"],
        "customer_id": ["cust_001", "cust_002", "cust_003"],
        "amount": ["99.99", "149.50", "25.00"],
        "currency": ["USD", "USD", "USD"],
        "transaction_type": ["purchase", "purchase", "refund"],
        "timestamp": ["2024-01-15T10:30:00Z", "2024-01-15T11:00:00Z", "2024-01-15T12:00:00Z"],
        "payment_method_type": ["credit_card", "debit_card", "credit_card"],
        "payment_method_last_four": ["1234", "5678", "9012"],
        "payment_method_provider": ["Visa", "Mastercard", "Visa"],
    }

    df = pd.DataFrame(data)

    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    yield temp_file.name

    # Cleanup
    os.unlink(temp_file.name)
