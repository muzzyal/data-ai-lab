"""
Tests for CSV processor functionality.
"""

import os
import tempfile

import pandas as pd
import pytest

from playground_batch_ingest.src.services.csv_processor import CSVProcessor


@pytest.fixture
def csv_processor():
    """Create CSV processor instance for testing."""
    return CSVProcessor(batch_size=10, encoding="utf-8")


def test_detect_transaction_data_type(csv_processor, sample_transaction_csv):
    """Test auto-detection of transaction data type."""
    data_type = csv_processor._detect_data_type(sample_transaction_csv)
    assert data_type == "transaction"


def test_process_transaction_csv(csv_processor, sample_transaction_csv):
    """Test processing of transaction CSV file."""
    result = csv_processor.process_csv_file(sample_transaction_csv, data_type="transaction")

    assert result["data_type"] == "transaction"
    assert result["total_rows"] == 3
    assert result["processed_rows"] == 3
    assert result["error_count"] == 0
    assert len(result["data"]) == 3

    # Check first record structure
    first_record = result["data"][0]
    assert first_record["transaction_id"] == "txn_001"
    assert first_record["customer_id"] == "cust_001"
    assert first_record["amount"] == 99.99
    assert first_record["currency"] == "USD"
    assert first_record["payment_method"]["type"] == "credit_card"
    assert first_record["payment_method"]["last_four"] == "1234"


def test_transform_transaction_row(csv_processor):
    """Test transaction row transformation."""
    row_data = {
        "transaction_id": "txn_123",
        "customer_id": "cust_456",
        "amount": "99.99",
        "currency": "USD",
        "transaction_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
        "payment_method_type": "credit_card",
        "payment_method_last_four": "1234",
        "payment_method_provider": "Visa",
        "merchant_id": "merch_789",
        "description": "Test purchase",
    }

    result = csv_processor._transform_transaction_row(row_data)

    assert result["transaction_id"] == "txn_123"
    assert result["customer_id"] == "cust_456"
    assert result["amount"] == 99.99
    assert result["currency"] == "USD"
    assert result["merchant_id"] == "merch_789"
    assert result["description"] == "Test purchase"
    assert result["payment_method"]["type"] == "credit_card"
    assert result["payment_method"]["last_four"] == "1234"
    assert result["payment_method"]["provider"] == "Visa"


if __name__ == "__main__":
    pytest.main([__file__])
