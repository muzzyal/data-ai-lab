import pytest
from unittest.mock import patch
import hmac
import binascii
import hashlib
import os
import json
from flask import Flask
from playground_stream_ingest.tests.conftest import create_signature_and_body, retrieve_secret_key
from playground_stream_ingest.src.services.validator import TransactionValidator


def return_flask_app_context():
    secret_key = retrieve_secret_key()

    app = Flask(__name__)
    app.config["SECRET_KEY"] = secret_key

    return app.app_context()


class TestTransactionValidator:
    """Test cases for the TransactionValidator service."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.validator = TransactionValidator()

    def test_validator_initialisation(self):
        """Test that the validator initialises correctly."""
        assert self.validator is not None
        assert self.validator.schema is not None

    def test_valid_transaction_passes_schema_validation(self, sample_transaction):
        """Test that a valid transaction passes schema validation."""
        is_valid, error_message = self.validator.validate_transaction(sample_transaction)
        assert is_valid is True
        assert error_message == ""

    def test_valid_transaction_passes_business_validation(self, sample_transaction):
        """Test that a valid transaction passes business rules validation."""
        is_valid, error_message = self.validator.validate_required_fields(sample_transaction)
        assert is_valid is True
        assert error_message == ""

    def test_valid_transaction_passes_full_validation(self, sample_transaction):
        """Test that a valid transaction passes complete validation."""
        signature, body = create_signature_and_body(sample_transaction)

        with return_flask_app_context():
            is_valid, error_message = self.validator.full_validation(body, signature, sample_transaction)

        assert is_valid is True
        assert error_message == ""

    def test_minimal_transaction_passes_validation(self, minimal_transaction):
        """Test that a minimal valid transaction passes validation."""
        signature, body = create_signature_and_body(minimal_transaction)

        with return_flask_app_context():
            is_valid, error_message = self.validator.full_validation(body, signature, minimal_transaction)
        assert is_valid is True
        assert error_message == ""

    def test_missing_required_field_fails_schema_validation(self):
        """Test that missing required fields fail schema validation."""
        incomplete_transaction = {
            "transaction_id": "txn_incomplete",
            "customer_id": "cust_123",
            # Missing amount, currency, transaction_type, timestamp, payment_method
        }

        is_valid, error_message = self.validator.validate_transaction(incomplete_transaction)
        assert is_valid is False
        assert "required" in error_message.lower()

    def test_invalid_transaction_type_fails_validation(self):
        """Test that invalid transaction type fails validation."""
        invalid_transaction = {
            "transaction_id": "txn_123",
            "customer_id": "cust_123",
            "amount": 50.00,
            "currency": "USD",
            "transaction_type": "invalid_type",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_transaction(invalid_transaction)
        assert is_valid is False

    def test_negative_amount_fails_business_validation(self):
        """Test that negative amounts fail business validation."""
        transaction_with_negative_amount = {
            "transaction_id": "txn_negative",
            "customer_id": "cust_123",
            "amount": -50.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_required_fields(transaction_with_negative_amount)
        assert is_valid is False
        assert "positive" in error_message.lower()

    def test_zero_amount_fails_business_validation(self):
        """Test that zero amounts fail business validation."""
        transaction_with_zero_amount = {
            "transaction_id": "txn_zero",
            "customer_id": "cust_123",
            "amount": 0.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_required_fields(transaction_with_zero_amount)
        assert is_valid is False
        assert "positive" in error_message.lower()

    def test_empty_customer_id_fails_business_validation(self):
        """Test that empty customer ID fails business validation."""
        transaction_with_empty_customer_id = {
            "transaction_id": "txn_123",
            "customer_id": "",
            "amount": 50.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_required_fields(transaction_with_empty_customer_id)
        assert is_valid is False
        assert "customer id" in error_message.lower()

    def test_invalid_currency_fails_schema_validation(self):
        """Test that invalid currency codes fail schema validation."""
        transaction_with_invalid_currency = {
            "transaction_id": "txn_123",
            "customer_id": "cust_123",
            "amount": 50.00,
            "currency": "INVALID",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_transaction(transaction_with_invalid_currency)
        assert is_valid is False

    def test_invalid_payment_method_type_fails_schema_validation(self):
        """Test that invalid payment method type fails schema validation."""
        transaction_with_invalid_payment = {
            "transaction_id": "txn_123",
            "customer_id": "cust_123",
            "amount": 50.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "invalid_payment_type"},
        }

        is_valid, error_message = self.validator.validate_transaction(transaction_with_invalid_payment)
        assert is_valid is False

    def test_invalid_transaction_type_fails_business_validation(self):
        """Test that invalid transaction type fails business validation."""
        transaction_with_invalid_type = {
            "transaction_id": "txn_123",
            "customer_id": "cust_123",
            "amount": 50.00,
            "currency": "USD",
            "transaction_type": "invalid_type",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_required_fields(transaction_with_invalid_type)
        assert is_valid is False
        assert "invalid transaction type" in error_message.lower()

    def test_valid_transaction_types_pass_business_validation(self):
        """Test that all valid transaction types pass business validation."""
        valid_types = ["purchase", "refund", "transfer", "deposit", "withdrawal"]

        for transaction_type in valid_types:
            transaction = {
                "transaction_id": f"txn_{transaction_type}",
                "customer_id": "cust_123",
                "amount": 50.00,
                "currency": "USD",
                "transaction_type": transaction_type,
                "timestamp": "2024-01-15T10:30:00Z",
                "payment_method": {"type": "credit_card"},
            }

            is_valid, error_message = self.validator.validate_required_fields(transaction)
            assert is_valid is True, f"Transaction type '{transaction_type}' should be valid"

    def test_exception_handling_in_validation(self):
        """Test that exceptions during validation are handled gracefully."""
        # Pass invalid data type that might cause exceptions
        invalid_data = "not_a_dict"

        is_valid, error_message = self.validator.validate_transaction(invalid_data)
        assert is_valid is False
        assert "validation failed" in error_message.lower()

    def test_exception_handling_in_business_validation(self):
        """Test that exceptions during business validation are handled gracefully."""
        # Pass data that might cause attribute errors
        problematic_data = {
            "amount": "not_a_number",  # This might cause type errors
            "customer_id": None,
            "transaction_type": None,
        }

        is_valid, error_message = self.validator.validate_required_fields(problematic_data)
        assert is_valid is False
        assert "error" in error_message.lower()

    def test_edge_case_very_large_amount(self):
        """Test validation with very large amounts."""
        transaction_with_large_amount = {
            "transaction_id": "txn_large",
            "customer_id": "cust_123",
            "amount": 999999.99,  # Within schema limit
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        signature, body = create_signature_and_body(transaction_with_large_amount)

        with return_flask_app_context():
            is_valid, error_message = self.validator.full_validation(body, signature, transaction_with_large_amount)
        assert is_valid is True

    def test_edge_case_amount_too_large(self):
        """Test validation with amount exceeding schema limit."""
        transaction_with_too_large_amount = {
            "transaction_id": "txn_too_large",
            "customer_id": "cust_123",
            "amount": 1000000.01,  # Exceeds schema maximum
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        is_valid, error_message = self.validator.validate_transaction(transaction_with_too_large_amount)
        assert is_valid is False

    def test_edge_case_minimum_valid_amount(self):
        """Test validation with minimum valid amount."""
        transaction_with_minimum_amount = {
            "transaction_id": "txn_minimum",
            "customer_id": "cust_123",
            "amount": 0.01,  # Minimum valid amount
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }
        signature, body = create_signature_and_body(transaction_with_minimum_amount)

        with return_flask_app_context():
            is_valid, error_message = self.validator.full_validation(body, signature, transaction_with_minimum_amount)
        assert is_valid is True
