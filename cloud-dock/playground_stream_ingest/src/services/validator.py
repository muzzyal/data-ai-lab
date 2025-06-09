import logging
from typing import Dict, Any, Tuple
from jsonschema import validate, ValidationError
from playground_stream_ingest.src.schemas.transaction_schema import TRANSACTION_SCHEMA
import hmac
import binascii
import hashlib
from flask import current_app as app

logger = logging.getLogger(__name__)


class TransactionValidator:
    """Service for validating transaction data against JSON schema."""

    def __init__(self):
        self.schema = TRANSACTION_SCHEMA
        logger.info("TransactionValidator initialized")

    def verify_signature(self, signature, body, secret):
        """Verify HMAC signature for the transaction data.

        Args:
            signature: HMAC signature to verify
            body: Raw transaction data
            secret: Secret key used for HMAC
        Returns:
            bool: True if signature is valid, False otherwise"""

        return hmac.new(binascii.a2b_hex(secret), body, hashlib.sha512).hexdigest() == signature

    def validate_transaction(self, data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate transaction data against the schema.

        Args:
            data: Transaction data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            validate(instance=data, schema=self.schema)
            logger.debug(
                f"Transaction validation successful for transaction_id: {data.get('transaction_id', 'unknown')}"
            )
            return True, ""
        except ValidationError as e:
            error_msg = f"Schema validation failed: {e.message}"
            logger.warning(f"Transaction validation failed: {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected validation error: {str(e)}"
            logger.error(f"Unexpected validation error: {error_msg}")
            return False, error_msg

    def validate_required_fields(self, data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Additional validation for critical business rules.

        Args:
            data: Transaction data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check for positive amount
            amount = data.get("amount", 0)
            if amount <= 0:
                return False, "Transaction amount must be positive"

            # Check for valid customer_id
            customer_id = data.get("customer_id", "")
            if not customer_id or not str(customer_id).strip():
                return False, "Customer ID cannot be empty"

            # Check for valid transaction_type
            valid_types = ["purchase", "refund", "transfer", "deposit", "withdrawal"]
            transaction_type = data.get("transaction_type", "").lower()
            if transaction_type not in valid_types:
                return False, f"Invalid transaction type. Must be one of: {', '.join(valid_types)}"

            logger.debug(f"Business rules validation successful for transaction: {data.get('transaction_id')}")
            return True, ""

        except Exception as e:
            error_msg = f"Business validation error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def full_validation(self, body: bytes, signature: str, data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Perform complete validation including schema and business rules.

        Args:
            data: Transaction data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # First validate signature
        secret_key = app.config.get("SECRET_KEY", "")

        if not secret_key:
            return False, "Secret Key not configured"

        valid_secret = self.verify_signature(signature, body, secret_key)

        if not valid_secret:
            return False, "Invalid signature"

        logger.info(f"Signature validation successful for transaction: {data.get('transaction_id')}")

        # Then validate against schema
        schema_valid, schema_error = self.validate_transaction(data)
        if not schema_valid:
            return False, schema_error

        # Finally validate business rules
        business_valid, business_error = self.validate_required_fields(data)
        if not business_valid:
            return False, business_error

        logger.info(f"Full validation successful for transaction: {data.get('transaction_id')}")
        return True, ""
