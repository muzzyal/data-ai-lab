"""
Tests for schema validation and CSV header definitions.
"""

import pytest
import pandas as pd
from jsonschema import validate, ValidationError, FormatChecker
from playground_batch_ingest.src.services.csv_processor import CSVProcessor
from playground_batch_ingest.src.schemas.transaction_schema import TRANSACTION_SCHEMA, TRANSACTION_CSV_HEADERS
from playground_batch_ingest.src.schemas.shop_schema import SHOP_SCHEMA, SHOP_CSV_HEADERS
from playground_batch_ingest.src.schemas.product_schema import PRODUCT_SCHEMA, PRODUCT_CSV_HEADERS


class TestTransactionSchema:
    """Tests for transaction schema validation."""

    def test_valid_transaction(self):
        """Test validation of valid transaction data."""
        valid_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card", "last_four": "1234", "provider": "Visa"},
        }

        # Should not raise an exception
        validate(instance=valid_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_valid_transaction_with_optional_fields(self):
        """Test validation of transaction with optional fields."""
        transaction_with_optional = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "merchant_id": "merch_456",
            "description": "Test purchase",
            "payment_method": {"type": "credit_card", "last_four": "1234", "provider": "Visa"},
            "location": {"country": "US", "city": "New York", "postal_code": "10001"},
        }

        validate(instance=transaction_with_optional, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        incomplete_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            # Missing amount, currency, transaction_type, timestamp, payment_method
        }

        with pytest.raises(ValidationError):
            validate(instance=incomplete_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_invalid_currency(self):
        """Test validation with invalid currency."""
        invalid_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "INVALID",  # Not in enum
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_invalid_amount(self):
        """Test validation with invalid amount."""
        invalid_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": -10.00,  # Negative amount
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_invalid_payment_method(self):
        """Test validation with invalid payment method."""
        invalid_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "invalid_type", "last_four": "1234"},  # Not in enum
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_invalid_last_four(self):
        """Test validation with invalid last_four digits."""
        invalid_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card", "last_four": "12"},  # Not 4 digits
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_transaction_csv_headers(self):
        """Test that transaction CSV headers are properly defined."""
        expected_headers = [
            "transaction_id",
            "customer_id",
            "amount",
            "currency",
            "transaction_type",
            "timestamp",
            "merchant_id",
            "description",
            "payment_method_type",
            "payment_method_last_four",
            "payment_method_provider",
            "location_country",
            "location_city",
            "location_postal_code",
        ]

        assert TRANSACTION_CSV_HEADERS == expected_headers


class TestShopSchema:
    """Tests for shop schema validation."""

    def test_valid_shop(self):
        """Test validation of valid shop data."""
        valid_shop = {
            "shop_id": "shop_123",
            "name": "Test Shop",
            "category": "electronics",
            "status": "active",
            "owner": {"name": "John Doe", "email": "john@example.com"},
            "address": {"street": "123 Main St", "city": "New York", "country": "US"},
            "registration_date": "2024-01-01T00:00:00Z",
        }

        validate(instance=valid_shop, schema=SHOP_SCHEMA, format_checker=FormatChecker())

    def test_valid_shop_with_optional_fields(self):
        """Test validation of shop with optional fields."""
        shop_with_optional = {
            "shop_id": "shop_123",
            "name": "Test Shop",
            "description": "A test electronics shop",
            "category": "electronics",
            "status": "active",
            "owner": {"name": "John Doe", "email": "john@example.com", "phone": "+1234567890"},
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "postal_code": "10001",
                "country": "US",
            },
            "contact": {"phone": "+1234567890", "email": "shop@example.com", "website": "https://testshop.com"},
            "business_hours": {"monday": "09:00-17:00", "tuesday": "09:00-17:00", "wednesday": "closed"},
            "registration_date": "2024-01-01T00:00:00Z",
            "last_updated": "2024-01-15T10:30:00Z",
        }

        validate(instance=shop_with_optional, schema=SHOP_SCHEMA, format_checker=FormatChecker())

    def test_missing_required_shop_fields(self):
        """Test validation with missing required fields."""
        incomplete_shop = {
            "shop_id": "shop_123",
            "name": "Test Shop",
            # Missing category, status, owner, address, registration_date
        }

        with pytest.raises(ValidationError):
            validate(instance=incomplete_shop, schema=SHOP_SCHEMA, format_checker=FormatChecker())

    def test_invalid_shop_category(self):
        """Test validation with invalid category."""
        invalid_shop = {
            "shop_id": "shop_123",
            "name": "Test Shop",
            "category": "invalid_category",
            "status": "active",
            "owner": {"name": "John Doe", "email": "john@example.com"},
            "address": {"street": "123 Main St", "city": "New York", "country": "US"},
            "registration_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_shop, schema=SHOP_SCHEMA, format_checker=FormatChecker())

    def test_invalid_email_format(self):
        """Test validation with invalid email format."""
        invalid_shop = {
            "shop_id": "shop_123",
            "name": "Test Shop",
            "category": "electronics",
            "status": "active",
            "owner": {"name": "John Doe", "email": "invalid-email"},  # Invalid format
            "address": {"street": "123 Main St", "city": "New York", "country": "US"},
            "registration_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_shop, schema=SHOP_SCHEMA, format_checker=FormatChecker())

    def test_shop_csv_headers(self):
        """Test that shop CSV headers are properly defined."""
        expected_headers = [
            "shop_id",
            "name",
            "description",
            "category",
            "status",
            "owner_name",
            "owner_email",
            "owner_phone",
            "address_street",
            "address_city",
            "address_state",
            "address_postal_code",
            "address_country",
            "contact_phone",
            "contact_email",
            "contact_website",
            "business_hours_monday",
            "business_hours_tuesday",
            "business_hours_wednesday",
            "business_hours_thursday",
            "business_hours_friday",
            "business_hours_saturday",
            "business_hours_sunday",
            "registration_date",
            "last_updated",
        ]

        assert SHOP_CSV_HEADERS == expected_headers


class TestProductSchema:
    """Tests for product schema validation."""

    def test_valid_product(self):
        """Test validation of valid product data."""
        valid_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "category": "electronics",
            "price": {"amount": 99.99, "currency": "USD"},
            "inventory": {"quantity": 10},
            "shop_id": "shop_456",
            "status": "active",
            "created_date": "2024-01-01T00:00:00Z",
        }

        validate(instance=valid_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_valid_product_with_optional_fields(self):
        """Test validation of product with optional fields."""
        product_with_optional = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "description": "A test electronic product",
            "category": "electronics",
            "subcategory": "smartphones",
            "brand": "TestBrand",
            "price": {"amount": 99.99, "currency": "USD", "discount_amount": 10.00, "discount_percentage": 10.0},
            "inventory": {"quantity": 10, "reserved": 2, "warehouse_location": "Warehouse A"},
            "dimensions": {"length": 15.0, "width": 8.0, "height": 1.0, "weight": 200.0},
            "attributes": {"color": "black", "size": "large", "material": "plastic", "style": "modern"},
            "shop_id": "shop_456",
            "status": "active",
            "images": ["https://example.com/image1.jpg", "https://example.com/image2.jpg"],
            "tags": ["electronics", "smartphone", "popular"],
            "created_date": "2024-01-01T00:00:00Z",
            "last_updated": "2024-01-15T10:30:00Z",
        }

        validate(instance=product_with_optional, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_missing_required_product_fields(self):
        """Test validation with missing required fields."""
        incomplete_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            # Missing name, category, price, inventory, shop_id, status, created_date
        }

        with pytest.raises(ValidationError):
            validate(instance=incomplete_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_invalid_product_category(self):
        """Test validation with invalid category."""
        invalid_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "category": "invalid_category",
            "price": {"amount": 99.99, "currency": "USD"},
            "inventory": {"quantity": 10},
            "shop_id": "shop_456",
            "status": "active",
            "created_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_invalid_price_amount(self):
        """Test validation with invalid price amount."""
        invalid_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "category": "electronics",
            "price": {"amount": -10.00, "currency": "USD"},  # Negative price
            "inventory": {"quantity": 10},
            "shop_id": "shop_456",
            "status": "active",
            "created_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_invalid_inventory_quantity(self):
        """Test validation with invalid inventory quantity."""
        invalid_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "category": "electronics",
            "price": {"amount": 99.99, "currency": "USD"},
            "inventory": {"quantity": -5},  # Negative quantity
            "shop_id": "shop_456",
            "status": "active",
            "created_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_too_many_images(self):
        """Test validation with too many images."""
        invalid_product = {
            "product_id": "prod_123",
            "sku": "SKU-123456",
            "name": "Test Product",
            "category": "electronics",
            "price": {"amount": 99.99, "currency": "USD"},
            "inventory": {"quantity": 10},
            "shop_id": "shop_456",
            "status": "active",
            "images": [f"https://example.com/image{i}.jpg" for i in range(15)],  # More than 10 images
            "created_date": "2024-01-01T00:00:00Z",
        }

        with pytest.raises(ValidationError):
            validate(instance=invalid_product, schema=PRODUCT_SCHEMA, format_checker=FormatChecker())

    def test_product_csv_headers(self):
        """Test that product CSV headers are properly defined."""
        expected_headers = [
            "product_id",
            "sku",
            "name",
            "description",
            "category",
            "subcategory",
            "brand",
            "price_amount",
            "price_currency",
            "price_discount_amount",
            "price_discount_percentage",
            "inventory_quantity",
            "inventory_reserved",
            "inventory_warehouse_location",
            "dimensions_length",
            "dimensions_width",
            "dimensions_height",
            "dimensions_weight",
            "attributes_color",
            "attributes_size",
            "attributes_material",
            "attributes_style",
            "shop_id",
            "status",
            "images",
            "tags",
            "created_date",
            "last_updated",
        ]

        assert PRODUCT_CSV_HEADERS == expected_headers


class TestSchemaEdgeCases:
    """Tests for edge cases and schema robustness."""

    def test_empty_optional_objects(self):
        """Test handling of empty optional objects."""
        transaction_with_empty_location = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
            "location": {},  # Empty but present
        }

        validate(instance=transaction_with_empty_location, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_string_length_limits(self):
        """Test string length validation."""
        # Test maximum length for transaction ID
        long_transaction = {
            "transaction_id": "a" * 101,  # Exceeds 100 char limit
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        with pytest.raises(ValidationError):
            validate(instance=long_transaction, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())

    def test_numeric_precision(self):
        """Test numeric precision validation."""
        # Test multipleOf constraint for amounts
        invalid_amount_transaction = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.999,  # More than 2 decimal places
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
        }

        csv_processor = CSVProcessor()
        schema_mapping = {
            "schema": TRANSACTION_SCHEMA,
            "headers": TRANSACTION_CSV_HEADERS,
            "transformer": csv_processor._transform_transaction_row,
        }
        df = pd.DataFrame([invalid_amount_transaction])

        (batch_data, batch_errors) = csv_processor._process_batch(df, schema_mapping, 0, "transaction")
        expected_batch_errors = [
            {
                "row": 1,
                "error": "Schema validation error: Amount 99.999 has more than 2 decimal places",
                "data": invalid_amount_transaction,
            }
        ]

        assert batch_data == []
        assert batch_errors == expected_batch_errors

    def test_additional_properties_rejected(self):
        """Test that additional properties are rejected."""
        transaction_with_extra_field = {
            "transaction_id": "txn_123456",
            "customer_id": "cust_789",
            "amount": 99.99,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
            "extra_field": "not allowed",  # Additional property
        }

        with pytest.raises(ValidationError):
            validate(instance=transaction_with_extra_field, schema=TRANSACTION_SCHEMA, format_checker=FormatChecker())
