"""
Enhanced tests for CSV processor functionality covering edge cases and error scenarios.
"""

import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from playground_batch_ingest.src.services.csv_processor import CSVProcessor


@pytest.fixture
def csv_processor():
    """Create CSV processor instance for testing."""
    return CSVProcessor(batch_size=5, encoding="utf-8")


@pytest.fixture
def sample_shop_csv():
    """Create a sample shop CSV file for testing."""
    data = {
        "shop_id": ["shop_001", "shop_002", "shop_003"],
        "name": ["Test Shop 1", "Test Shop 2", "Test Shop 3"],
        "category": ["electronics", "clothing", "food_beverage"],
        "status": ["active", "active", "inactive"],
        "owner_name": ["John Doe", "Jane Smith", "Bob Johnson"],
        "owner_email": ["john@test.com", "jane@test.com", "bob@test.com"],
        "address_street": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
        "address_city": ["New York", "Los Angeles", "Chicago"],
        "address_country": ["US", "US", "US"],
        "registration_date": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"],
    }

    df = pd.DataFrame(data)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    yield temp_file.name

    os.unlink(temp_file.name)


@pytest.fixture
def sample_product_csv():
    """Create a sample product CSV file for testing."""
    data = {
        "product_id": ["prod_001", "prod_002", "prod_003"],
        "sku": ["SKU-001", "SKU-002", "SKU-003"],
        "name": ["Product 1", "Product 2", "Product 3"],
        "category": ["electronics", "clothing", "books_media"],
        "price_amount": ["99.99", "49.50", "29.95"],
        "price_currency": ["USD", "USD", "USD"],
        "inventory_quantity": ["10", "25", "5"],
        "shop_id": ["shop_001", "shop_002", "shop_003"],
        "status": ["active", "active", "out_of_stock"],
        "created_date": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", "2024-01-03T00:00:00Z"],
    }

    df = pd.DataFrame(data)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    yield temp_file.name

    os.unlink(temp_file.name)


def test_detect_shop_data_type(csv_processor, sample_shop_csv):
    """Test auto-detection of shop data type."""
    data_type = csv_processor._detect_data_type(sample_shop_csv)
    assert data_type == "shop"


def test_detect_product_data_type(csv_processor, sample_product_csv):
    """Test auto-detection of product data type."""
    data_type = csv_processor._detect_data_type(sample_product_csv)
    assert data_type == "product"


def test_detect_data_type_fallback(csv_processor):
    """Test data type detection fallback to transaction."""
    # Create CSV with unknown headers
    data = {
        "unknown_header1": ["value1", "value2"],
        "unknown_header2": ["value3", "value4"],
    }
    df = pd.DataFrame(data)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    try:
        data_type = csv_processor._detect_data_type(temp_file.name)
        assert data_type == "transaction"  # Should fallback to transaction
    finally:
        os.unlink(temp_file.name)


def test_detect_data_type_error_handling(csv_processor):
    """Test data type detection error handling."""
    # Test with non-existent file
    data_type = csv_processor._detect_data_type("/non/existent/file.csv")
    assert data_type == "transaction"  # Should fallback to transaction


def test_process_shop_csv(csv_processor, sample_shop_csv):
    """Test processing of shop CSV file."""
    result = csv_processor.process_csv_file(sample_shop_csv, data_type="shop")

    assert result["data_type"] == "shop"
    assert result["total_rows"] == 3
    assert result["processed_rows"] == 3
    assert result["error_count"] == 0
    assert len(result["data"]) == 3

    # Check first record structure
    first_record = result["data"][0]
    assert first_record["shop_id"] == "shop_001"
    assert first_record["name"] == "Test Shop 1"
    assert first_record["category"] == "electronics"
    assert first_record["owner"]["name"] == "John Doe"
    assert first_record["owner"]["email"] == "john@test.com"
    assert first_record["address"]["street"] == "123 Main St"


def test_process_product_csv(csv_processor, sample_product_csv):
    """Test processing of product CSV file."""
    result = csv_processor.process_csv_file(sample_product_csv, data_type="product")

    assert result["data_type"] == "product"
    assert result["total_rows"] == 3
    assert result["processed_rows"] == 3
    assert result["error_count"] == 0
    assert len(result["data"]) == 3

    # Check first record structure
    first_record = result["data"][0]
    assert first_record["product_id"] == "prod_001"
    assert first_record["sku"] == "SKU-001"
    assert first_record["name"] == "Product 1"
    assert first_record["price"]["amount"] == 99.99
    assert first_record["price"]["currency"] == "USD"
    assert first_record["inventory"]["quantity"] == 10


def test_transform_shop_row(csv_processor):
    """Test shop row transformation."""
    row_data = {
        "shop_id": "shop_123",
        "name": "Test Shop",
        "category": "electronics",
        "status": "active",
        "owner_name": "John Doe",
        "owner_email": "john@test.com",
        "owner_phone": "+1234567890",
        "address_street": "123 Main St",
        "address_city": "New York",
        "address_state": "NY",
        "address_postal_code": "10001",
        "address_country": "US",
        "contact_phone": "+1234567890",
        "contact_email": "shop@test.com",
        "contact_website": "https://testshop.com",
        "business_hours_monday": "09:00-17:00",
        "business_hours_tuesday": "09:00-17:00",
        "business_hours_wednesday": "closed",
        "registration_date": "2024-01-01T00:00:00Z",
        "description": "A test shop",
        "last_updated": "2024-01-15T10:30:00Z",
    }

    result = csv_processor._transform_shop_row(row_data)

    assert result["shop_id"] == "shop_123"
    assert result["name"] == "Test Shop"
    assert result["category"] == "electronics"
    assert result["status"] == "active"
    assert result["description"] == "A test shop"
    assert result["owner"]["name"] == "John Doe"
    assert result["owner"]["email"] == "john@test.com"
    assert result["owner"]["phone"] == "+1234567890"
    assert result["address"]["street"] == "123 Main St"
    assert result["address"]["city"] == "New York"
    assert result["address"]["state"] == "NY"
    assert result["address"]["postal_code"] == "10001"
    assert result["address"]["country"] == "US"
    assert result["contact"]["phone"] == "+1234567890"
    assert result["contact"]["email"] == "shop@test.com"
    assert result["contact"]["website"] == "https://testshop.com"
    assert result["business_hours"]["monday"] == "09:00-17:00"
    assert result["business_hours"]["tuesday"] == "09:00-17:00"
    assert result["business_hours"]["wednesday"] == "closed"
    assert result["registration_date"] == "2024-01-01T00:00:00Z"
    assert result["last_updated"] == "2024-01-15T10:30:00Z"


def test_transform_product_row(csv_processor):
    """Test product row transformation."""
    row_data = {
        "product_id": "prod_123",
        "sku": "SKU-123456",
        "name": "Test Product",
        "description": "A test product",
        "category": "electronics",
        "subcategory": "smartphones",
        "brand": "TestBrand",
        "price_amount": "99.99",
        "price_currency": "USD",
        "price_discount_amount": "10.00",
        "price_discount_percentage": "10.0",
        "inventory_quantity": "10",
        "inventory_reserved": "2",
        "inventory_warehouse_location": "Warehouse A",
        "dimensions_length": "15.0",
        "dimensions_width": "8.0",
        "dimensions_height": "1.0",
        "dimensions_weight": "200.0",
        "attributes_color": "black",
        "attributes_size": "large",
        "attributes_material": "plastic",
        "attributes_style": "modern",
        "shop_id": "shop_456",
        "status": "active",
        "images": '["https://example.com/img1.jpg", "https://example.com/img2.jpg"]',
        "tags": '["electronics", "smartphone", "popular"]',
        "created_date": "2024-01-01T00:00:00Z",
        "last_updated": "2024-01-15T10:30:00Z",
    }

    result = csv_processor._transform_product_row(row_data)

    assert result["product_id"] == "prod_123"
    assert result["sku"] == "SKU-123456"
    assert result["name"] == "Test Product"
    assert result["description"] == "A test product"
    assert result["category"] == "electronics"
    assert result["subcategory"] == "smartphones"
    assert result["brand"] == "TestBrand"
    assert result["price"]["amount"] == 99.99
    assert result["price"]["currency"] == "USD"
    assert result["price"]["discount_amount"] == 10.00
    assert result["price"]["discount_percentage"] == 10.0
    assert result["inventory"]["quantity"] == 10
    assert result["inventory"]["reserved"] == 2
    assert result["inventory"]["warehouse_location"] == "Warehouse A"
    assert result["dimensions"]["length"] == 15.0
    assert result["dimensions"]["width"] == 8.0
    assert result["dimensions"]["height"] == 1.0
    assert result["dimensions"]["weight"] == 200.0
    assert result["attributes"]["color"] == "black"
    assert result["attributes"]["size"] == "large"
    assert result["attributes"]["material"] == "plastic"
    assert result["attributes"]["style"] == "modern"
    assert result["shop_id"] == "shop_456"
    assert result["status"] == "active"
    assert result["images"] == ["https://example.com/img1.jpg", "https://example.com/img2.jpg"]
    assert result["tags"] == ["electronics", "smartphone", "popular"]
    assert result["created_date"] == "2024-01-01T00:00:00Z"
    assert result["last_updated"] == "2024-01-15T10:30:00Z"


def test_transform_product_row_with_comma_separated_tags(csv_processor):
    """Test product row transformation with comma-separated tags (not JSON)."""
    row_data = {
        "product_id": "prod_123",
        "sku": "SKU-123456",
        "name": "Test Product",
        "category": "electronics",
        "price_amount": "99.99",
        "price_currency": "USD",
        "inventory_quantity": "10",
        "shop_id": "shop_456",
        "status": "active",
        "tags": "electronics, smartphone, popular",  # Comma-separated instead of JSON
        "created_date": "2024-01-01T00:00:00Z",
    }

    result = csv_processor._transform_product_row(row_data)

    assert result["tags"] == ["electronics", "smartphone", "popular"]


def test_transform_product_row_invalid_json(csv_processor):
    """Test product row transformation with invalid JSON for images/tags."""
    row_data = {
        "product_id": "prod_123",
        "sku": "SKU-123456",
        "name": "Test Product",
        "category": "electronics",
        "price_amount": "99.99",
        "price_currency": "USD",
        "inventory_quantity": "10",
        "shop_id": "shop_456",
        "status": "active",
        "images": "invalid-json-string",  # Invalid JSON
        "tags": "invalid-json-string",  # Invalid JSON
        "created_date": "2024-01-01T00:00:00Z",
    }

    result = csv_processor._transform_product_row(row_data)

    # Should fallback to treating as single string
    assert result["images"] == ["invalid-json-string"]
    assert result["tags"] == ["invalid-json-string"]


def test_process_csv_file_with_validation_errors(csv_processor):
    """Test processing CSV file with validation errors."""
    # Create CSV with invalid transaction data
    data = {
        "transaction_id": ["txn_001", "txn_002", ""],  # Empty transaction_id
        "customer_id": ["cust_001", "", "cust_003"],  # Empty customer_id
        "amount": ["99.99", "invalid", "25.00"],  # Invalid amount
        "currency": ["USD", "USD", "INVALID"],  # Invalid currency
        "transaction_type": ["purchase", "purchase", "refund"],
        "timestamp": ["2024-01-15T10:30:00Z", "2024-01-15T11:00:00Z", "2024-01-15T12:00:00Z"],
        "payment_method_type": ["credit_card", "debit_card", "credit_card"],
        "payment_method_last_four": ["1234", "5678", "9012"],
        "payment_method_provider": ["Visa", "Mastercard", "Visa"],
    }

    df = pd.DataFrame(data)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    try:
        result = csv_processor.process_csv_file(temp_file.name, data_type="transaction")

        assert result["data_type"] == "transaction"
        assert result["total_rows"] == 3
        assert result["error_count"] > 0  # Should have validation errors
        assert len(result["errors"]) > 0

        # At least one record should have been processed successfully
        assert result["processed_rows"] > 0

    finally:
        os.unlink(temp_file.name)


def test_process_csv_file_large_batch(csv_processor):
    """Test processing CSV file with multiple batches."""
    # Create CSV with more rows than batch size (5)
    data = {
        "transaction_id": [f"txn_{i:03d}" for i in range(12)],
        "customer_id": [f"cust_{i:03d}" for i in range(12)],
        "amount": ["99.99"] * 12,
        "currency": ["USD"] * 12,
        "transaction_type": ["purchase"] * 12,
        "timestamp": ["2024-01-15T10:30:00Z"] * 12,
        "payment_method_type": ["credit_card"] * 12,
        "payment_method_last_four": ["1234"] * 12,
        "payment_method_provider": ["Visa"] * 12,
    }

    df = pd.DataFrame(data)

    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()

    try:
        result = csv_processor.process_csv_file(temp_file.name, data_type="transaction")

        assert result["data_type"] == "transaction"
        assert result["total_rows"] == 12
        assert result["processed_rows"] == 12
        assert result["error_count"] == 0
        assert len(result["data"]) == 12

    finally:
        os.unlink(temp_file.name)


def test_process_csv_file_unsupported_data_type(csv_processor, sample_transaction_csv):
    """Test processing CSV file with unsupported data type."""
    result = csv_processor.process_csv_file(sample_transaction_csv, data_type="unsupported")

    assert "Unsupported data type" in result["errors"][0]["error"]


def test_process_csv_file_exception_handling(csv_processor):
    """Test CSV file processing exception handling."""
    # Test with non-existent file
    result = csv_processor.process_csv_file("/non/existent/file.csv")

    assert result["data_type"] == "transaction"  # Should fallback to transaction
    assert result["total_rows"] == 0
    assert result["processed_rows"] == 0
    assert result["error_count"] == 1
    assert len(result["errors"]) == 1


def test_process_batch_error_handling(csv_processor):
    """Test batch processing error handling."""
    # Create a mock DataFrame
    df = pd.DataFrame(
        {
            "transaction_id": ["txn_001"],
            "customer_id": ["cust_001"],
            "amount": ["99.99"],
            "currency": ["USD"],
            "transaction_type": ["purchase"],
            "timestamp": ["2024-01-15T10:30:00Z"],
            "payment_method_type": ["credit_card"],
        }
    )

    schema_info = {
        "transformer": csv_processor._transform_transaction_row,
        "schema": {"type": "object", "properties": {}},  # Minimal schema
    }

    # Mock transformer to raise exception
    def failing_transformer(row):
        raise ValueError("Transformer error")

    schema_info["transformer"] = failing_transformer

    batch_data, batch_errors = csv_processor._process_batch(df, schema_info, 0, "transaction")

    assert len(batch_data) == 0
    assert len(batch_errors) == 1
    assert "Processing error" in batch_errors[0]["error"]


def test_csv_processor_different_encoding():
    """Test CSV processor with different encoding."""
    processor = CSVProcessor(batch_size=10, encoding="latin-1")
    assert processor.encoding == "latin-1"


def test_csv_processor_different_batch_size():
    """Test CSV processor with different batch size."""
    processor = CSVProcessor(batch_size=500, encoding="utf-8")
    assert processor.batch_size == 500


def test_transform_transaction_row_minimal_data(csv_processor):
    """Test transaction row transformation with minimal required data."""
    row_data = {
        "transaction_id": "txn_123",
        "customer_id": "cust_456",
        "amount": "99.99",
        "currency": "USD",
        "transaction_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
        "payment_method_type": "credit_card",
        # No optional fields
    }

    result = csv_processor._transform_transaction_row(row_data)

    assert result["transaction_id"] == "txn_123"
    assert result["customer_id"] == "cust_456"
    assert result["amount"] == 99.99
    assert result["currency"] == "USD"
    assert result["payment_method"]["type"] == "credit_card"
    assert result["payment_method"]["last_four"] == ""
    assert result["payment_method"]["provider"] == ""
    # Optional fields should not be present
    assert "merchant_id" not in result
    assert "description" not in result
    assert "location" not in result


def test_transform_transaction_row_empty_amount(csv_processor):
    """Test transaction row transformation with empty amount."""
    row_data = {
        "transaction_id": "txn_123",
        "customer_id": "cust_456",
        "amount": "",  # Empty amount
        "currency": "USD",
        "transaction_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
        "payment_method_type": "credit_card",
    }

    result = csv_processor._transform_transaction_row(row_data)

    assert result["amount"] == 0.0  # Should default to 0.0


def test_transform_shop_row_minimal_data(csv_processor):
    """Test shop row transformation with minimal required data."""
    row_data = {
        "shop_id": "shop_123",
        "name": "Test Shop",
        "category": "electronics",
        "status": "active",
        "owner_name": "John Doe",
        "owner_email": "john@test.com",
        "address_street": "123 Main St",
        "address_city": "New York",
        "address_country": "US",
        "registration_date": "2024-01-01T00:00:00Z",
        # No optional fields
    }

    result = csv_processor._transform_shop_row(row_data)

    assert result["shop_id"] == "shop_123"
    assert result["name"] == "Test Shop"
    assert result["owner"]["name"] == "John Doe"
    assert result["owner"]["email"] == "john@test.com"
    assert result["address"]["street"] == "123 Main St"
    # Optional fields should not be present
    assert "description" not in result
    assert "contact" not in result
    assert "business_hours" not in result
