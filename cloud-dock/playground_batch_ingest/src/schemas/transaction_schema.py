"""
JSON Schema definition for transaction data validation.
This schema defines the structure and validation rules for incoming transaction data.
"""

TRANSACTION_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/transaction-schema.json",
    "title": "Customer Transaction",
    "description": "Schema for validating customer transaction data",
    "type": "object",
    "properties": {
        "transaction_id": {
            "type": "string",
            "description": "Unique identifier for the transaction",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "minLength": 1,
            "maxLength": 100,
        },
        "customer_id": {
            "type": "string",
            "description": "Unique identifier for the customer",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "minLength": 1,
            "maxLength": 50,
        },
        "amount": {
            "type": "number",
            "description": "Transaction amount",
            "minimum": 0.01,
            "maximum": 1000000.00,
            "multipleOf": 0.01,
        },
        "currency": {
            "type": "string",
            "description": "Currency code (ISO 4217)",
            "pattern": "^[A-Z]{3}$",
            "enum": ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"],
        },
        "transaction_type": {
            "type": "string",
            "description": "Type of transaction",
            "enum": ["purchase", "refund", "transfer", "deposit", "withdrawal"],
        },
        "timestamp": {
            "type": "string",
            "description": "Transaction timestamp in ISO 8601 format",
            "format": "date-time",
        },
        "merchant_id": {
            "type": "string",
            "description": "Merchant identifier (optional for some transaction types)",
            "pattern": "^[a-zA-Z0-9_-]*$",
            "maxLength": 50,
        },
        "description": {"type": "string", "description": "Transaction description", "maxLength": 500},
        "payment_method": {
            "type": "object",
            "description": "Payment method details",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["credit_card", "debit_card", "bank_transfer", "digital_wallet", "cash"],
                },
                "last_four": {
                    "type": "string",
                    "description": "Last four digits of payment method (if applicable)",
                    "pattern": "^[0-9]{4}$",
                },
                "provider": {"type": "string", "description": "Payment provider", "maxLength": 50},
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "location": {
            "type": "object",
            "description": "Transaction location (optional)",
            "properties": {
                "country": {
                    "type": "string",
                    "description": "Country code (ISO 3166-1 alpha-2)",
                    "pattern": "^[A-Z]{2}$",
                },
                "city": {"type": "string", "description": "City name", "maxLength": 100},
                "postal_code": {"type": "string", "description": "Postal/ZIP code", "maxLength": 20},
            },
            "additionalProperties": False,
        },
        "metadata": {"type": "object", "description": "Additional metadata (optional)", "additionalProperties": True},
    },
    "required": [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "transaction_type",
        "timestamp",
        "payment_method",
    ],
    "additionalProperties": False,
}

# CSV headers for transaction data
TRANSACTION_CSV_HEADERS = [
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
