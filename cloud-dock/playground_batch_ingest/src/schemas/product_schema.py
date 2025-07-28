"""
JSON Schema definition for product data validation.

This module contains the JSON schema and CSV header definitions for product data
validation. The schema enforces data integrity, business rules, and format
constraints for product catalog information.

Schema Features:
    - Comprehensive product information validation
    - Price validation with currency constraints
    - Inventory tracking with quantity limits
    - Product dimensions and attributes support
    - Image and tag management
    - Status lifecycle management

Business Rules:
    - All prices must be positive and within reasonable limits
    - SKU and product IDs must follow specific patterns
    - Inventory quantities cannot be negative
    - Required fields ensure minimum data quality
    - Category enumeration prevents data inconsistency

Version: 1.0
Last Updated: 2025-07-23
"""

PRODUCT_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/product-schema.json",
    "title": "Product Data",
    "description": "Schema for validating product data",
    "type": "object",
    "properties": {
        "product_id": {
            "type": "string",
            "description": "Unique identifier for the product",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "minLength": 1,
            "maxLength": 50,
        },
        "sku": {
            "type": "string",
            "description": "Stock Keeping Unit",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "minLength": 1,
            "maxLength": 100,
        },
        "name": {
            "type": "string",
            "description": "Product name",
            "minLength": 1,
            "maxLength": 200,
        },
        "description": {
            "type": "string",
            "description": "Product description",
            "maxLength": 2000,
        },
        "category": {
            "type": "string",
            "description": "Product category",
            "enum": [
                "electronics",
                "clothing",
                "food_beverage",
                "health_beauty",
                "home_garden",
                "sports_outdoors",
                "books_media",
                "automotive",
                "toys_games",
                "jewelry_accessories",
                "digital_services",
                "other",
            ],
        },
        "subcategory": {
            "type": "string",
            "description": "Product subcategory",
            "maxLength": 100,
        },
        "brand": {
            "type": "string",
            "description": "Product brand",
            "maxLength": 100,
        },
        "price": {
            "type": "object",
            "description": "Product pricing information",
            "properties": {
                "amount": {
                    "type": "number",
                    "description": "Product price",
                    "minimum": 0.00,
                    "maximum": 1000000.00,
                },
                "currency": {
                    "type": "string",
                    "description": "Currency code (ISO 4217)",
                    "pattern": "^[A-Z]{3}$",
                    "enum": ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"],
                },
                "discount_amount": {
                    "type": "number",
                    "description": "Discount amount",
                    "minimum": 0.00,
                },
                "discount_percentage": {
                    "type": "number",
                    "description": "Discount percentage",
                    "minimum": 0.0,
                    "maximum": 100.0,
                },
            },
            "required": ["amount", "currency"],
            "additionalProperties": False,
        },
        "inventory": {
            "type": "object",
            "description": "Inventory information",
            "properties": {
                "quantity": {
                    "type": "integer",
                    "description": "Available quantity",
                    "minimum": 0,
                },
                "reserved": {
                    "type": "integer",
                    "description": "Reserved quantity",
                    "minimum": 0,
                },
                "warehouse_location": {
                    "type": "string",
                    "description": "Warehouse location",
                    "maxLength": 100,
                },
            },
            "required": ["quantity"],
            "additionalProperties": False,
        },
        "dimensions": {
            "type": "object",
            "description": "Product dimensions",
            "properties": {
                "length": {"type": "number", "description": "Length in cm", "minimum": 0},
                "width": {"type": "number", "description": "Width in cm", "minimum": 0},
                "height": {"type": "number", "description": "Height in cm", "minimum": 0},
                "weight": {"type": "number", "description": "Weight in grams", "minimum": 0},
            },
            "additionalProperties": False,
        },
        "attributes": {
            "type": "object",
            "description": "Product attributes (color, size, etc.)",
            "properties": {
                "color": {"type": "string", "maxLength": 50},
                "size": {"type": "string", "maxLength": 20},
                "material": {"type": "string", "maxLength": 100},
                "style": {"type": "string", "maxLength": 50},
            },
            "additionalProperties": True,
        },
        "shop_id": {
            "type": "string",
            "description": "ID of the shop selling this product",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "maxLength": 50,
        },
        "status": {
            "type": "string",
            "description": "Product status",
            "enum": ["active", "inactive", "discontinued", "out_of_stock"],
        },
        "images": {
            "type": "array",
            "description": "Product image URLs",
            "items": {
                "type": "string",
                "format": "uri",
                "maxLength": 500,
            },
            "maxItems": 10,
        },
        "tags": {
            "type": "array",
            "description": "Product tags for search and categorisation",
            "items": {"type": "string", "maxLength": 50},
            "maxItems": 20,
        },
        "created_date": {
            "type": "string",
            "description": "Product creation date in ISO 8601 format",
            "format": "date-time",
        },
        "last_updated": {
            "type": "string",
            "description": "Last update timestamp in ISO 8601 format",
            "format": "date-time",
        },
        "metadata": {
            "type": "object",
            "description": "Additional metadata (optional)",
            "additionalProperties": True,
        },
    },
    "required": [
        "product_id",
        "sku",
        "name",
        "category",
        "price",
        "inventory",
        "shop_id",
        "status",
        "created_date",
    ],
    "additionalProperties": False,
}

# CSV headers for product data
PRODUCT_CSV_HEADERS = [
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
