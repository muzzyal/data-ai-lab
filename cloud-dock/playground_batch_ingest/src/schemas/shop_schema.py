"""
JSON Schema definition for shop data validation.
This schema defines the structure and validation rules for shop/merchant data.
"""

SHOP_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.com/shop-schema.json",
    "title": "Shop Data",
    "description": "Schema for validating shop/merchant data",
    "type": "object",
    "properties": {
        "shop_id": {
            "type": "string",
            "description": "Unique identifier for the shop",
            "pattern": "^[a-zA-Z0-9_-]+$",
            "minLength": 1,
            "maxLength": 50,
        },
        "name": {
            "type": "string",
            "description": "Shop name",
            "minLength": 1,
            "maxLength": 200,
        },
        "description": {
            "type": "string",
            "description": "Shop description",
            "maxLength": 1000,
        },
        "category": {
            "type": "string",
            "description": "Shop category",
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
                "services",
                "other",
            ],
        },
        "status": {
            "type": "string",
            "description": "Shop status",
            "enum": ["active", "inactive", "suspended", "pending"],
        },
        "owner": {
            "type": "object",
            "description": "Shop owner information",
            "properties": {
                "name": {"type": "string", "description": "Owner name", "maxLength": 100},
                "email": {
                    "type": "string",
                    "description": "Owner email",
                    "format": "email",
                    "maxLength": 100,
                },
                "phone": {
                    "type": "string",
                    "description": "Owner phone number",
                    "pattern": "^\\+?[1-9]\\d{1,14}$",
                },
            },
            "required": ["name", "email"],
            "additionalProperties": False,
        },
        "address": {
            "type": "object",
            "description": "Shop address",
            "properties": {
                "street": {"type": "string", "description": "Street address", "maxLength": 200},
                "city": {"type": "string", "description": "City", "maxLength": 100},
                "state": {"type": "string", "description": "State/Province", "maxLength": 100},
                "postal_code": {"type": "string", "description": "Postal/ZIP code", "maxLength": 20},
                "country": {
                    "type": "string",
                    "description": "Country code (ISO 3166-1 alpha-2)",
                    "pattern": "^[A-Z]{2}$",
                },
            },
            "required": ["street", "city", "country"],
            "additionalProperties": False,
        },
        "contact": {
            "type": "object",
            "description": "Shop contact information",
            "properties": {
                "phone": {
                    "type": "string",
                    "description": "Shop phone number",
                    "pattern": "^\\+?[1-9]\\d{1,14}$",
                },
                "email": {
                    "type": "string",
                    "description": "Shop email",
                    "format": "email",
                    "maxLength": 100,
                },
                "website": {
                    "type": "string",
                    "description": "Shop website URL",
                    "format": "uri",
                    "maxLength": 200,
                },
            },
            "additionalProperties": False,
        },
        "business_hours": {
            "type": "object",
            "description": "Business operating hours",
            "properties": {
                "monday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "tuesday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "wednesday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "thursday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "friday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "saturday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
                "sunday": {"type": "string", "pattern": "^([0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}|closed)$"},
            },
            "additionalProperties": False,
        },
        "registration_date": {
            "type": "string",
            "description": "Shop registration date in ISO 8601 format",
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
        "shop_id",
        "name",
        "category",
        "status",
        "owner",
        "address",
        "registration_date",
    ],
    "additionalProperties": False,
}

# CSV headers for shop data
SHOP_CSV_HEADERS = [
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
