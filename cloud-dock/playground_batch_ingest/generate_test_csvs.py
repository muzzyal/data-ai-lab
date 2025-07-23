#!/usr/bin/env python3
"""
Simple CSV generator for batch ingestion testing.
Generates CSV files based on the schemas in src/schemas directory.
"""

import csv
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any


def generate_random_string(length: int = 10) -> str:
    """Generate random alphanumeric string."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_id(prefix: str = "") -> str:
    """Generate random ID with optional prefix."""
    return f"{prefix}{generate_random_string(8)}"


def generate_random_date(start_days_ago: int = 365, end_days_ago: int = 0) -> str:
    """Generate random date within specified range."""
    start_date = datetime.now() - timedelta(days=start_days_ago)
    end_date = datetime.now() - timedelta(days=end_days_ago)

    time_between = end_date - start_date
    random_days = random.randrange(time_between.days)
    random_date = start_date + timedelta(days=random_days)

    return random_date.isoformat()


def generate_transactions(count: int = 100) -> List[Dict[str, Any]]:
    """Generate transaction data based on transaction schema."""
    currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"]
    transaction_types = ["purchase", "refund", "transfer", "deposit", "withdrawal"]
    payment_types = ["credit_card", "debit_card", "bank_transfer", "digital_wallet", "cash"]
    payment_providers = ["Visa", "Mastercard", "PayPal", "Stripe", "Square", "Apple Pay"]
    countries = ["US", "CA", "GB", "FR", "DE", "JP", "AU", "IN", "CN"]
    cities = ["New York", "London", "Paris", "Tokyo", "Sydney", "Toronto", "Berlin", "Mumbai", "Shanghai"]

    transactions = []
    for _ in range(count):
        transaction = {
            "transaction_id": generate_random_id("txn_"),
            "customer_id": generate_random_id("cust_"),
            "amount": round(random.uniform(0.01, 5000.00), 2),
            "currency": random.choice(currencies),
            "transaction_type": random.choice(transaction_types),
            "timestamp": generate_random_date(30, 0),
            "merchant_id": generate_random_id("merch_") if random.random() > 0.2 else "",
            "description": f"Transaction {generate_random_string(5)}",
            "payment_method_type": random.choice(payment_types),
            "payment_method_last_four": f"{random.randint(1000, 9999)}",
            "payment_method_provider": random.choice(payment_providers),
            "location_country": random.choice(countries),
            "location_city": random.choice(cities),
            "location_postal_code": f"{random.randint(10000, 99999)}",
        }
        transactions.append(transaction)

    return transactions


def generate_products(count: int = 50) -> List[Dict[str, Any]]:
    """Generate product data based on product schema."""
    categories = [
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
    ]
    statuses = ["active", "inactive", "discontinued", "out_of_stock"]
    currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"]

    product_names = [
        "Wireless Headphones",
        "Smartphone",
        "Laptop",
        "Coffee Maker",
        "Running Shoes",
        "Backpack",
        "Desk Chair",
        "Monitor",
        "Keyboard",
        "Mouse",
        "Tablet",
        "Smartwatch",
    ]
    brands = [
        "Apple",
        "Samsung",
        "Sony",
        "Microsoft",
        "Google",
        "Amazon",
        "Nike",
        "Adidas",
        "Dell",
        "HP",
        "Lenovo",
        "Canon",
        "Nikon",
        "LG",
        "Panasonic",
        "Bose",
        "JBL",
    ]

    products = []
    for _ in range(count):
        product = {
            "product_id": generate_random_id("prod_"),
            "sku": generate_random_id("SKU_"),
            "name": random.choice(product_names),
            "description": f"High-quality {random.choice(product_names).lower()} with excellent features",
            "category": random.choice(categories),
            "subcategory": f"{random.choice(['premium', 'standard', 'basic'])} {random.choice(categories)}",
            "brand": random.choice(brands),
            "price_amount": round(random.uniform(10.00, 2000.00), 2),
            "price_currency": random.choice(currencies),
            "price_discount_amount": round(random.uniform(0, 100.00), 2) if random.random() > 0.7 else "",
            "price_discount_percentage": round(random.uniform(5, 50), 1) if random.random() > 0.8 else "",
            "inventory_quantity": random.randint(0, 1000),
            "inventory_reserved": random.randint(0, 50),
            "inventory_warehouse_location": f"Warehouse {random.choice(['A', 'B', 'C', 'D'])}",
            "dimensions_length": round(random.uniform(5, 50), 2),
            "dimensions_width": round(random.uniform(5, 50), 2),
            "dimensions_height": round(random.uniform(5, 50), 2),
            "dimensions_weight": round(random.uniform(100, 5000), 2),
            "attributes_color": random.choice(["Red", "Blue", "Green", "Black", "White", "Silver"]),
            "attributes_size": random.choice(["XS", "S", "M", "L", "XL", "XXL", "One Size"]),
            "attributes_material": random.choice(["Cotton", "Plastic", "Metal", "Wood", "Glass", "Leather"]),
            "attributes_style": random.choice(["Modern", "Classic", "Vintage", "Minimalist", "Elegant"]),
            "shop_id": generate_random_id("shop_"),
            "status": random.choice(statuses),
            "images": f"https://example.com/images/{generate_random_string(8)}.jpg",
            "tags": f"tag1,tag2,tag3",
            "created_date": generate_random_date(180, 30),
            "last_updated": generate_random_date(30, 0),
        }
        products.append(product)

    return products


def generate_shops(count: int = 20) -> List[Dict[str, Any]]:
    """Generate shop data based on shop schema."""
    categories = [
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
    ]
    statuses = ["active", "inactive", "suspended", "pending"]
    countries = ["US", "CA", "GB", "FR", "DE", "JP", "AU", "IN", "CN"]

    shop_names = [
        "Tech World",
        "Fashion Hub",
        "Sports Central",
        "Book Haven",
        "Home Essentials",
        "Gadget Store",
        "Style Shop",
        "Outdoor Adventures",
        "Digital Dreams",
        "Comfort Zone",
    ]

    shops = []
    for _ in range(count):
        shop = {
            "shop_id": generate_random_id("shop_"),
            "name": random.choice(shop_names),
            "description": f"Premium {random.choice(categories)} store with excellent service",
            "category": random.choice(categories),
            "status": random.choice(statuses),
            "owner_name": f"{random.choice(['John', 'Jane', 'Mike', 'Sarah', 'David'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones'])}",
            "owner_email": f"{generate_random_string(6)}@example.com",
            "owner_phone": f"+1{random.randint(2000000000, 9999999999)}",
            "address_street": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} St",
            "address_city": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
            "address_state": random.choice(["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]),
            "address_postal_code": f"{random.randint(10000, 99999)}",
            "address_country": random.choice(countries),
            "contact_phone": f"+1{random.randint(2000000000, 9999999999)}",
            "contact_email": f"info@{generate_random_string(6)}.com",
            "contact_website": f"https://www.{generate_random_string(8)}.com",
            "business_hours_monday": "09:00-17:00",
            "business_hours_tuesday": "09:00-17:00",
            "business_hours_wednesday": "09:00-17:00",
            "business_hours_thursday": "09:00-17:00",
            "business_hours_friday": "09:00-17:00",
            "business_hours_saturday": "10:00-16:00",
            "business_hours_sunday": "closed",
            "registration_date": generate_random_date(365, 30),
            "last_updated": generate_random_date(30, 0),
        }
        shops.append(shop)

    return shops


def save_to_csv(data: List[Dict[str, Any]], filename: str):
    """Save data to CSV file."""
    if not data:
        print(f"No data to save for {filename}")
        return

    # Create output directory
    output_dir = Path("test_csvs")
    output_dir.mkdir(exist_ok=True)

    filepath = output_dir / filename

    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"✓ Generated {filename} with {len(data)} records")


def main():
    """Generate all test CSV files."""
    print("Generating test CSV files...")
    print("=" * 50)

    # Generate transactions
    print("Generating transactions...")
    transactions = generate_transactions(100)
    save_to_csv(transactions, "transactions.csv")

    # Generate products
    print("Generating products...")
    products = generate_products(50)
    save_to_csv(products, "products.csv")

    # Generate shops
    print("Generating shops...")
    shops = generate_shops(20)
    save_to_csv(shops, "shops.csv")

    print("\n" + "=" * 50)
    print("All CSV files generated in 'test_csvs' directory!")
    print("You can now manually upload these files to your GCS bucket for testing.")
    print("\nFiles created:")
    print("- test_csvs/transactions.csv (100 records)")
    print("- test_csvs/products.csv (50 records)")
    print("- test_csvs/shops.csv (20 records)")


if __name__ == "__main__":
    main()
