"""
Data generators for realistic e-commerce data.
Generates products, shops, and transactions with realistic attributes.
"""

import csv
import io
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Dict, List

# Sample data for realistic generation
PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home & Garden",
    "Sports",
    "Beauty",
    "Toys",
    "Automotive",
    "Health",
    "Food & Beverage",
]

PRODUCT_NAMES = {
    "Electronics": ["Smartphone", "Laptop", "Headphones", "Camera", "Tablet", "Speaker", "Monitor", "Keyboard"],
    "Clothing": ["T-Shirt", "Jeans", "Dress", "Jacket", "Shoes", "Hat", "Sweater", "Socks"],
    "Books": ["Novel", "Textbook", "Biography", "Cookbook", "Manual", "Guide", "Dictionary", "Magazine"],
    "Home & Garden": ["Chair", "Table", "Lamp", "Plant", "Rug", "Curtains", "Vase", "Mirror"],
    "Sports": ["Running Shoes", "Football", "Basketball", "Tennis Racket", "Yoga Mat", "Weights", "Bike", "Helmet"],
    "Beauty": ["Lipstick", "Foundation", "Perfume", "Shampoo", "Moisturizer", "Nail Polish", "Mascara", "Cleanser"],
    "Toys": ["Action Figure", "Doll", "Building Blocks", "Board Game", "Puzzle", "Remote Car", "Teddy Bear", "Ball"],
    "Automotive": ["Tires", "Oil", "Battery", "Brake Pads", "Air Filter", "Spark Plugs", "Wiper Blades", "Floor Mats"],
    "Health": [
        "Vitamins",
        "Protein Powder",
        "Thermometer",
        "Blood Pressure Monitor",
        "First Aid Kit",
        "Hand Sanitizer",
    ],
    "Food & Beverage": ["Coffee", "Tea", "Chocolate", "Nuts", "Cookies", "Juice", "Water", "Energy Bar"],
}

SHOP_TYPES = ["Online Store", "Physical Store", "Marketplace", "Boutique", "Department Store"]

CITIES = [
    "London",
    "Manchester",
    "Birmingham",
    "Leeds",
    "Glasgow",
    "Sheffield",
    "Bristol",
    "Edinburgh",
    "Liverpool",
    "Cardiff",
    "Nottingham",
    "Newcastle",
    "Belfast",
    "Leicester",
    "Brighton",
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]


def generate_products(count: int) -> List[Dict]:
    """Generate realistic product data."""
    products = []

    for i in range(count):
        category = random.choice(PRODUCT_CATEGORIES)
        base_name = random.choice(PRODUCT_NAMES[category])
        brand = generate_brand_name()

        product = {
            "product_id": f"PROD_{uuid.uuid4().hex[:8].upper()}",
            "name": f"{brand} {base_name}",
            "category": category,
            "brand": brand,
            "price": round(random.uniform(5.99, 999.99), 2),
            "cost": round(random.uniform(2.99, 599.99), 2),
            "stock_quantity": random.randint(0, 500),
            "description": f"High-quality {base_name.lower()} from {brand}",
            "weight": round(random.uniform(0.1, 10.0), 2),
            "dimensions": f"{random.randint(5,50)}x{random.randint(5,50)}x{random.randint(5,50)}cm",
            "supplier": f"{random.choice(['Global', 'Premium', 'Direct', 'Quality'])} Suppliers Ltd",
            "created_at": generate_recent_timestamp(),
            "is_active": random.choice([True, True, True, False]),  # 75% active
        }
        products.append(product)

    return products


def generate_shops(count: int) -> List[Dict]:
    """Generate realistic shop data."""
    shops = []

    for i in range(count):
        shop_name = generate_shop_name()
        city = random.choice(CITIES)

        shop = {
            "shop_id": f"SHOP_{uuid.uuid4().hex[:8].upper()}",
            "name": shop_name,
            "type": random.choice(SHOP_TYPES),
            "city": city,
            "address": f"{random.randint(1, 999)} {random.choice(['High Street', 'Market Square', 'Victoria Road', 'Church Lane', 'King Street'])}",
            "postcode": generate_uk_postcode(),
            "phone": generate_uk_phone(),
            "email": f"info@{shop_name.lower().replace(' ', '').replace('&', 'and')}.co.uk",
            "manager": generate_person_name(),
            "opening_hours": "Mon-Sat 9:00-18:00, Sun 10:00-16:00",
            "commission_rate": round(random.uniform(0.02, 0.15), 3),
            "rating": round(random.uniform(3.0, 5.0), 1),
            "created_at": generate_recent_timestamp(days_back=365),
            "is_active": random.choice([True, True, True, False]),
        }
        shops.append(shop)

    return shops


def generate_transactions(count: int, transaction_type: str = "stream") -> List[Dict]:
    """Generate realistic transaction data.

    Args:
        count: Number of transactions to generate
        transaction_type: Type of transactions to generate ("stream" for streaming API, "batch" for CSV)
    """
    transactions = []

    # Generate some consistent customer IDs for realistic transactions
    customer_ids = [f"cust_{uuid.uuid4().hex[:8]}" for _ in range(min(count // 3, 50))]

    for i in range(count):
        if transaction_type == "stream":
            # Generate transaction for stream service (matches exact schema)
            amount = round(random.uniform(0.01, 1000.0), 2)

            # Payment method object as expected by schema (only 'type' is required)
            payment_methods = [
                {"type": "credit_card"},
                {"type": "debit_card"},
                {"type": "digital_wallet"},
                {"type": "bank_transfer"},
                {"type": "cash"},
            ]

            transaction = {
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "customer_id": random.choice(customer_ids),
                "amount": amount,
                "currency": random.choice(["USD", "EUR", "GBP", "CAD", "AUD"]),  # Valid currencies
                "transaction_type": random.choice(["purchase", "refund", "deposit"]),
                "timestamp": generate_recent_timestamp(hours_back=24),
                "payment_method": random.choice(payment_methods),
                # Optional fields
                "merchant_id": f"merch_{uuid.uuid4().hex[:8]}",
                "description": random.choice(
                    [
                        "Online purchase",
                        "Store transaction",
                        "Mobile payment",
                        "Subscription payment",
                        "Service payment",
                    ]
                ),
                "metadata": {
                    "channel": random.choice(["web", "mobile", "pos", "api"]),
                    "category": random.choice(["retail", "food", "entertainment", "transport", "utilities"]),
                },
            }
        else:
            # Generate transaction for batch processing (detailed ecommerce data)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(5.99, 299.99), 2)
            subtotal = round(quantity * unit_price, 2)
            tax = round(subtotal * 0.20, 2)  # UK VAT
            total = subtotal + tax

            transaction = {
                "transaction_id": f"TXN_{uuid.uuid4().hex[:12].upper()}",
                "customer_id": random.choice(customer_ids),
                "product_id": f"PROD_{uuid.uuid4().hex[:8].upper()}",
                "shop_id": f"SHOP_{uuid.uuid4().hex[:8].upper()}",
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal,
                "tax": tax,
                "total": total,
                "currency": "GBP",
                "payment_method": random.choice(PAYMENT_METHODS),
                "status": random.choice(["completed", "completed", "completed", "pending", "cancelled"]),
                "timestamp": generate_recent_timestamp(hours_back=24),
                "session_id": f"SESS_{uuid.uuid4().hex[:16]}",
                "user_agent": random.choice(
                    [
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15",
                    ]
                ),
                "ip_address": generate_ip_address(),
                "discount_applied": round(random.uniform(0, subtotal * 0.2), 2) if random.random() < 0.3 else 0,
            }

        transactions.append(transaction)

    return transactions


def create_csv_content(data: List[Dict], data_type: str) -> str:
    """Convert data to CSV format."""
    if not data:
        return ""

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())

    writer.writeheader()
    for row in data:
        writer.writerow(row)

    return output.getvalue()


def generate_brand_name() -> str:
    """Generate a realistic brand name."""
    prefixes = ["Tech", "Pro", "Ultra", "Smart", "Digital", "Premium", "Elite", "Global", "Pure", "Modern"]
    suffixes = ["Corp", "Ltd", "Inc", "Solutions", "Systems", "Products", "Brands", "Industries"]

    if random.random() < 0.7:
        return f"{random.choice(prefixes)}{random.choice(['', 'X', 'Pro', 'Max', 'Plus'])}"
    else:
        return f"{random.choice(prefixes)} {random.choice(suffixes)}"


def generate_shop_name() -> str:
    """Generate a realistic shop name."""
    types = ["Electronics", "Fashion", "Books", "Home", "Sports", "Beauty", "Toys", "Auto", "Health", "Food"]
    styles = ["Emporium", "Boutique", "Store", "Shop", "Market", "Outlet", "Corner", "Hub", "World", "Plus"]

    if random.random() < 0.6:
        return f"{random.choice(types)} {random.choice(styles)}"
    else:
        first_names = ["John", "Sarah", "Mike", "Emma", "David", "Lisa", "James", "Anna"]
        return f"{random.choice(first_names)}'s {random.choice(types)}"


def generate_person_name() -> str:
    """Generate a realistic person name."""
    first_names = ["James", "Sarah", "Michael", "Emma", "David", "Lisa", "John", "Anna", "Robert", "Helen"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Moore"]

    return f"{random.choice(first_names)} {random.choice(last_names)}"


def generate_uk_postcode() -> str:
    """Generate a realistic UK postcode."""
    area = random.choice(["SW", "NW", "E", "W", "N", "SE", "EC", "WC", "M", "B"])
    return f"{area}{random.randint(1, 20)} {random.randint(1, 9)}{random.choice(string.ascii_uppercase)}{random.choice(string.ascii_uppercase)}"


def generate_uk_phone() -> str:
    """Generate a realistic UK phone number."""
    return f"0{random.choice([1, 2, 7])}{random.randint(10000000, 99999999)}"


def generate_ip_address() -> str:
    """Generate a realistic IP address."""
    return f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"


def generate_recent_timestamp(days_back: int = 7, hours_back: int = None) -> str:
    """Generate a recent timestamp."""
    if hours_back:
        max_delta = timedelta(hours=hours_back)
    else:
        max_delta = timedelta(days=days_back)

    random_delta = timedelta(seconds=random.randint(0, int(max_delta.total_seconds())))
    timestamp = datetime.now() - random_delta

    return timestamp.isoformat()
