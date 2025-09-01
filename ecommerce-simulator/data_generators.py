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


def generate_products(count: int, existing_shops: List[Dict] = None) -> List[Dict]:
    """Generate realistic product data."""
    products = []

    for i in range(count):
        category = random.choice(PRODUCT_CATEGORIES)
        base_name = random.choice(PRODUCT_NAMES[category])
        brand = generate_brand_name()

        # Use integer cents to avoid floating point precision issues
        price_cents = random.randint(599, 99999)  # 5.99 to 999.99 in cents
        cost_cents = random.randint(299, 59999)  # 2.99 to 599.99 in cents

        # Map categories to match batch service schema
        category_mapping = {
            "Electronics": "electronics",
            "Clothing": "clothing",
            "Books": "books_media",
            "Home & Garden": "home_garden",
            "Sports": "sports_outdoors",
            "Beauty": "health_beauty",
            "Toys": "toys_games",
            "Automotive": "automotive",
            "Health": "health_beauty",
            "Food & Beverage": "food_beverage",
        }

        # Create product matching batch service schema structure
        product = {
            "product_id": f"PROD_{uuid.uuid4().hex[:8].upper()}",
            "sku": f"SKU_{uuid.uuid4().hex[:12].upper()}",
            "name": f"{brand} {base_name}",
            "description": f"High-quality {base_name.lower()} from {brand}",
            "category": category_mapping.get(category, "other"),
            "subcategory": base_name,
            "brand": brand,
            "price_amount": price_cents / 100.0,
            "price_currency": "GBP",
            "price_discount_amount": 0.0,
            "price_discount_percentage": 0.0,
            "inventory_quantity": random.randint(0, 500),
            "inventory_reserved": 0,
            "inventory_warehouse_location": f"Warehouse {random.choice(['A', 'B', 'C'])}",
            "dimensions_length": random.randint(5, 50),
            "dimensions_width": random.randint(5, 50),
            "dimensions_height": random.randint(5, 50),
            "dimensions_weight": random.randint(100, 5000),  # grams
            "attributes_color": random.choice(["Black", "White", "Blue", "Red", "Green", ""]),
            "attributes_size": random.choice(["S", "M", "L", "XL", "One Size", ""]),
            "attributes_material": random.choice(["Cotton", "Plastic", "Metal", "Wood", ""]),
            "attributes_style": random.choice(["Modern", "Classic", "Vintage", ""]),
            "shop_id": (
                random.choice(existing_shops)["shop_id"]
                if existing_shops and len(existing_shops) > 0
                else f"SHOP_{uuid.uuid4().hex[:8].upper()}"
            ),
            "status": random.choice(["active", "active", "active", "inactive"]),  # 75% active
            "images": "",
            "tags": "",
            "created_date": generate_recent_timestamp(),
            "last_updated": generate_recent_timestamp(),
        }
        products.append(product)

    return products


def generate_shops(count: int) -> List[Dict]:
    """Generate realistic shop data."""
    shops = []

    for i in range(count):
        shop_name = generate_shop_name()
        city = random.choice(CITIES)

        # Map shop types to categories
        category_mapping = {
            "Online Store": "electronics",
            "Physical Store": "clothing",
            "Marketplace": "food_beverage",
            "Boutique": "clothing",
            "Department Store": "other",
        }

        shop_type = random.choice(SHOP_TYPES)
        owner_name = generate_person_name()
        # Clean owner name for email
        clean_owner = owner_name.lower().replace(" ", ".").replace("'", "")
        # Clean shop name for domain
        clean_shop = shop_name.lower().replace(" ", "").replace("&", "and").replace("'", "").replace("-", "")
        owner_email = f"{clean_owner}@{clean_shop}.co.uk"
        street_address = f"{random.randint(1, 999)} {random.choice(['High Street', 'Market Square', 'Victoria Road', 'Church Lane', 'King Street'])}"

        # Create shop matching batch service schema structure
        shop = {
            "shop_id": f"SHOP_{uuid.uuid4().hex[:8].upper()}",
            "name": shop_name,
            "description": f"A {shop_type.lower()} specializing in quality products",
            "category": category_mapping.get(shop_type, "other"),
            "status": random.choice(["active", "active", "active", "inactive"]),  # 75% active
            "owner_name": owner_name,
            "owner_email": owner_email,
            "owner_phone": generate_uk_phone(),
            "address_street": street_address,
            "address_city": city,
            "address_state": random.choice(["England", "Scotland", "Wales", "Northern Ireland"]),
            "address_postal_code": generate_uk_postcode(),
            "address_country": "GB",
            "contact_phone": generate_uk_phone(),
            "contact_email": f"info@{clean_shop}.co.uk",
            "contact_website": f"https://www.{clean_shop}.co.uk",
            "business_hours_monday": "09:00-18:00",
            "business_hours_tuesday": "09:00-18:00",
            "business_hours_wednesday": "09:00-18:00",
            "business_hours_thursday": "09:00-18:00",
            "business_hours_friday": "09:00-18:00",
            "business_hours_saturday": "09:00-17:00",
            "business_hours_sunday": "10:00-16:00",
            "registration_date": generate_recent_timestamp(days_back=365),
            "last_updated": generate_recent_timestamp(),
        }
        shops.append(shop)

    return shops


def generate_transactions(
    count: int,
    transaction_type: str = "stream",
    existing_products: List[Dict] = None,
    existing_shops: List[Dict] = None,
    existing_customers: List[str] = None,
) -> List[Dict]:
    """Generate realistic transaction data.

    Args:
        count: Number of transactions to generate
        transaction_type: Type of transactions to generate ("stream" for streaming API, "batch" for CSV)
        existing_products: List of existing products from BigQuery
        existing_shops: List of existing shops from BigQuery
        existing_customers: List of existing customer IDs from BigQuery
    """
    transactions = []

    # Use existing customer IDs if available, otherwise generate new ones
    if existing_customers and len(existing_customers) > 0:
        customer_ids = existing_customers
        # Add a few new customers for variety
        customer_ids.extend([f"cust_{uuid.uuid4().hex[:8]}" for _ in range(min(count // 5, 10))])
    else:
        # Generate some consistent customer IDs for realistic transactions
        customer_ids = [f"cust_{uuid.uuid4().hex[:8]}" for _ in range(min(count // 3, 50))]

    for i in range(count):
        if transaction_type == "stream":
            # Generate transaction for stream service with rich data matching schema
            # Use integer cents then divide to avoid floating point precision issues
            amount_cents = random.randint(1000, 500000)  # 10.00 to 5000.00 in cents
            amount = amount_cents / 100.0

            # Enhanced payment method objects matching schema requirements
            payment_methods = [
                {
                    "type": "credit_card",
                    "last_four": str(random.randint(1000, 9999)),
                    "provider": random.choice(["Visa", "Mastercard", "Amex", "Discover"]),
                },
                {
                    "type": "debit_card",
                    "last_four": str(random.randint(1000, 9999)),
                    "provider": random.choice(["Visa", "Mastercard"]),
                },
                {"type": "digital_wallet", "provider": random.choice(["Apple Pay", "Google Pay", "PayPal", "Stripe"])},
                {"type": "bank_transfer", "provider": "Bank Transfer"},
                {"type": "cash"},
            ]

            # Location data matching schema (ISO country codes)
            locations = [
                {"country": "US", "city": "New York", "postal_code": "10001"},
                {"country": "GB", "city": "London", "postal_code": "SW1A 1AA"},
                {"country": "CA", "city": "Toronto", "postal_code": "M5V 3A8"},
                {"country": "DE", "city": "Berlin", "postal_code": "10115"},
                {"country": "AU", "city": "Sydney", "postal_code": "2000"},
                {"country": "IN", "city": "Mumbai", "postal_code": "400001"},
                {"country": "JP", "city": "Tokyo", "postal_code": "100-0001"},
            ]

            # Use real merchant IDs from shops if available
            selected_shop = None
            if existing_shops and len(existing_shops) > 0:
                selected_shop = random.choice(existing_shops)
                merchant_id = selected_shop["shop_id"]
                shop_city = selected_shop["city"]
                # Use shop location if available
                shop_location = {"country": "GB", "city": shop_city, "postal_code": "SW1A 1AA"}
            else:
                merchant_id = f"merch_{uuid.uuid4().hex[:8]}"
                shop_location = random.choice(locations)

            # Create richer description if we have product/shop data
            selected_product = None
            if existing_products and len(existing_products) > 0:
                selected_product = random.choice(existing_products)
                shop_name = selected_shop["name"] if selected_shop else "Online Store"
                description = f"Purchase of {selected_product['name']} from {shop_name}"
                amount = max(amount, selected_product["price"])  # Use realistic product price
            else:
                description = f"Transaction {uuid.uuid4().hex[:5]} - {random.choice(['Online purchase', 'Store transaction', 'Mobile payment', 'Service payment', 'Subscription renewal'])}"

            # Generate transaction matching exact schema structure
            transaction = {
                "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
                "customer_id": random.choice(customer_ids),
                "amount": amount,
                "currency": random.choice(["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR"]),
                "transaction_type": random.choice(["purchase", "refund", "transfer", "deposit", "withdrawal"]),
                "timestamp": generate_recent_timestamp(hours_back=168),  # Last week
                "payment_method": random.choice(payment_methods),
                # Optional fields that make it richer
                "merchant_id": merchant_id,
                "description": description,
                "location": shop_location,
                "metadata": {
                    "batch_index": i,
                    "data_type": "transaction",
                    "message_id": str(uuid.uuid4()),
                    "source": random.choice(["web", "mobile", "pos", "api"]),
                    "category": random.choice(["retail", "food", "entertainment", "transport", "utilities"]),
                    "session_id": f"sess_{uuid.uuid4().hex[:16]}",
                    "product_info": (
                        {
                            "product_id": selected_product["product_id"] if existing_products else None,
                            "product_name": selected_product["name"] if existing_products else None,
                            "category": selected_product["category"] if existing_products else None,
                        }
                        if existing_products
                        else {}
                    ),
                },
            }
        else:
            # Generate transaction for batch processing (detailed ecommerce data)
            quantity = random.randint(1, 5)
            # Use integer cents to avoid floating point precision issues
            unit_price_cents = random.randint(599, 29999)  # 5.99 to 299.99 in cents
            unit_price = unit_price_cents / 100.0
            subtotal_cents = quantity * unit_price_cents
            subtotal = subtotal_cents / 100.0
            tax_cents = int(subtotal_cents * 0.20)  # UK VAT, keep as integer
            tax = tax_cents / 100.0
            total = (subtotal_cents + tax_cents) / 100.0

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
    """Generate a realistic UK phone number that matches schema pattern ^\\+?[1-9]\\d{1,14}$."""
    # Generate E.164 format: +44 followed by 9-10 digits
    area_codes = [20, 121, 131, 161, 113, 117, 118, 151, 191, 1273]  # Major UK area codes
    area_code = random.choice(area_codes)
    local_number = random.randint(1000000, 9999999)  # 7 digits
    return f"+44{area_code}{local_number}"


def generate_ip_address() -> str:
    """Generate a realistic IP address."""
    return f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"


def generate_recent_timestamp(days_back: int = 7, hours_back: int = None) -> str:
    """Generate a recent timestamp in ISO 8601 format with timezone."""
    if hours_back:
        max_delta = timedelta(hours=hours_back)
    else:
        max_delta = timedelta(days=days_back)

    random_delta = timedelta(seconds=random.randint(0, int(max_delta.total_seconds())))
    timestamp = datetime.now() - random_delta

    # Ensure timezone info is included (JSON Schema date-time format requires it)
    return timestamp.isoformat() + "Z"
