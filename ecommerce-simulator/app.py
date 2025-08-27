"""
E-commerce Data Simulator Flask App
Generates and sends realistic e-commerce data to stream and batch services.
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List

import requests
from data_generators import create_csv_content, generate_products, generate_shops, generate_transactions
from flask import Flask, jsonify, render_template, request
from google.cloud import secretmanager, storage

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
STREAM_ENDPOINT = os.getenv("STREAM_ENDPOINT", "")
BATCH_BUCKET = os.getenv("BATCH_BUCKET", "")
SECRET_ID = os.getenv("SECRET_ID", "")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "")

# Cache for secret key
_cached_secret = None

# Global stats tracking
stats = {
    "transactions_sent": 0,
    "products_uploaded": 0,
    "shops_uploaded": 0,
    "batch_transactions_uploaded": 0,
    "last_activity": None,
}


@app.route("/")
def dashboard():
    """Main dashboard with controls."""
    return render_template("dashboard.html", stats=stats)


@app.route("/api/send-transactions", methods=["POST"])
def send_transactions():
    """Send streaming transactions to the stream service."""
    try:
        data = request.get_json()
        count = int(data.get("count", 10))
        delay_ms = int(data.get("delay", 100))  # Delay between transactions in ms

        logger.info(f"Sending {count} transactions with {delay_ms}ms delay")

        # Generate transactions for streaming (matches stream service schema)
        transactions = generate_transactions(count, transaction_type="stream")

        sent_count = 0
        failed_count = 0

        for transaction in transactions:
            try:
                # Create webhook payload with signature
                payload = json.dumps(transaction)
                secret_key = get_secret_key()
                signature = create_webhook_signature(payload, secret_key)

                headers = {"Content-Type": "application/json", "X-Signature": signature}

                # Send to stream service
                response = requests.post(STREAM_ENDPOINT, data=payload, headers=headers, timeout=10)

                if response.status_code == 200:
                    sent_count += 1
                else:
                    failed_count += 1
                    logger.warning(f"Transaction failed: {response.status_code} - {response.text}")

                # Add delay between transactions
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)

            except Exception as e:
                failed_count += 1
                logger.error(f"Error sending transaction: {e}")

        # Update stats
        stats["transactions_sent"] += sent_count
        stats["last_activity"] = datetime.now().isoformat()

        return jsonify({"success": True, "sent": sent_count, "failed": failed_count, "total_requested": count})

    except Exception as e:
        logger.error(f"Error in send_transactions: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/upload-products", methods=["POST"])
def upload_products():
    """Generate and upload products CSV to GCS bucket."""
    try:
        data = request.get_json()
        count = int(data.get("count", 50))

        logger.info(f"Generating and uploading {count} products")

        # Generate products data
        products = generate_products(count)
        csv_content = create_csv_content(products, "products")

        # Upload to GCS
        filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        success = upload_to_gcs(csv_content, filename)

        if success:
            stats["products_uploaded"] += count
            stats["last_activity"] = datetime.now().isoformat()

            return jsonify({"success": True, "filename": filename, "count": count})
        else:
            return jsonify({"success": False, "error": "Failed to upload to GCS"}), 500

    except Exception as e:
        logger.error(f"Error in upload_products: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/upload-shops", methods=["POST"])
def upload_shops():
    """Generate and upload shops CSV to GCS bucket."""
    try:
        data = request.get_json()
        count = int(data.get("count", 20))

        logger.info(f"Generating and uploading {count} shops")

        # Generate shops data
        shops = generate_shops(count)
        csv_content = create_csv_content(shops, "shops")

        # Upload to GCS
        filename = f"shops_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        success = upload_to_gcs(csv_content, filename)

        if success:
            stats["shops_uploaded"] += count
            stats["last_activity"] = datetime.now().isoformat()

            return jsonify({"success": True, "filename": filename, "count": count})
        else:
            return jsonify({"success": False, "error": "Failed to upload to GCS"}), 500

    except Exception as e:
        logger.error(f"Error in upload_shops: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/upload-batch-transactions", methods=["POST"])
def upload_batch_transactions():
    """Generate and upload batch transactions CSV to GCS bucket."""
    try:
        data = request.get_json()
        count = int(data.get("count", 100))

        logger.info(f"Generating and uploading {count} batch transactions")

        # Generate transactions data for batch processing (detailed ecommerce schema)
        transactions = generate_transactions(count, transaction_type="batch")
        csv_content = create_csv_content(transactions, "transactions")

        # Upload to GCS
        filename = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        success = upload_to_gcs(csv_content, filename)

        if success:
            stats["batch_transactions_uploaded"] += count
            stats["last_activity"] = datetime.now().isoformat()

            return jsonify({"success": True, "filename": filename, "count": count})
        else:
            return jsonify({"success": False, "error": "Failed to upload to GCS"}), 500

    except Exception as e:
        logger.error(f"Error in upload_batch_transactions: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/stats")
def get_stats():
    """Get current statistics."""
    return jsonify(stats)


@app.route("/api/reset-stats", methods=["POST"])
def reset_stats():
    """Reset all statistics."""
    global stats
    stats = {
        "transactions_sent": 0,
        "products_uploaded": 0,
        "shops_uploaded": 0,
        "batch_transactions_uploaded": 0,
        "last_activity": None,
    }
    return jsonify({"success": True, "message": "Stats reset"})


def get_secret_key() -> str:
    """Retrieve secret key from Google Secret Manager with caching."""
    global _cached_secret

    if _cached_secret is not None:
        return _cached_secret

    try:
        # Initialize Secret Manager client
        client = secretmanager.SecretManagerServiceClient()

        # Build the resource name of the secret version
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"

        # Access the secret version
        response = client.access_secret_version(request={"name": name})

        # Get the secret value
        secret_value = response.payload.data.decode("UTF-8")

        # Cache the secret
        _cached_secret = secret_value
        logger.info(f"Successfully retrieved secret from Secret Manager: {SECRET_ID}")

        return secret_value

    except Exception as e:
        logger.error(f"Failed to retrieve secret {SECRET_ID} from Secret Manager: {e}")
        # Fallback to environment variable if Secret Manager fails
        fallback_secret = os.getenv("SECRET_KEY", "default-secret-key")
        logger.warning(f"Using fallback secret from environment variable")
        _cached_secret = fallback_secret
        return fallback_secret


def create_webhook_signature(payload: str, secret: str) -> str:
    """Create HMAC signature for webhook payload."""
    import hashlib
    import hmac

    # Convert secret from hex if needed
    try:
        secret_bytes = bytes.fromhex(secret)
    except ValueError:
        secret_bytes = secret.encode()

    signature = hmac.new(secret_bytes, payload.encode(), hashlib.sha512).hexdigest()

    return signature


def upload_to_gcs(content: str, filename: str) -> bool:
    """Upload content to GCS bucket."""
    try:
        # Initialize GCS client
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BATCH_BUCKET)
        blob = bucket.blob(filename)

        # Upload content
        blob.upload_from_string(content, content_type="text/csv")

        logger.info(f"Successfully uploaded {filename} to GCS")
        return True

    except Exception as e:
        logger.error(f"Failed to upload {filename} to GCS: {e}")
        return False


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
