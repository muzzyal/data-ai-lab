import logging
from flask import Blueprint, request, jsonify, render_template
from src.services.validator import TransactionValidator
from src.services.publisher import PubSubPublisher, PublishError
from src.services.dlq import DeadLetterQueue
from src.config.loader import retrieve_environment_variables
import json
import os

logger = logging.getLogger(__name__)

# Create blueprint for transaction routes
transaction_bp = Blueprint("transactions", __name__)

project_id, topic_name, dlq_topic_name, secret_id = retrieve_environment_variables()

# Initialize services
validator = TransactionValidator()
publisher = PubSubPublisher(project_id, topic_name)
dlq = DeadLetterQueue(project_id, dlq_topic_name)


@transaction_bp.route("/api/transactions", methods=["POST"])
def ingest_transaction():
    """
    Main endpoint for ingesting transaction data.
    Validates the data and publishes to Pub/Sub or sends to DLQ on failure.
    """
    try:
        # Check content type
        if not request.is_json:
            return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

        # Get JSON data
        try:
            transaction_data = request.get_json()
            body = request.data
            signature = request.headers.get("X-Signature", "")
        except Exception as e:
            return jsonify({"status": "error", "message": "Invalid JSON format"}), 400

        if not transaction_data:
            return jsonify({"status": "error", "message": "Request body must contain valid JSON"}), 400

        logger.info(
            f"Received transaction ingestion request for transaction_id: {transaction_data.get('transaction_id', 'unknown')}"
        )

        # Validate the transaction data
        is_valid, validation_error = validator.full_validation(body, signature, transaction_data)

        if not is_valid:
            # Send to DLQ for validation failures
            dlq_message_id = dlq.send_validation_failure_to_dlq(transaction_data, validation_error)

            logger.warning(f"Transaction validation failed, sent to DLQ: {dlq_message_id}")

            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Transaction validation failed",
                        "validation_error": validation_error,
                        "dlq_message_id": dlq_message_id,
                    }
                ),
                400,
            )

        # Try to publish the valid transaction
        try:
            message_id = publisher.publish_with_retry(
                data=transaction_data,
                max_retries=3,
                attributes={
                    "source": "transaction-ingestion-service",
                    "transaction_type": transaction_data.get("transaction_type", "unknown"),
                },
            )

            logger.info(f"Transaction published successfully: {message_id}")

            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Transaction ingested successfully",
                        "message_id": message_id,
                        "transaction_id": transaction_data.get("transaction_id"),
                    }
                ),
                200,
            )

        except PublishError as e:
            # Send to DLQ for publishing failures
            dlq_message_id = dlq.send_publish_failure_to_dlq(transaction_data, str(e), retry_count=3)

            logger.error(f"Publishing failed, sent to DLQ: {dlq_message_id}")

            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Failed to publish transaction after retries",
                        "publish_error": str(e),
                        "dlq_message_id": dlq_message_id,
                    }
                ),
                500,
            )

    except Exception as e:
        logger.error(f"Unexpected error in transaction ingestion: {str(e)}")

        # Try to send to DLQ for unexpected errors
        try:
            request_data = {}
            try:
                request_data = request.get_json() or {}
            except:
                pass

            dlq_message_id = dlq.send_to_dlq(request_data, f"Unexpected error: {str(e)}")

            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Unexpected error occurred",
                        "error": str(e),
                        "dlq_message_id": dlq_message_id,
                    }
                ),
                500,
            )

        except Exception:
            # If even DLQ fails, return basic error
            return jsonify({"status": "error", "message": "Critical system error", "error": str(e)}), 500


@transaction_bp.route("/api/transactions/validate", methods=["POST"])
def validate_transaction():
    """
    Endpoint to validate transaction data without publishing.
    Useful for testing and validation purposes.
    """
    try:
        if not request.is_json:
            return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

        try:
            transaction_data = request.get_json()
            body = request.data
            signature = request.headers.get("X-Signature", "")
        except Exception as e:
            return jsonify({"status": "error", "message": "Invalid JSON format"}), 400

        if not transaction_data:
            return jsonify({"status": "error", "message": "Request body must contain valid JSON"}), 400

        # Validate the transaction data
        is_valid, validation_error = validator.full_validation(body, signature, transaction_data)

        if is_valid:
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Transaction data is valid",
                        "transaction_id": transaction_data.get("transaction_id"),
                    }
                ),
                200,
            )
        else:
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Transaction validation failed",
                        "validation_error": validation_error,
                    }
                ),
                400,
            )

    except Exception as e:
        logger.error(f"Error in transaction validation: {str(e)}")
        return jsonify({"status": "error", "message": "Unexpected error during validation", "error": str(e)}), 500


@transaction_bp.route("/api/status")
def service_status():
    """Get service status and statistics."""
    try:
        # Get DLQ statistics
        dlq_stats = dlq.get_dlq_stats()

        # Get published message count
        published_count = len(publisher.get_published_messages())

        return (
            jsonify(
                {
                    "status": "healthy",
                    "service": "transaction-ingestion",
                    "statistics": {"published_messages": published_count, "dlq_stats": dlq_stats},
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error getting service status: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to get service status", "error": str(e)}), 500


@transaction_bp.route("/api/dlq/messages")
def get_dlq_messages():
    """Get DLQ messages for monitoring (admin endpoint)."""
    try:
        messages = dlq.get_dlq_messages()
        return jsonify({"status": "success", "dlq_messages": messages, "count": len(messages)}), 200

    except Exception as e:
        logger.error(f"Error getting DLQ messages: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to get DLQ messages", "error": str(e)}), 500


@transaction_bp.route("/api/published/messages")
def get_published_messages():
    """Get published messages for monitoring (admin endpoint)."""
    try:
        messages = publisher.get_published_messages()
        return jsonify({"status": "success", "published_messages": messages, "count": len(messages)}), 200

    except Exception as e:
        logger.error(f"Error getting published messages: {str(e)}")
        return jsonify({"status": "error", "message": "Failed to get published messages", "error": str(e)}), 500
