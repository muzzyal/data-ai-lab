"""
Flask routes for batch ingestion service.
"""

import base64
import json
import logging
from typing import Any, Dict

from cloudevents.http import from_http
from flask import Blueprint, jsonify, request

from playground_batch_ingest.src.config_loader.loader import config_loader
from playground_batch_ingest.src.services.batch_processor import BatchProcessor

logger = logging.getLogger(__name__)

batch_bp = Blueprint("batch", __name__, url_prefix="/api/batch")


def get_batch_processor() -> BatchProcessor:
    """Get configured batch processor instance."""
    config = config_loader.get_config()
    return BatchProcessor(config)


@batch_bp.route("/gcs-event", methods=["POST"])
def handle_gcs_event():
    """
    Handle GCS file creation events from Eventarc CloudEvents.

    Expected CloudEvent format from Eventarc:
    {
        "specversion": "1.0",
        "type": "google.cloud.storage.object.v1.finalized",
        "source": "//storage.googleapis.com/projects/_/buckets/bucket-name",
        "id": "unique-event-id",
        "time": "2023-01-01T12:00:00Z",
        "datacontenttype": "application/json",
        "data": {
            "bucket": "bucket-name",
            "name": "file.csv",
            "generation": "123456"
        }
    }
    """
    try:
        # Parse CloudEvent from Eventarc
        event = from_http(request.headers, request.get_data())
        logger.info(f"Received CloudEvent: {event}")

        # Extract GCS event data
        event_data = event.data
        if not event_data:
            logger.error("No data in CloudEvent")
            return jsonify({"error": "No data in CloudEvent"}), 400

        # Log the event
        logger.info(f"Received GCS event: {event_data}")

        # Process the event
        processor = get_batch_processor()
        result = processor.process_gcs_event(event_data)

        # Return appropriate HTTP status
        if result.get("success"):
            return jsonify(result), 200
        elif result.get("skipped"):
            return jsonify(result), 200  # Successful skip
        else:
            return jsonify(result), 500

    except Exception as e:
        logger.error(f"Error handling GCS event: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/process-file", methods=["POST"])
def process_single_file():
    """
    Process a single file directly (for testing/manual processing).

    Expected request format:
    {
        "bucket_name": "my-bucket",
        "object_name": "path/to/file.csv"
    }
    """
    try:
        data = request.get_json(force=False, silent=True)
    except Exception:
        return jsonify({"error": "Missing request body"}), 400

    if not data:
        return jsonify({"error": "Missing request body"}), 400

    bucket_name = data.get("bucket_name") if isinstance(data, dict) else None
    object_name = data.get("object_name") if isinstance(data, dict) else None
    if not bucket_name or not object_name:
        return jsonify({"error": "Missing bucket_name or object_name"}), 400

    logger.info(f"Manual processing request for {bucket_name}/{object_name}")

    processor = get_batch_processor()
    result = processor.process_file(bucket_name, object_name)
    if result.get("success"):
        return jsonify(result), 200
    else:
        return jsonify(result), 500


@batch_bp.route("/process-multiple", methods=["POST"])
def process_multiple_files():
    """
    Process multiple files concurrently.

    Expected request format:
    {
        "files": [
            {"bucket_name": "bucket1", "object_name": "file1.csv"},
            {"bucket_name": "bucket2", "object_name": "file2.csv"}
        ]
    }
    """
    try:
        request_json = request.get_json()

        if not request_json or "files" not in request_json:
            return jsonify({"error": "Missing files list"}), 400

        files = request_json["files"]

        if not isinstance(files, list) or not files:
            return jsonify({"error": "Files must be a non-empty list"}), 400

        # Validate file entries
        for file_info in files:
            if not isinstance(file_info, dict):
                return jsonify({"error": "Each file entry must be an object"}), 400
            if not file_info.get("bucket_name") or not file_info.get("object_name"):
                return jsonify({"error": "Missing bucket_name or object_name in file entry"}), 400

        logger.info(f"Multiple file processing request for {len(files)} files")

        # Process files
        processor = get_batch_processor()
        result = processor.process_multiple_files(files)

        if result.get("success"):
            return jsonify(result), 200
        else:
            return jsonify(result), 207  # Multi-status

    except Exception as e:
        logger.error(f"Error processing multiple files: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/stats", methods=["GET"])
def get_processing_stats():
    """Get processing statistics and service status."""
    try:
        processor = get_batch_processor()
        stats = processor.get_processing_stats()

        return (
            jsonify(
                {
                    "service": "batch_ingestion",
                    "status": "healthy",
                    "stats": stats,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error getting processing stats: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/published", methods=["GET"])
def get_published_messages():
    """Get recently published messages for monitoring."""
    try:
        limit = request.args.get("limit", 100, type=int)

        processor = get_batch_processor()
        messages = processor.publisher.get_published_messages(limit=limit)

        return (
            jsonify(
                {
                    "published_messages": messages,
                    "count": len(messages),
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error getting published messages: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/dlq", methods=["GET"])
def get_dlq_messages():
    """Get recent dead letter queue messages for monitoring."""
    try:
        limit = request.args.get("limit", 100, type=int)

        processor = get_batch_processor()
        messages = processor.dlq.get_dlq_messages(limit=limit)
        stats = processor.dlq.get_dlq_stats()

        return (
            jsonify(
                {
                    "dlq_messages": messages,
                    "count": len(messages),
                    "stats": stats,
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error getting DLQ messages: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/cleanup", methods=["POST"])
def cleanup_temp_files():
    """Clean up temporary files (admin endpoint)."""
    try:
        processor = get_batch_processor()
        processor.cleanup_temp_files()

        return jsonify({"message": "Cleanup completed successfully"}), 200

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return jsonify({"error": str(e)}), 500


@batch_bp.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return (
        jsonify(
            {
                "service": "batch_ingestion",
                "status": "healthy",
                "version": "0.1.0",
            }
        ),
        200,
    )
