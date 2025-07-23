"""
Dead Letter Queue service for handling failed batch processing.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from google.cloud import pubsub_v1
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Handles failed batch processing messages and sends them to DLQ."""

    def __init__(
        self,
        project_id: str,
        dlq_topic: str,
        use_real_pubsub: bool = True,
        max_retries: int = 3,
    ):
        self.project_id = project_id
        self.dlq_topic = dlq_topic
        self.use_real_pubsub = use_real_pubsub
        self.max_retries = max_retries

        if self.use_real_pubsub:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(project_id, dlq_topic)
        else:
            self.publisher = None
            self.topic_path = f"projects/{project_id}/topics/{dlq_topic}"

        # DLQ message tracking
        self.dlq_messages = []

    def send_processing_error(
        self,
        original_data: Dict[str, Any],
        error_reason: str,
        error_details: Dict[str, Any] = None,
        source_file: str = None,
    ) -> bool:
        """
        Send a processing error to the dead letter queue.

        Args:
            original_data: Original data that failed processing
            error_reason: Reason for the failure
            error_details: Additional error details
            source_file: Source file path

        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        try:
            dlq_message = {
                "error_type": "processing_error",
                "error_reason": error_reason,
                "error_details": error_details or {},
                "original_data": original_data,
                "source_file": source_file,
                "timestamp": time.time(),
                "service": "batch_ingestion",
                "message_id": str(uuid.uuid4()),
            }

            return self._send_to_dlq(dlq_message, "processing_error")

        except Exception as e:
            logger.error(f"Error sending processing error to DLQ: {e}")
            return False

    def send_file_error(
        self,
        file_path: str,
        bucket_name: str,
        object_name: str,
        error_reason: str,
        error_details: Dict[str, Any] = None,
    ) -> bool:
        """
        Send a file processing error to the dead letter queue.

        Args:
            file_path: Local file path (if downloaded)
            bucket_name: GCS bucket name
            object_name: GCS object name
            error_reason: Reason for the failure
            error_details: Additional error details

        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        try:
            dlq_message = {
                "error_type": "file_error",
                "error_reason": error_reason,
                "error_details": error_details or {},
                "file_info": {
                    "local_path": file_path,
                    "bucket_name": bucket_name,
                    "object_name": object_name,
                },
                "timestamp": time.time(),
                "service": "batch_ingestion",
                "message_id": str(uuid.uuid4()),
            }

            return self._send_to_dlq(dlq_message, "file_error")

        except Exception as e:
            logger.error(f"Error sending file error to DLQ: {e}")
            return False

    def send_validation_errors(
        self,
        validation_errors: List[Dict[str, Any]],
        source_file: str,
        data_type: str,
    ) -> bool:
        """
        Send validation errors to the dead letter queue.

        Args:
            validation_errors: List of validation error records
            source_file: Source file path
            data_type: Type of data being processed

        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        try:
            dlq_message = {
                "error_type": "validation_errors",
                "error_reason": "Schema validation failed",
                "error_details": {
                    "data_type": data_type,
                    "validation_errors": validation_errors,
                },
                "source_file": source_file,
                "timestamp": time.time(),
                "service": "batch_ingestion",
                "message_id": str(uuid.uuid4()),
            }

            return self._send_to_dlq(dlq_message, "validation_errors")

        except Exception as e:
            logger.error(f"Error sending validation errors to DLQ: {e}")
            return False

    def send_publishing_error(
        self,
        processed_data: Dict[str, Any],
        publishing_result: Dict[str, Any],
        error_reason: str,
    ) -> bool:
        """
        Send a publishing error to the dead letter queue.

        Args:
            processed_data: Data that failed to publish
            publishing_result: Result from the publishing attempt
            error_reason: Reason for the publishing failure

        Returns:
            True if successfully sent to DLQ, False otherwise
        """
        try:
            dlq_message = {
                "error_type": "publishing_error",
                "error_reason": error_reason,
                "error_details": {
                    "publishing_result": publishing_result,
                    "failed_count": publishing_result.get("failed_count", 0),
                    "published_count": publishing_result.get("published_count", 0),
                },
                "original_data": processed_data,
                "timestamp": time.time(),
                "service": "batch_ingestion",
                "message_id": str(uuid.uuid4()),
            }

            return self._send_to_dlq(dlq_message, "publishing_error")

        except Exception as e:
            logger.error(f"Error sending publishing error to DLQ: {e}")
            return False

    def _send_to_dlq(self, message_data: Dict[str, Any], error_type: str) -> bool:
        """Send a message to the dead letter queue with retry logic."""
        attributes = {
            "error_type": error_type,
            "service": "batch_ingestion",
            "message_id": message_data["message_id"],
            "timestamp": str(int(message_data["timestamp"])),
        }

        for attempt in range(self.max_retries + 1):
            try:
                if self.use_real_pubsub:
                    # Real Pub/Sub publishing
                    message_json = json.dumps(message_data)
                    future = self.publisher.publish(
                        self.topic_path,
                        message_json.encode("utf-8"),
                        **attributes,
                    )
                    message_id = future.result(timeout=30)

                else:
                    # Simulation mode
                    message_id = f"dlq_sim_{uuid.uuid4().hex[:8]}"
                    logger.debug(f"Simulated DLQ message {message_id}")

                # Track DLQ message
                self.dlq_messages.append(
                    {
                        "message_id": message_id,
                        "error_type": error_type,
                        "sent_at": time.time(),
                        "topic": self.dlq_topic,
                        "attributes": attributes,
                    }
                )

                logger.info(f"Sent {error_type} to DLQ: {message_id}")
                return True

            except GoogleCloudError as e:
                if attempt < self.max_retries:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(f"DLQ publish error on attempt {attempt + 1}, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to send to DLQ after {self.max_retries + 1} attempts: {e}")
                    return False

            except Exception as e:
                logger.error(f"Unexpected error sending to DLQ: {e}")
                return False

        return False

    def get_dlq_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent DLQ messages for monitoring."""
        return self.dlq_messages[-limit:]

    def clear_dlq_history(self) -> None:
        """Clear DLQ message history."""
        self.dlq_messages.clear()
        logger.info("Cleared DLQ message history")

    def get_dlq_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        if not self.dlq_messages:
            return {
                "total_messages": 0,
                "error_types": {},
                "recent_count": 0,
            }

        error_types = {}
        recent_count = 0
        recent_threshold = time.time() - 3600  # Last hour

        for msg in self.dlq_messages:
            error_type = msg.get("error_type", "unknown")
            error_types[error_type] = error_types.get(error_type, 0) + 1

            if msg.get("sent_at", 0) > recent_threshold:
                recent_count += 1

        return {
            "total_messages": len(self.dlq_messages),
            "error_types": error_types,
            "recent_count": recent_count,
            "dlq_topic": self.dlq_topic,
        }
