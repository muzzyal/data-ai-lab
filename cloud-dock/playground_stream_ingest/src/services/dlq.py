import logging
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import uuid
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class DeadLetterQueue:
    """Service for handling messages that failed processing."""

    def __init__(self, project_id: str, dlq_topic_name: str):
        self.project_id = project_id
        self.dlq_topic_name = dlq_topic_name
        self.dlq_messages = []  # Store for testing/verification

        # initialise Pub/Sub client only if running in production
        self.use_real_pubsub = os.environ.get("USE_REAL_PUBSUB", "false").lower() == "true"
        if self.use_real_pubsub:
            self.publisher = pubsub_v1.PublisherClient()
            self.dlq_topic_path = self.publisher.topic_path(self.project_id, self.dlq_topic_name)
            logger.info(f"DeadLetterQueue initialised with real Pub/Sub for topic: {self.dlq_topic_path}")
        else:
            self.publisher = None
            self.dlq_topic_path = None
            logger.info(f"DeadLetterQueue initialised in simulation mode for topic: {self.dlq_topic_name}")

    def send_to_dlq(
        self,
        original_data: Dict[str, Any],
        error_reason: str,
        original_message_id: Optional[str] = None,
        retry_count: int = 0,
    ) -> str:
        """
        Send a failed message to the Dead Letter Queue.

        Args:
            original_data: The original transaction data that failed
            error_reason: Reason why the message failed processing
            original_message_id: ID of the original message (if any)
            retry_count: Number of times processing was retried

        Returns:
            DLQ message ID
        """
        try:
            dlq_message_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()

            # Create DLQ message envelope with error metadata
            dlq_message = {
                "dlq_message_id": dlq_message_id,
                "original_message_id": original_message_id,
                "original_data": original_data,
                "error_details": {
                    "reason": error_reason,
                    "retry_count": retry_count,
                    "failed_at": timestamp,
                    "service": "transaction-ingestion",
                },
                "dlq_topic": f"projects/{self.project_id}/topics/{self.dlq_topic_name}",
                "sent_to_dlq_at": timestamp,
            }

            if self.use_real_pubsub and self.publisher:
                # Send to real DLQ Pub/Sub topic
                message_data = json.dumps(dlq_message).encode("utf-8")
                future = self.publisher.publish(self.dlq_topic_path, message_data)
                pubsub_message_id = future.result()
                dlq_message["pubsub_message_id"] = pubsub_message_id
                logger.info(f"DLQ message published to Pub/Sub. Message ID: {pubsub_message_id}")
            else:
                # Simulation mode for testing
                self._simulate_dlq_send(dlq_message)

            # Store for verification/testing
            self.dlq_messages.append(dlq_message)

            logger.warning(
                f"Message sent to DLQ. DLQ Message ID: {dlq_message_id}, "
                f"Original Transaction ID: {original_data.get('transaction_id')}, "
                f"Reason: {error_reason}"
            )

            return dlq_message_id

        except Exception as e:
            error_msg = f"Failed to send message to DLQ: {str(e)}"
            logger.error(error_msg)
            raise DLQError(error_msg)

    def _simulate_dlq_send(self, dlq_message: Dict[str, Any]) -> None:
        """
        Simulate sending message to DLQ.
        In a real implementation, this would publish to the actual DLQ topic.
        """
        import time
        import random

        # Small delay to simulate network latency
        time.sleep(random.uniform(0.005, 0.02))  # nosec

        logger.debug(f"Message sent to DLQ successfully: {dlq_message['dlq_message_id']}")

    def send_validation_failure_to_dlq(self, original_data: Dict[str, Any], validation_error: str) -> str:
        """
        Send a message that failed validation to DLQ.

        Args:
            original_data: The original transaction data that failed validation
            validation_error: The validation error message

        Returns:
            DLQ message ID
        """
        error_reason = f"Schema validation failed: {validation_error}"
        return self.send_to_dlq(original_data, error_reason, retry_count=0)

    def send_publish_failure_to_dlq(
        self, original_data: Dict[str, Any], publish_error: str, retry_count: int = 0
    ) -> str:
        """
        Send a message that failed publishing to DLQ.

        Args:
            original_data: The original transaction data that failed to publish
            publish_error: The publishing error message
            retry_count: Number of times publishing was retried

        Returns:
            DLQ message ID
        """
        error_reason = f"Publishing failed after {retry_count} retries: {publish_error}"
        return self.send_to_dlq(original_data, error_reason, retry_count=retry_count)

    def get_dlq_messages(self) -> list:
        """Return list of DLQ messages for testing/monitoring purposes."""
        return self.dlq_messages.copy()

    def clear_dlq_messages(self) -> None:
        """Clear the DLQ messages list."""
        self.dlq_messages.clear()

    def get_dlq_stats(self) -> Dict[str, Any]:
        """Get statistics about DLQ messages."""
        total_messages = len(self.dlq_messages)

        if total_messages == 0:
            return {"total_messages": 0, "validation_failures": 0, "publish_failures": 0, "other_failures": 0}

        validation_failures = sum(
            1 for msg in self.dlq_messages if "validation failed" in msg["error_details"]["reason"].lower()
        )
        publish_failures = sum(
            1 for msg in self.dlq_messages if "publishing failed" in msg["error_details"]["reason"].lower()
        )
        other_failures = total_messages - validation_failures - publish_failures

        return {
            "total_messages": total_messages,
            "validation_failures": validation_failures,
            "publish_failures": publish_failures,
            "other_failures": other_failures,
        }


class DLQError(Exception):
    """Custom exception for DLQ operations."""

    pass
