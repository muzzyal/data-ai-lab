import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from google.api_core import retry
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class PubSubPublisher:
    """Service for publishing messages to Google Pub/Sub."""

    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name
        self.published_messages = []  # Store for testing/verification

        # initialise Pub/Sub client only if running in production
        self.use_real_pubsub = os.environ.get("USE_REAL_PUBSUB", "false").lower() == "true"
        if self.use_real_pubsub:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)
            logger.info(f"PubSubPublisher initialised with real Pub/Sub for topic: {self.topic_path}")
        else:
            self.publisher = None
            self.topic_path = None
            logger.info(f"PubSubPublisher initialised in simulation mode for topic: {self.topic_name}")

    def publish_message(self, data: Dict[str, Any], attributes: Optional[Dict[str, str]] = None) -> str:
        """
        Publish a message to Pub/Sub topic.

        Args:
            data: The transaction data to publish
            attributes: Optional message attributes

        Returns:
            Message ID of the published message

        Raises:
            PublishError: If publishing fails
        """
        try:
            message_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()

            # Create message envelope
            message = {
                "message_id": message_id,
                "data": data,
                "attributes": attributes or {},
                "publish_time": timestamp,
                "topic": f"projects/{self.project_id}/topics/{self.topic_name}",
            }

            if self.use_real_pubsub and self.publisher:
                # Publish to real Pub/Sub
                message_data = json.dumps(data).encode("utf-8")
                future = self.publisher.publish(self.topic_path, message_data, **attributes or {})
                # Get the published message ID
                pubsub_message_id = future.result()
                logger.info(
                    f"Message published to Pub/Sub. Message ID: {pubsub_message_id}, Transaction ID: {data.get('transaction_id')}"
                )

                # Store with real message ID for monitoring
                message["pubsub_message_id"] = pubsub_message_id
                self.published_messages.append(message)
                return str(pubsub_message_id)
            else:
                # Simulation mode for testing
                self._simulate_publish(message)
                self.published_messages.append(message)
                logger.info(
                    f"Message published (simulation). Message ID: {message_id}, Transaction ID: {data.get('transaction_id')}"
                )
                return message_id

        except Exception as e:
            error_msg = f"Failed to publish message: {str(e)}"
            logger.error(error_msg)
            raise PublishError(error_msg)

    def _simulate_publish(self, message: Dict[str, Any]) -> None:
        """
        Simulate the actual publishing process.
        In a real implementation, this would make the API call to Google Pub/Sub.
        """
        # Simulate network delay and potential failures
        import random
        import time

        # Small delay to simulate network latency
        time.sleep(random.uniform(0.01, 0.05))  # nosec

        # Ability to simulate a network error for testing
        if os.getenv("SIMULATE_NETWORK_ERROR", "false").lower() == "true":
            raise Exception("Simulated network error during publishing")

        logger.debug(f"Message simulated publish successful: {message['message_id']}")

    def publish_with_retry(
        self, data: Dict[str, Any], max_retries: int = 3, attributes: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Publish message with retry logic.

        Args:
            data: The transaction data to publish
            max_retries: Maximum number of retry attempts
            attributes: Optional message attributes

        Returns:
            Message ID of the published message

        Raises:
            PublishError: If all retry attempts fail
        """
        last_error = None

        for attempt in range(max_retries + 1):
            try:
                message_id = self.publish_message(data, attributes)
                if attempt > 0:
                    logger.info(f"Message published successfully on retry attempt {attempt}")
                return message_id
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(f"Publish attempt {attempt + 1} failed, retrying in {wait_time}s: {str(e)}")
                    import time

                    time.sleep(wait_time)
                else:
                    logger.error(f"All {max_retries + 1} publish attempts failed")

        raise PublishError(f"Failed to publish after {max_retries + 1} attempts. Last error: {str(last_error)}")

    def get_published_messages(self) -> list:
        """Return list of published messages for testing purposes."""
        return self.published_messages.copy()

    def clear_published_messages(self) -> None:
        """Clear the published messages list."""
        self.published_messages.clear()


class PublishError(Exception):
    """Custom exception for publishing errors."""

    pass
