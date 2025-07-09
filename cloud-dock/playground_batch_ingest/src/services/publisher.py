"""
Pub/Sub publisher service for sending processed batch data.
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional
from google.cloud import pubsub_v1
from google.cloud.exceptions import GoogleCloudError
import uuid


logger = logging.getLogger(__name__)


class BatchPublisher:
    """Handles publishing processed batch data to Pub/Sub topics."""

    def __init__(
        self,
        project_id: str,
        topic_name: str,
        use_real_pubsub: bool = True,
        max_retries: int = 3,
    ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.use_real_pubsub = use_real_pubsub
        self.max_retries = max_retries

        if self.use_real_pubsub:
            self.publisher = pubsub_v1.PublisherClient()
            self.topic_path = self.publisher.topic_path(project_id, topic_name)
        else:
            self.publisher = None
            self.topic_path = f"projects/{project_id}/topics/{topic_name}"

        # Message tracking
        self.published_messages = []

    def publish_batch_data(self, processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish processed batch data to Pub/Sub.

        Args:
            processed_data: Dictionary containing processed batch data

        Returns:
            Publishing result summary
        """
        try:
            data_type = processed_data.get("data_type", "unknown")
            data_items = processed_data.get("data", [])

            if not data_items:
                logger.warning("No data items to publish")
                return {
                    "success": True,
                    "published_count": 0,
                    "failed_count": 0,
                    "message_ids": [],
                }

            logger.info(f"Publishing {len(data_items)} {data_type} records")

            published_ids = []
            failed_count = 0

            # Publish each data item as a separate message
            for idx, data_item in enumerate(data_items):
                try:
                    message_id = self._publish_single_message(data_item, data_type, idx, processed_data)
                    if message_id:
                        published_ids.append(message_id)
                    else:
                        failed_count += 1

                except Exception as e:
                    logger.error(f"Error publishing message {idx}: {e}")
                    failed_count += 1

            result = {
                "success": failed_count == 0,
                "published_count": len(published_ids),
                "failed_count": failed_count,
                "message_ids": published_ids,
                "data_type": data_type,
            }

            logger.info(f"Publishing completed: {len(published_ids)} successful, " f"{failed_count} failed")

            return result

        except Exception as e:
            logger.error(f"Error in batch publishing: {e}")
            return {
                "success": False,
                "published_count": 0,
                "failed_count": len(processed_data.get("data", [])),
                "message_ids": [],
                "error": str(e),
            }

    def _publish_single_message(
        self,
        data_item: Dict[str, Any],
        data_type: str,
        index: int,
        batch_context: Dict[str, Any],
    ) -> Optional[str]:
        """Publish a single message with retry logic."""
        message_data = {
            "data": data_item,
            "metadata": {
                "data_type": data_type,
                "batch_index": index,
                "source_file": batch_context.get("file_path", "unknown"),
                "processed_at": time.time(),
                "message_id": str(uuid.uuid4()),
            },
        }

        attributes = {
            "data_type": data_type,
            "source": "batch_ingestion",
            "message_id": message_data["metadata"]["message_id"],
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
                    message_id = future.result(timeout=30)  # Wait for publish confirmation

                else:
                    # Simulation mode
                    message_id = f"sim_{uuid.uuid4().hex[:8]}"
                    logger.debug(f"Simulated publishing message {message_id}")

                # Track successful publication
                self.published_messages.append(
                    {
                        "message_id": message_id,
                        "data_type": data_type,
                        "published_at": time.time(),
                        "topic": self.topic_name,
                        "attributes": attributes,
                    }
                )

                return message_id

            except GoogleCloudError as e:
                if attempt < self.max_retries:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(f"Pub/Sub error on attempt {attempt + 1}, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to publish message after {self.max_retries + 1} attempts: {e}")
                    return None

            except Exception as e:
                logger.error(f"Unexpected error publishing message: {e}")
                return None

        return None

    def get_published_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recently published messages for monitoring."""
        return self.published_messages[-limit:]

    def clear_message_history(self) -> None:
        """Clear published message history."""
        self.published_messages.clear()
        logger.info("Cleared published message history")

    def get_topic_info(self) -> Dict[str, Any]:
        """Get information about the configured topic."""
        return {
            "project_id": self.project_id,
            "topic_name": self.topic_name,
            "topic_path": self.topic_path,
            "use_real_pubsub": self.use_real_pubsub,
            "max_retries": self.max_retries,
            "published_count": len(self.published_messages),
        }
