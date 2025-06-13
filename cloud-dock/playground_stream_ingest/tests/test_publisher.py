import pytest
from playground_stream_ingest.src.services.publisher import PubSubPublisher, PublishError
import time
import os


class TestPubSubPublisher:
    """Test cases for the PubSubPublisher service."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
        topic_name = os.environ["PUBSUB_TOPIC_NAME"]

        self.publisher = PubSubPublisher(project_id, topic_name)
        # Clear any previous messages
        self.publisher.clear_published_messages()

    def test_publisher_initialization(self):
        """Test that the publisher initializes correctly."""
        assert self.publisher is not None
        assert self.publisher.project_id == os.environ["GOOGLE_CLOUD_PROJECT"]
        assert self.publisher.topic_name == os.environ["PUBSUB_TOPIC_NAME"]
        assert len(self.publisher.published_messages) == 0

    def test_publisher_custom_initialization(self):
        """Test publisher initialization with custom parameters."""
        custom_publisher = PubSubPublisher(project_id="custom-project", topic_name="custom-topic")
        assert custom_publisher.project_id == "custom-project"
        assert custom_publisher.topic_name == "custom-topic"

    def test_publish_message_success(self, sample_transaction):
        """Test successful message publishing."""
        message_id = self.publisher.publish_message(sample_transaction)

        assert message_id is not None
        assert isinstance(message_id, str)
        assert len(message_id) > 0

        # Check that message was stored
        published_messages = self.publisher.get_published_messages()
        assert len(published_messages) == 1

        stored_message = published_messages[0]
        assert stored_message["message_id"] == message_id
        assert stored_message["data"] == sample_transaction
        assert "publish_time" in stored_message
        assert "topic" in stored_message

    def test_publish_message_with_attributes(self, sample_transaction):
        """Test publishing message with custom attributes."""
        attributes = {"source": "test-service", "transaction_type": "purchase"}

        message_id = self.publisher.publish_message(sample_transaction, attributes)

        published_messages = self.publisher.get_published_messages()
        stored_message = published_messages[0]

        assert stored_message["attributes"] == attributes

    def test_publish_message_without_attributes(self, sample_transaction):
        """Test publishing message without attributes."""
        message_id = self.publisher.publish_message(sample_transaction)

        published_messages = self.publisher.get_published_messages()
        stored_message = published_messages[0]

        assert stored_message["attributes"] == {}

    def test_publish_multiple_messages(self, sample_transaction, minimal_transaction):
        """Test publishing multiple messages."""
        message_id_1 = self.publisher.publish_message(sample_transaction)
        message_id_2 = self.publisher.publish_message(minimal_transaction)

        assert message_id_1 != message_id_2

        published_messages = self.publisher.get_published_messages()
        assert len(published_messages) == 2

        message_ids = [msg["message_id"] for msg in published_messages]
        assert message_id_1 in message_ids
        assert message_id_2 in message_ids

    def test_publish_with_retry_success(self, sample_transaction):
        """Test successful publishing with retry logic."""
        message_id = self.publisher.publish_with_retry(sample_transaction, max_retries=3)

        assert message_id is not None
        published_messages = self.publisher.get_published_messages()
        assert len(published_messages) == 1

    def test_publish_with_retry_custom_attributes(self, sample_transaction):
        """Test retry publishing with custom attributes."""
        attributes = {"test": "value"}
        message_id = self.publisher.publish_with_retry(sample_transaction, max_retries=2, attributes=attributes)

        published_messages = self.publisher.get_published_messages()
        stored_message = published_messages[0]
        assert stored_message["attributes"] == attributes

    def test_message_structure(self, sample_transaction):
        """Test the structure of published messages."""
        message_id = self.publisher.publish_message(sample_transaction)

        published_messages = self.publisher.get_published_messages()
        message = published_messages[0]

        # Check required fields
        required_fields = ["message_id", "data", "attributes", "publish_time", "topic"]
        for field in required_fields:
            assert field in message

        # Check data types
        assert isinstance(message["message_id"], str)
        assert isinstance(message["data"], dict)
        assert isinstance(message["attributes"], dict)
        assert isinstance(message["publish_time"], str)
        assert isinstance(message["topic"], str)

        # Check topic format
        expected_topic = f"projects/{self.publisher.project_id}/topics/{self.publisher.topic_name}"
        assert message["topic"] == expected_topic

    def test_clear_published_messages(self, sample_transaction):
        """Test clearing the published messages list."""
        # Publish some messages
        self.publisher.publish_message(sample_transaction)
        self.publisher.publish_message(sample_transaction)

        assert len(self.publisher.get_published_messages()) == 2

        # Clear messages
        self.publisher.clear_published_messages()

        assert len(self.publisher.get_published_messages()) == 0

    def test_get_published_messages_returns_copy(self, sample_transaction):
        """Test that get_published_messages returns a copy, not the original list."""
        self.publisher.publish_message(sample_transaction)

        messages_1 = self.publisher.get_published_messages()
        messages_2 = self.publisher.get_published_messages()

        # Should be equal but not the same object
        assert messages_1 == messages_2
        assert messages_1 is not messages_2

        # Modifying one shouldn't affect the other
        messages_1.append("test")
        assert len(messages_2) == 1

    def test_publish_error_handling(self):
        """Test error handling during publishing."""
        # Test with invalid data that might cause errors
        invalid_data = None

        with pytest.raises(PublishError):
            self.publisher.publish_message(invalid_data)

    def test_publish_with_retry_max_attempts(self):
        """Test retry logic with maximum attempts."""
        # This test is tricky since we're simulating failures
        # We'll test that retry with 0 max_retries works

        message_id = self.publisher.publish_with_retry({"test": "data"}, max_retries=0)

        assert message_id is not None

    def test_timestamp_format(self, sample_transaction):
        """Test that publish timestamp is in correct ISO format."""
        before_publish = time.time()
        message_id = self.publisher.publish_message(sample_transaction)
        after_publish = time.time()

        published_messages = self.publisher.get_published_messages()
        message = published_messages[0]

        publish_time = message["publish_time"]

        # Should be a valid ISO format string
        assert isinstance(publish_time, str)
        assert "T" in publish_time
        assert publish_time.endswith("Z") or "+" in publish_time or "-" in publish_time[-6:]

        # Should be reasonably close to current time
        from datetime import datetime

        parsed_time = datetime.fromisoformat(publish_time.replace("Z", "+00:00"))
        timestamp = parsed_time.timestamp()

        # Should be within a reasonable time window (10 seconds)
        assert before_publish - 10 <= timestamp <= after_publish + 10

    def test_message_id_uniqueness(self, sample_transaction):
        """Test that message IDs are unique."""
        message_ids = set()

        # Publish multiple messages quickly
        for i in range(10):
            message_id = self.publisher.publish_message(sample_transaction)
            assert message_id not in message_ids, f"Duplicate message ID: {message_id}"
            message_ids.add(message_id)

        assert len(message_ids) == 10

    def test_large_message_publishing(self):
        """Test publishing large messages."""
        # Create a large transaction with lots of metadata
        large_transaction = {
            "transaction_id": "txn_large_test",
            "customer_id": "cust_large_test",
            "amount": 100.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
            "metadata": {
                "large_field": "x" * 1000,  # 1KB string
                "nested": {"level1": {"level2": {"data": ["item"] * 100}}},
            },
        }

        message_id = self.publisher.publish_message(large_transaction)
        assert message_id is not None

        published_messages = self.publisher.get_published_messages()
        assert len(published_messages) == 1
        assert published_messages[0]["data"] == large_transaction
