import pytest
from unittest.mock import patch, MagicMock
from playground_stream_ingest.src.services.dlq import DeadLetterQueue, DLQError
import time
import os


class TestDeadLetterQueueRealPubSub:
    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        # Always enable real pubsub for these tests
        monkeypatch.setenv("USE_REAL_PUBSUB", "true")

    def test_dlq_init_with_real_pubsub(self):
        # Patch the PublisherClient
        with patch("playground_stream_ingest.src.services.dlq.pubsub_v1.PublisherClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.topic_path.return_value = "projects/test-project/topics/test-dlq-topic"

            dlq = DeadLetterQueue("test-project", "test-dlq-topic")

            assert dlq.use_real_pubsub is True
            assert dlq.publisher == mock_instance
            assert dlq.dlq_topic_path == "projects/test-project/topics/test-dlq-topic"

    def test_send_to_dlq_with_real_pubsub(self, sample_transaction):
        # Patch the PublisherClient and its publish method
        with patch("playground_stream_ingest.src.services.dlq.pubsub_v1.PublisherClient") as mock_client:
            mock_instance = MagicMock()
            mock_client.return_value = mock_instance
            mock_instance.topic_path.return_value = "projects/test-project/topics/test-dlq-topic"
            # Mock the publish method to return a future with a result
            mock_future = MagicMock()
            mock_future.result.return_value = "pubsub-message-id-123"
            mock_instance.publish.return_value = mock_future

            dlq = DeadLetterQueue("test-project", "test-dlq-topic")
            dlq.use_real_pubsub = True

            dlq_message_id = dlq.send_to_dlq(sample_transaction, "Test error for real pubsub")

            # Check that publish was called
            assert mock_instance.publish.called
            # Check that the returned message ID is a string
            assert isinstance(dlq_message_id, str)


class TestDeadLetterQueue:
    """Test cases for the DeadLetterQueue service."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
        dlq_topic_name = os.environ["DLQ_TOPIC_NAME"]

        self.dlq = DeadLetterQueue(project_id, dlq_topic_name)
        # Clear any previous messages
        self.dlq.clear_dlq_messages()

    def test_dlq_initialization(self):
        """Test that the DLQ initializes correctly."""
        assert self.dlq is not None
        assert self.dlq.project_id == os.environ["GOOGLE_CLOUD_PROJECT"]
        assert self.dlq.dlq_topic_name == os.environ["DLQ_TOPIC_NAME"]
        assert len(self.dlq.dlq_messages) == 0

    def test_dlq_custom_initialization(self):
        """Test DLQ initialization with custom parameters."""
        custom_dlq = DeadLetterQueue(project_id="custom-project", dlq_topic_name="custom-dlq")
        assert custom_dlq.project_id == "custom-project"
        assert custom_dlq.dlq_topic_name == "custom-dlq"

    def test_send_to_dlq_basic(self, sample_transaction):
        """Test basic DLQ message sending."""
        error_reason = "Test validation failure"

        dlq_message_id = self.dlq.send_to_dlq(original_data=sample_transaction, error_reason=error_reason)

        assert dlq_message_id is not None
        assert isinstance(dlq_message_id, str)
        assert len(dlq_message_id) > 0

        # Check that message was stored
        dlq_messages = self.dlq.get_dlq_messages()
        assert len(dlq_messages) == 1

        stored_message = dlq_messages[0]
        assert stored_message["dlq_message_id"] == dlq_message_id
        assert stored_message["original_data"] == sample_transaction
        assert stored_message["error_details"]["reason"] == error_reason

    def test_send_to_dlq_with_all_parameters(self, sample_transaction):
        """Test DLQ message sending with all parameters."""
        error_reason = "Publishing failed after retries"
        original_message_id = "original_msg_123"
        retry_count = 3

        dlq_message_id = self.dlq.send_to_dlq(
            original_data=sample_transaction,
            error_reason=error_reason,
            original_message_id=original_message_id,
            retry_count=retry_count,
        )

        dlq_messages = self.dlq.get_dlq_messages()
        stored_message = dlq_messages[0]

        assert stored_message["original_message_id"] == original_message_id
        assert stored_message["error_details"]["retry_count"] == retry_count
        assert stored_message["error_details"]["reason"] == error_reason

    def test_send_validation_failure_to_dlq(self, invalid_transaction):
        """Test sending validation failures to DLQ."""
        validation_error = "Schema validation failed: amount must be positive"

        dlq_message_id = self.dlq.send_validation_failure_to_dlq(
            original_data=invalid_transaction, validation_error=validation_error
        )

        dlq_messages = self.dlq.get_dlq_messages()
        stored_message = dlq_messages[0]

        assert "Schema validation failed" in stored_message["error_details"]["reason"]
        assert validation_error in stored_message["error_details"]["reason"]
        assert stored_message["error_details"]["retry_count"] == 0

    def test_send_publish_failure_to_dlq(self, sample_transaction):
        """Test sending publishing failures to DLQ."""
        publish_error = "Network timeout during publishing"
        retry_count = 2

        dlq_message_id = self.dlq.send_publish_failure_to_dlq(
            original_data=sample_transaction, publish_error=publish_error, retry_count=retry_count
        )

        dlq_messages = self.dlq.get_dlq_messages()
        stored_message = dlq_messages[0]

        assert "Publishing failed" in stored_message["error_details"]["reason"]
        assert publish_error in stored_message["error_details"]["reason"]
        assert stored_message["error_details"]["retry_count"] == retry_count

    def test_dlq_message_structure(self, sample_transaction):
        """Test the structure of DLQ messages."""
        dlq_message_id = self.dlq.send_to_dlq(original_data=sample_transaction, error_reason="Test error")

        dlq_messages = self.dlq.get_dlq_messages()
        message = dlq_messages[0]

        # Check required fields
        required_fields = [
            "dlq_message_id",
            "original_message_id",
            "original_data",
            "error_details",
            "dlq_topic",
            "sent_to_dlq_at",
        ]
        for field in required_fields:
            assert field in message

        # Check error_details structure
        error_details = message["error_details"]
        error_required_fields = ["reason", "retry_count", "failed_at", "service"]
        for field in error_required_fields:
            assert field in error_details

        # Check data types
        assert isinstance(message["dlq_message_id"], str)
        assert isinstance(message["original_data"], dict)
        assert isinstance(message["error_details"], dict)
        assert isinstance(message["dlq_topic"], str)
        assert isinstance(message["sent_to_dlq_at"], str)

        # Check topic format
        expected_topic = f"projects/{self.dlq.project_id}/topics/{self.dlq.dlq_topic_name}"
        assert message["dlq_topic"] == expected_topic

    def test_multiple_dlq_messages(self, sample_transaction, minimal_transaction):
        """Test sending multiple messages to DLQ."""
        dlq_id_1 = self.dlq.send_validation_failure_to_dlq(sample_transaction, "Error 1")
        dlq_id_2 = self.dlq.send_publish_failure_to_dlq(minimal_transaction, "Error 2", 1)

        assert dlq_id_1 != dlq_id_2

        dlq_messages = self.dlq.get_dlq_messages()
        assert len(dlq_messages) == 2

        message_ids = [msg["dlq_message_id"] for msg in dlq_messages]
        assert dlq_id_1 in message_ids
        assert dlq_id_2 in message_ids

    def test_clear_dlq_messages(self, sample_transaction):
        """Test clearing the DLQ messages list."""
        # Send some messages to DLQ
        self.dlq.send_to_dlq(sample_transaction, "Error 1")
        self.dlq.send_to_dlq(sample_transaction, "Error 2")

        assert len(self.dlq.get_dlq_messages()) == 2

        # Clear messages
        self.dlq.clear_dlq_messages()

        assert len(self.dlq.get_dlq_messages()) == 0

    def test_get_dlq_messages_returns_copy(self, sample_transaction):
        """Test that get_dlq_messages returns a copy, not the original list."""
        self.dlq.send_to_dlq(sample_transaction, "Test error")

        messages_1 = self.dlq.get_dlq_messages()
        messages_2 = self.dlq.get_dlq_messages()

        # Should be equal but not the same object
        assert messages_1 == messages_2
        assert messages_1 is not messages_2

        # Modifying one shouldn't affect the other
        messages_1.append("test")
        assert len(messages_2) == 1

    def test_get_dlq_stats_empty(self):
        """Test DLQ statistics when no messages exist."""
        stats = self.dlq.get_dlq_stats()

        expected_stats = {"total_messages": 0, "validation_failures": 0, "publish_failures": 0, "other_failures": 0}

        assert stats == expected_stats

    def test_get_dlq_stats_with_messages(self, sample_transaction, minimal_transaction):
        """Test DLQ statistics with various message types."""
        # Add validation failures
        self.dlq.send_validation_failure_to_dlq(sample_transaction, "Schema error")
        self.dlq.send_validation_failure_to_dlq(minimal_transaction, "Another validation error")

        # Add publish failures
        self.dlq.send_publish_failure_to_dlq(sample_transaction, "Network error", 2)

        # Add other failure
        self.dlq.send_to_dlq(sample_transaction, "Unknown system error")

        stats = self.dlq.get_dlq_stats()

        assert stats["total_messages"] == 4
        assert stats["validation_failures"] == 2
        assert stats["publish_failures"] == 1
        assert stats["other_failures"] == 1

    def test_timestamp_format(self, sample_transaction):
        """Test that DLQ timestamps are in correct ISO format."""
        before_send = time.time()
        dlq_message_id = self.dlq.send_to_dlq(sample_transaction, "Test error")
        after_send = time.time()

        dlq_messages = self.dlq.get_dlq_messages()
        message = dlq_messages[0]

        sent_time = message["sent_to_dlq_at"]
        failed_time = message["error_details"]["failed_at"]

        # Should be valid ISO format strings
        for timestamp in [sent_time, failed_time]:
            assert isinstance(timestamp, str)
            assert "T" in timestamp
            assert timestamp.endswith("Z") or "+" in timestamp or "-" in timestamp[-6:]

            # Should be reasonably close to current time
            from datetime import datetime

            parsed_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            timestamp_value = parsed_time.timestamp()

            # Should be within a reasonable time window (10 seconds)
            assert before_send - 10 <= timestamp_value <= after_send + 10

    def test_dlq_message_id_uniqueness(self, sample_transaction):
        """Test that DLQ message IDs are unique."""
        message_ids = set()

        # Send multiple messages quickly
        for i in range(10):
            dlq_message_id = self.dlq.send_to_dlq(sample_transaction, f"Error {i}")
            assert dlq_message_id not in message_ids, f"Duplicate DLQ message ID: {dlq_message_id}"
            message_ids.add(dlq_message_id)

        assert len(message_ids) == 10

    def test_error_handling_in_dlq(self):
        """Test error handling during DLQ operations."""
        # Test with invalid data that might cause errors
        invalid_data = None

        with pytest.raises(DLQError):
            self.dlq.send_to_dlq(invalid_data, "Test error")

    def test_service_metadata_in_dlq(self, sample_transaction):
        """Test that service metadata is correctly included in DLQ messages."""
        dlq_message_id = self.dlq.send_to_dlq(sample_transaction, "Test error")

        dlq_messages = self.dlq.get_dlq_messages()
        message = dlq_messages[0]

        assert message["error_details"]["service"] == "transaction-ingestion"

    def test_large_error_message_handling(self, sample_transaction):
        """Test handling of large error messages."""
        large_error = "x" * 10000  # 10KB error message

        dlq_message_id = self.dlq.send_to_dlq(sample_transaction, large_error)

        dlq_messages = self.dlq.get_dlq_messages()
        message = dlq_messages[0]

        assert message["error_details"]["reason"] == large_error
        assert len(message["error_details"]["reason"]) == 10000

    def test_stats_categorization_case_sensitivity(self, sample_transaction):
        """Test that stats categorization works with different case variations."""
        # Test validation failures with different cases
        self.dlq.send_to_dlq(sample_transaction, "Schema VALIDATION Failed: error")
        self.dlq.send_to_dlq(sample_transaction, "validation failed: another error")

        # Test publishing failures with different cases
        self.dlq.send_to_dlq(sample_transaction, "PUBLISHING FAILED: network error")
        self.dlq.send_to_dlq(sample_transaction, "Publishing Failed after retries")

        stats = self.dlq.get_dlq_stats()

        assert stats["validation_failures"] == 2
        assert stats["publish_failures"] == 2
        assert stats["other_failures"] == 0
        assert stats["total_messages"] == 4
