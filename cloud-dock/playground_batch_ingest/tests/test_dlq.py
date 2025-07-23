"""
Tests for Dead Letter Queue service.
"""

import time
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.exceptions import GoogleCloudError

from playground_batch_ingest.src.services.dlq import DeadLetterQueue


@pytest.fixture
def mock_publisher_client():
    """Mock Pub/Sub publisher client."""
    with patch("playground_batch_ingest.src.services.dlq.pubsub_v1.PublisherClient") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def dlq_real_pubsub(mock_publisher_client):
    """Create DLQ with real Pub/Sub enabled."""
    return DeadLetterQueue(project_id="test-project", dlq_topic="test-dlq", use_real_pubsub=True, max_retries=2)


@pytest.fixture
def dlq_sim_pubsub():
    """Create DLQ with simulated Pub/Sub."""
    return DeadLetterQueue(project_id="test-project", dlq_topic="test-dlq", use_real_pubsub=False, max_retries=2)


def test_dlq_initialisation_real_pubsub(mock_publisher_client):
    """Test DLQ initialisation with real Pub/Sub."""
    dlq = DeadLetterQueue(project_id="test-project", dlq_topic="test-dlq", use_real_pubsub=True)

    assert dlq.project_id == "test-project"
    assert dlq.dlq_topic == "test-dlq"
    assert dlq.use_real_pubsub is True
    assert dlq.max_retries == 3
    assert dlq.publisher == mock_publisher_client
    assert dlq.topic_path == mock_publisher_client.topic_path.return_value


def test_dlq_initialisation_sim_pubsub():
    """Test DLQ initialisation with simulated Pub/Sub."""
    dlq = DeadLetterQueue(project_id="test-project", dlq_topic="test-dlq", use_real_pubsub=False)

    assert dlq.project_id == "test-project"
    assert dlq.dlq_topic == "test-dlq"
    assert dlq.use_real_pubsub is False
    assert dlq.publisher is None
    assert dlq.topic_path == "projects/test-project/topics/test-dlq"


def test_send_processing_error_success(dlq_sim_pubsub):
    """Test sending processing error to DLQ."""
    original_data = {"test": "data"}
    error_reason = "Processing failed"
    error_details = {"exception": "ValueError"}
    source_file = "/tmp/test.csv"

    result = dlq_sim_pubsub.send_processing_error(original_data, error_reason, error_details, source_file)

    assert result is True

    # Check that message was tracked
    messages = dlq_sim_pubsub.get_dlq_messages()
    assert len(messages) == 1

    message = messages[0]
    assert message["error_type"] == "processing_error"
    assert "message_id" in message


def test_send_file_error_success(dlq_sim_pubsub):
    """Test sending file error to DLQ."""
    result = dlq_sim_pubsub.send_file_error(
        file_path="/tmp/test.csv",
        bucket_name="test-bucket",
        object_name="test.csv",
        error_reason="Download failed",
        error_details={"status": 404},
    )

    assert result is True

    # Check message structure
    messages = dlq_sim_pubsub.get_dlq_messages()
    assert len(messages) == 1

    message = messages[0]
    assert message["error_type"] == "file_error"


def test_send_validation_errors_success(dlq_sim_pubsub):
    """Test sending validation errors to DLQ."""
    validation_errors = [{"row": 1, "error": "Missing field"}, {"row": 3, "error": "Invalid format"}]

    result = dlq_sim_pubsub.send_validation_errors(
        validation_errors=validation_errors, source_file="/tmp/test.csv", data_type="transaction"
    )

    assert result is True

    # Check message structure
    messages = dlq_sim_pubsub.get_dlq_messages()
    assert len(messages) == 1

    message = messages[0]
    assert message["error_type"] == "validation_errors"


def test_send_publishing_error_success(dlq_sim_pubsub):
    """Test sending publishing error to DLQ."""
    processed_data = {"data": [{"id": "1"}]}
    publishing_result = {"published_count": 0, "failed_count": 1}

    result = dlq_sim_pubsub.send_publishing_error(
        processed_data=processed_data, publishing_result=publishing_result, error_reason="Pub/Sub unavailable"
    )

    assert result is True

    # Check message structure
    messages = dlq_sim_pubsub.get_dlq_messages()
    assert len(messages) == 1

    message = messages[0]
    assert message["error_type"] == "publishing_error"


def test_send_to_dlq_real_pubsub_success(dlq_real_pubsub, mock_publisher_client):
    """Test sending to DLQ with real Pub/Sub."""
    message_data = {"error_type": "test_error", "timestamp": time.time(), "message_id": "test_123"}

    # Mock successful publish
    mock_future = MagicMock()
    mock_future.result.return_value = "dlq_message_id_123"
    mock_publisher_client.publish.return_value = mock_future

    result = dlq_real_pubsub._send_to_dlq(message_data, "test_error")

    assert result is True

    # Verify publish was called
    mock_publisher_client.publish.assert_called_once()
    call_args = mock_publisher_client.publish.call_args
    assert call_args[0][0] == mock_publisher_client.topic_path.return_value


def test_send_to_dlq_retry_logic(dlq_real_pubsub, mock_publisher_client):
    """Test retry logic for DLQ sending."""
    message_data = {"error_type": "test_error", "timestamp": time.time(), "message_id": "test_123"}

    # Mock publish to fail twice then succeed
    call_count = 0

    def publish_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        mock_future = MagicMock()
        if call_count <= 2:
            mock_future.result.side_effect = GoogleCloudError("Temporary error")
        else:
            mock_future.result.return_value = "dlq_message_id_123"
        return mock_future

    mock_publisher_client.publish.side_effect = publish_side_effect

    with patch("time.sleep"):  # Speed up test by mocking sleep
        result = dlq_real_pubsub._send_to_dlq(message_data, "test_error")

    assert result is True
    assert call_count == 3  # Failed twice, succeeded on third try


def test_send_to_dlq_max_retries_exceeded(dlq_real_pubsub, mock_publisher_client):
    """Test DLQ sending when max retries are exceeded."""
    message_data = {"error_type": "test_error", "timestamp": time.time(), "message_id": "test_123"}

    # Mock publish to always fail
    mock_future = MagicMock()
    mock_future.result.side_effect = GoogleCloudError("Persistent error")
    mock_publisher_client.publish.return_value = mock_future

    with patch("time.sleep"):  # Speed up test by mocking sleep
        result = dlq_real_pubsub._send_to_dlq(message_data, "test_error")

    assert result is False
    assert mock_publisher_client.publish.call_count == 3  # max_retries + 1


def test_send_to_dlq_unexpected_error(dlq_real_pubsub, mock_publisher_client):
    """Test DLQ sending with unexpected error."""
    message_data = {"error_type": "test_error", "timestamp": time.time(), "message_id": "test_123"}

    # Mock publish to raise unexpected error
    mock_publisher_client.publish.side_effect = ValueError("Unexpected error")

    result = dlq_real_pubsub._send_to_dlq(message_data, "test_error")

    assert result is False


def test_get_dlq_messages(dlq_sim_pubsub):
    """Test getting DLQ messages."""
    # Send some test errors
    dlq_sim_pubsub.send_processing_error({"test": "data1"}, "Error 1")
    dlq_sim_pubsub.send_file_error("/tmp/file1", "bucket", "obj1", "Error 2")
    dlq_sim_pubsub.send_validation_errors([{"row": 1}], "/tmp/file2", "transaction")

    # Get messages with limit
    messages = dlq_sim_pubsub.get_dlq_messages(limit=2)
    assert len(messages) == 2

    # Get all messages
    all_messages = dlq_sim_pubsub.get_dlq_messages(limit=100)
    assert len(all_messages) == 3


def test_clear_dlq_history(dlq_sim_pubsub):
    """Test clearing DLQ history."""
    # Send some test errors
    dlq_sim_pubsub.send_processing_error({"test": "data"}, "Error")
    assert len(dlq_sim_pubsub.dlq_messages) == 1

    # Clear history
    dlq_sim_pubsub.clear_dlq_history()
    assert len(dlq_sim_pubsub.dlq_messages) == 0


def test_get_dlq_stats_empty(dlq_sim_pubsub):
    """Test getting DLQ stats when empty."""
    stats = dlq_sim_pubsub.get_dlq_stats()

    assert stats["total_messages"] == 0
    assert stats["error_types"] == {}
    assert stats["recent_count"] == 0


def test_get_dlq_stats_with_data(dlq_sim_pubsub):
    """Test getting DLQ stats with data."""
    # Send different types of errors
    dlq_sim_pubsub.send_processing_error({"test": "data1"}, "Error 1")
    dlq_sim_pubsub.send_file_error("/tmp/file1", "bucket", "obj1", "Error 2")
    dlq_sim_pubsub.send_processing_error({"test": "data2"}, "Error 3")

    stats = dlq_sim_pubsub.get_dlq_stats()

    assert stats["total_messages"] == 3
    assert stats["error_types"]["processing_error"] == 2
    assert stats["error_types"]["file_error"] == 1
    assert stats["recent_count"] == 3  # All should be recent
    assert stats["dlq_topic"] == "test-dlq"


def test_send_processing_error_exception_handling(dlq_sim_pubsub):
    """Test exception handling in send_processing_error."""
    with patch.object(dlq_sim_pubsub, "_send_to_dlq", side_effect=Exception("Test error")):
        result = dlq_sim_pubsub.send_processing_error({"test": "data"}, "Error")
        assert result is False


def test_send_file_error_exception_handling(dlq_sim_pubsub):
    """Test exception handling in send_file_error."""
    with patch.object(dlq_sim_pubsub, "_send_to_dlq", side_effect=Exception("Test error")):
        result = dlq_sim_pubsub.send_file_error("/tmp/file", "bucket", "obj", "Error")
        assert result is False


def test_send_validation_errors_exception_handling(dlq_sim_pubsub):
    """Test exception handling in send_validation_errors."""
    with patch.object(dlq_sim_pubsub, "_send_to_dlq", side_effect=Exception("Test error")):
        result = dlq_sim_pubsub.send_validation_errors([{"row": 1}], "/tmp/file", "transaction")
        assert result is False


def test_send_publishing_error_exception_handling(dlq_sim_pubsub):
    """Test exception handling in send_publishing_error."""
    with patch.object(dlq_sim_pubsub, "_send_to_dlq", side_effect=Exception("Test error")):
        result = dlq_sim_pubsub.send_publishing_error({"data": []}, {"failed_count": 1}, "Error")
        assert result is False


def test_dlq_message_structure(dlq_sim_pubsub):
    """Test the structure of DLQ messages."""
    dlq_sim_pubsub.send_processing_error(
        original_data={"test": "data"},
        error_reason="Processing failed",
        error_details={"exception": "ValueError"},
        source_file="/tmp/test.csv",
    )

    messages = dlq_sim_pubsub.get_dlq_messages()
    assert len(messages) == 1

    message = messages[0]
    assert message["error_type"] == "processing_error"
    assert "message_id" in message
    assert "sent_at" in message
    assert message["topic"] == "test-dlq"
    assert "attributes" in message

    # Check attributes
    attrs = message["attributes"]
    assert attrs["error_type"] == "processing_error"
    assert attrs["service"] == "batch_ingestion"
    assert "message_id" in attrs
    assert "timestamp" in attrs
