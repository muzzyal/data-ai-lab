"""
Tests for Pub/Sub publisher service.
"""

import pytest
import json
from unittest.mock import patch, MagicMock
from google.cloud.exceptions import GoogleCloudError
from playground_batch_ingest.src.services.publisher import BatchPublisher


@pytest.fixture
def mock_publisher_client():
    """Mock Pub/Sub publisher client."""
    with patch("playground_batch_ingest.src.services.publisher.pubsub_v1.PublisherClient") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def publisher_real_pubsub(mock_publisher_client):
    """Create BatchPublisher with real Pub/Sub enabled."""
    return BatchPublisher(project_id="test-project", topic_name="test-topic", use_real_pubsub=True, max_retries=2)


@pytest.fixture
def publisher_sim_pubsub():
    """Create BatchPublisher with simulated Pub/Sub."""
    return BatchPublisher(project_id="test-project", topic_name="test-topic", use_real_pubsub=False, max_retries=2)


def test_publisher_initialisation_real_pubsub(mock_publisher_client):
    """Test publisher initialisation with real Pub/Sub."""
    publisher = BatchPublisher(project_id="test-project", topic_name="test-topic", use_real_pubsub=True)

    assert publisher.project_id == "test-project"
    assert publisher.topic_name == "test-topic"
    assert publisher.use_real_pubsub is True
    assert publisher.max_retries == 3
    assert publisher.publisher == mock_publisher_client
    assert publisher.topic_path == mock_publisher_client.topic_path.return_value


def test_publisher_initialisation_sim_pubsub():
    """Test publisher initialisation with simulated Pub/Sub."""
    publisher = BatchPublisher(project_id="test-project", topic_name="test-topic", use_real_pubsub=False)

    assert publisher.project_id == "test-project"
    assert publisher.topic_name == "test-topic"
    assert publisher.use_real_pubsub is False
    assert publisher.publisher is None
    assert publisher.topic_path == "projects/test-project/topics/test-topic"


def test_publish_batch_data_empty(publisher_sim_pubsub):
    """Test publishing empty batch data."""
    processed_data = {"data_type": "transaction", "data": []}

    result = publisher_sim_pubsub.publish_batch_data(processed_data)

    assert result["success"] is True
    assert result["published_count"] == 0
    assert result["failed_count"] == 0
    assert result["message_ids"] == []


def test_publish_batch_data_success_simulation(publisher_sim_pubsub):
    """Test successful batch data publishing in simulation mode."""
    processed_data = {
        "data_type": "transaction",
        "data": [{"transaction_id": "txn_001", "amount": 100}, {"transaction_id": "txn_002", "amount": 200}],
        "file_path": "/tmp/test.csv",
    }

    result = publisher_sim_pubsub.publish_batch_data(processed_data)

    assert result["success"] is True
    assert result["published_count"] == 2
    assert result["failed_count"] == 0
    assert len(result["message_ids"]) == 2
    assert result["data_type"] == "transaction"

    # Check that messages were tracked
    messages = publisher_sim_pubsub.get_published_messages()
    assert len(messages) == 2
    assert messages[0]["data_type"] == "transaction"


def test_publish_batch_data_success_real_pubsub(publisher_real_pubsub, mock_publisher_client):
    """Test successful batch data publishing with real Pub/Sub."""
    processed_data = {
        "data_type": "transaction",
        "data": [{"transaction_id": "txn_001", "amount": 100}],
        "file_path": "/tmp/test.csv",
    }

    # Mock successful publish
    mock_future = MagicMock()
    mock_future.result.return_value = "message_id_123"
    mock_publisher_client.publish.return_value = mock_future

    result = publisher_real_pubsub.publish_batch_data(processed_data)

    assert result["success"] is True
    assert result["published_count"] == 1
    assert result["failed_count"] == 0
    assert result["message_ids"] == ["message_id_123"]

    # Verify publish was called
    mock_publisher_client.publish.assert_called_once()
    call_args = mock_publisher_client.publish.call_args
    assert call_args[0][0] == mock_publisher_client.topic_path.return_value


def test_publish_batch_data_with_failures(publisher_real_pubsub, mock_publisher_client):
    """Test batch data publishing with some failures."""
    processed_data = {
        "data_type": "transaction",
        "data": [{"transaction_id": "txn_001", "amount": 100}, {"transaction_id": "txn_002", "amount": 200}],
    }

    # Mock first publish success, second publish failure
    def publish_side_effect(*args, **kwargs):
        mock_future = MagicMock()
        if "txn_001" in args[1].decode():
            mock_future.result.return_value = "message_id_123"
        else:
            mock_future.result.side_effect = GoogleCloudError("Publish failed")
        return mock_future

    mock_publisher_client.publish.side_effect = publish_side_effect

    result = publisher_real_pubsub.publish_batch_data(processed_data)

    assert result["success"] is False
    assert result["published_count"] == 1
    assert result["failed_count"] == 1
    assert len(result["message_ids"]) == 1


def test_publish_single_message_retry_logic(publisher_real_pubsub, mock_publisher_client):
    """Test retry logic for single message publishing."""
    data_item = {"transaction_id": "txn_001", "amount": 100}
    batch_context = {"file_path": "/tmp/test.csv"}

    # Mock publish to fail twice then succeed
    call_count = 0

    def publish_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        mock_future = MagicMock()
        if call_count <= 2:
            mock_future.result.side_effect = GoogleCloudError("Temporary error")
        else:
            mock_future.result.return_value = "message_id_123"
        return mock_future

    mock_publisher_client.publish.side_effect = publish_side_effect

    with patch("time.sleep"):  # Speed up test by mocking sleep
        message_id = publisher_real_pubsub._publish_single_message(data_item, "transaction", 0, batch_context)

    assert message_id == "message_id_123"
    assert call_count == 3  # Failed twice, succeeded on third try


def test_publish_single_message_max_retries_exceeded(publisher_real_pubsub, mock_publisher_client):
    """Test single message publishing when max retries are exceeded."""
    data_item = {"transaction_id": "txn_001", "amount": 100}
    batch_context = {"file_path": "/tmp/test.csv"}

    # Mock publish to always fail
    mock_future = MagicMock()
    mock_future.result.side_effect = GoogleCloudError("Persistent error")
    mock_publisher_client.publish.return_value = mock_future

    with patch("time.sleep"):  # Speed up test by mocking sleep
        message_id = publisher_real_pubsub._publish_single_message(data_item, "transaction", 0, batch_context)

    assert message_id is None
    assert mock_publisher_client.publish.call_count == 3  # max_retries + 1


def test_publish_single_message_unexpected_error(publisher_real_pubsub, mock_publisher_client):
    """Test single message publishing with unexpected error."""
    data_item = {"transaction_id": "txn_001", "amount": 100}
    batch_context = {"file_path": "/tmp/test.csv"}

    # Mock publish to raise unexpected error
    mock_publisher_client.publish.side_effect = ValueError("Unexpected error")

    message_id = publisher_real_pubsub._publish_single_message(data_item, "transaction", 0, batch_context)

    assert message_id is None


def test_get_published_messages(publisher_sim_pubsub):
    """Test getting published messages."""
    # Publish some test data
    processed_data = {"data_type": "transaction", "data": [{"id": "1"}, {"id": "2"}, {"id": "3"}]}

    publisher_sim_pubsub.publish_batch_data(processed_data)

    # Get messages with limit
    messages = publisher_sim_pubsub.get_published_messages(limit=2)
    assert len(messages) == 2

    # Get all messages
    all_messages = publisher_sim_pubsub.get_published_messages(limit=100)
    assert len(all_messages) == 3


def test_clear_message_history(publisher_sim_pubsub):
    """Test clearing message history."""
    # Publish some test data
    processed_data = {"data_type": "transaction", "data": [{"id": "1"}]}

    publisher_sim_pubsub.publish_batch_data(processed_data)
    assert len(publisher_sim_pubsub.published_messages) == 1

    # Clear history
    publisher_sim_pubsub.clear_message_history()
    assert len(publisher_sim_pubsub.published_messages) == 0


def test_get_topic_info(publisher_real_pubsub, mock_publisher_client):
    """Test getting topic information."""
    # Publish some test data first
    processed_data = {"data_type": "transaction", "data": [{"id": "1"}]}

    # Mock successful publish
    mock_future = MagicMock()
    mock_future.result.return_value = "message_id_123"
    mock_publisher_client.publish.return_value = mock_future

    publisher_real_pubsub.publish_batch_data(processed_data)

    topic_info = publisher_real_pubsub.get_topic_info()

    assert topic_info["project_id"] == "test-project"
    assert topic_info["topic_name"] == "test-topic"
    assert topic_info["use_real_pubsub"] is True
    assert topic_info["max_retries"] == 2
    assert topic_info["published_count"] == 1


def test_message_structure(publisher_sim_pubsub):
    """Test the structure of published messages."""
    processed_data = {
        "data_type": "transaction",
        "data": [{"transaction_id": "txn_001", "amount": 100}],
        "file_path": "/tmp/test.csv",
    }

    publisher_sim_pubsub.publish_batch_data(processed_data)

    messages = publisher_sim_pubsub.get_published_messages()
    assert len(messages) == 1

    message = messages[0]
    assert "message_id" in message
    assert message["data_type"] == "transaction"
    assert "published_at" in message
    assert message["topic"] == "test-topic"
    assert "attributes" in message

    # Check attributes
    attrs = message["attributes"]
    assert attrs["data_type"] == "transaction"
    assert attrs["source"] == "batch_ingestion"
    assert "message_id" in attrs


def test_batch_data_error_handling(publisher_sim_pubsub, caplog):
    """Test error handling in publish_batch_data."""
    processed_data = {"data_type": "transaction", "data": [{"id": "1"}]}

    # Mock _publish_single_message to raise exception
    with patch.object(publisher_sim_pubsub, "_publish_single_message", side_effect=Exception("Test error")):
        result = publisher_sim_pubsub.publish_batch_data(processed_data)

        assert result["success"] is False
        assert result["published_count"] == 0
        assert result["failed_count"] == 1

        with caplog.at_level("ERROR"):
            f"Error publishing message" in caplog.text
