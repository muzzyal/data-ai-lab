"""
Tests for Flask batch processing routes.
"""

import base64
import json
from unittest.mock import MagicMock, patch

import pytest
from cloudevents.http import CloudEvent

from playground_batch_ingest.src.app import create_app
from playground_batch_ingest.src.routes.batch_routes import get_batch_processor


@pytest.fixture
def app():
    """Create test Flask application."""
    test_config = {
        "flask_debug": True,
        "log_level": "DEBUG",
        "environment": "test",
        "pubsub_topic": "test-topic",
        "dlq_topic": "test-dlq",
        "use_real_pubsub": False,
    }
    with patch("playground_batch_ingest.src.app.config_loader.get_config", return_value=test_config):
        app = create_app()
        app.config["TESTING"] = True
        yield app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


@pytest.fixture
def mock_batch_processor():
    """Mock batch processor."""
    with patch("playground_batch_ingest.src.routes.batch_routes.get_batch_processor") as mock_get:
        mock_processor = MagicMock()
        mock_get.return_value = mock_processor
        yield mock_processor


def create_cloudevent_request(bucket, object_name):
    """Helper function to create a CloudEvent request for testing."""
    # Create a CloudEvent
    event = CloudEvent(
        {
            "type": "google.cloud.storage.object.v1.finalized",
            "source": f"//storage.googleapis.com/projects/_/buckets/{bucket}",
            "specversion": "1.0",
        },
        {"bucket": bucket, "name": object_name, "generation": "123456"},
    )

    # Convert to HTTP request format
    headers = {
        "ce-specversion": "1.0",
        "ce-type": "google.cloud.storage.object.v1.finalized",
        "ce-source": f"//storage.googleapis.com/projects/_/buckets/{bucket}",
        "ce-id": "test-event-id",
        "content-type": "application/json",
    }

    data = json.dumps({"bucket": bucket, "name": object_name, "generation": "123456"})

    return headers, data


def test_get_batch_processor():
    """Test get_batch_processor function."""
    with (
        patch("playground_batch_ingest.src.routes.batch_routes.config_loader.get_config") as mock_config,
        patch("playground_batch_ingest.src.routes.batch_routes.BatchProcessor") as mock_processor_class,
    ):

        mock_config.return_value = {"test": "config"}

        processor = get_batch_processor()

        mock_config.assert_called_once()
        mock_processor_class.assert_called_once_with({"test": "config"})


def test_handle_gcs_event_success(client, mock_batch_processor):
    """Test successful GCS event handling."""
    # Prepare CloudEvent test data
    headers, data = create_cloudevent_request("test-bucket", "test-file.csv")

    # Mock successful processing
    mock_result = {
        "success": True,
        "processing_summary": {"processed_rows": 10},
        "publishing_summary": {"published_count": 10},
    }
    mock_batch_processor.process_gcs_event.return_value = mock_result

    response = client.post("/api/batch/gcs-event", data=data, headers=headers)

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["success"] is True

    # Verify processor was called with correct data
    expected_event_data = {"bucket": "test-bucket", "name": "test-file.csv", "generation": "123456"}
    mock_batch_processor.process_gcs_event.assert_called_once_with(expected_event_data)


def test_handle_gcs_event_skipped(client, mock_batch_processor):
    """Test GCS event handling with skipped file."""
    headers, data = create_cloudevent_request("test-bucket", "test-file.txt")

    # Mock skipped result
    mock_result = {"success": False, "skipped": True, "error": "Unsupported file type"}
    mock_batch_processor.process_gcs_event.return_value = mock_result

    response = client.post("/api/batch/gcs-event", data=data, headers=headers)

    assert response.status_code == 200  # Skipped is still successful HTTP response
    data = json.loads(response.data)
    assert data["success"] is False
    assert data["skipped"] is True


def test_handle_gcs_event_processing_error(client, mock_batch_processor):
    """Test GCS event handling with processing error."""
    headers, data = create_cloudevent_request("test-bucket", "test-file.csv")

    # Mock processing error
    mock_result = {"success": False, "error": "Processing failed"}
    mock_batch_processor.process_gcs_event.return_value = mock_result

    response = client.post("/api/batch/gcs-event", data=data, headers=headers)

    assert response.status_code == 500
    data = json.loads(response.data)
    assert data["success"] is False


def test_handle_gcs_event_invalid_cloudevent_format(client):
    """Test GCS event handling with invalid CloudEvent format."""
    # Invalid CloudEvent (missing required headers)
    headers = {"content-type": "application/json"}
    data = json.dumps({"invalid": "format"})

    response = client.post("/api/batch/gcs-event", data=data, headers=headers)

    assert response.status_code == 500
    data = json.loads(response.data)
    assert "error" in data


def test_handle_gcs_event_no_data(client):
    """Test GCS event handling with CloudEvent containing no data."""
    # CloudEvent with no data
    headers = {
        "ce-specversion": "1.0",
        "ce-type": "google.cloud.storage.object.v1.finalized",
        "ce-source": "//storage.googleapis.com/projects/_/buckets/test-bucket",
        "ce-id": "test-event-id",
        "content-type": "application/json",
    }

    response = client.post("/api/batch/gcs-event", data="", headers=headers)

    assert response.status_code == 400
    data = json.loads(response.data)
    assert "No data in CloudEvent" in data["error"]


def test_handle_gcs_event_exception(client, mock_batch_processor):
    """Test GCS event handling with exception."""
    headers, data = create_cloudevent_request("test-bucket", "test-file.csv")

    # Mock processor to raise exception
    mock_batch_processor.process_gcs_event.side_effect = Exception("Unexpected error")

    response = client.post("/api/batch/gcs-event", data=data, headers=headers)

    assert response.status_code == 500
    data = json.loads(response.data)
    assert "Unexpected error" in data["error"]


def test_process_single_file_success(client, mock_batch_processor):
    """Test successful single file processing."""
    request_data = {"bucket_name": "test-bucket", "object_name": "test-file.csv"}

    mock_result = {"success": True, "processing_summary": {"processed_rows": 5}}
    mock_batch_processor.process_file.return_value = mock_result

    response = client.post("/api/batch/process-file", data=json.dumps(request_data), content_type="application/json")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["success"] is True

    mock_batch_processor.process_file.assert_called_once_with("test-bucket", "test-file.csv")


def test_process_single_file_missing_data(client):
    """Test single file processing with missing data."""
    request_data = {"bucket_name": "test-bucket"}  # Missing object_name

    response = client.post("/api/batch/process-file", data=json.dumps(request_data), content_type="application/json")

    assert response.status_code == 400
    data = json.loads(response.data)
    assert "Missing bucket_name or object_name" in data["error"]


def test_process_single_file_no_body(client):
    """Test single file processing without request body."""
    # Explicitly send empty JSON with correct headers
    response = client.post(
        "/api/batch/process-file", json=None, headers={"Content-Type": "application/json"}  # Explicit None
    )

    assert response.status_code == 400
    data = response.get_json()
    assert "Missing request body" in data["error"]


def test_process_single_file_error(client, mock_batch_processor):
    """Test single file processing with error."""
    request_data = {"bucket_name": "test-bucket", "object_name": "test-file.csv"}

    mock_result = {"success": False, "error": "File not found"}
    mock_batch_processor.process_file.return_value = mock_result

    response = client.post("/api/batch/process-file", data=json.dumps(request_data), content_type="application/json")

    assert response.status_code == 500
    data = json.loads(response.data)
    assert data["success"] is False


def test_process_multiple_files_success(client, mock_batch_processor):
    """Test successful multiple file processing."""
    request_data = {
        "files": [
            {"bucket_name": "bucket1", "object_name": "file1.csv"},
            {"bucket_name": "bucket2", "object_name": "file2.csv"},
        ]
    }

    mock_result = {"success": True, "processed_files": 2, "successful_files": 2}
    mock_batch_processor.process_multiple_files.return_value = mock_result

    response = client.post(
        "/api/batch/process-multiple", data=json.dumps(request_data), content_type="application/json"
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["success"] is True

    mock_batch_processor.process_multiple_files.assert_called_once_with(request_data["files"])


def test_process_multiple_files_missing_files(client):
    """Test multiple file processing with missing files."""
    request_data = {}  # Missing files

    response = client.post(
        "/api/batch/process-multiple", data=json.dumps(request_data), content_type="application/json"
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert "Missing files list" in data["error"]


def test_process_multiple_files_empty_list(client):
    """Test multiple file processing with empty files list."""
    request_data = {"files": []}

    response = client.post(
        "/api/batch/process-multiple", data=json.dumps(request_data), content_type="application/json"
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert "Files must be a non-empty list" in data["error"]


def test_process_multiple_files_invalid_file_entry(client):
    """Test multiple file processing with invalid file entry."""
    request_data = {
        "files": [
            {"bucket_name": "bucket1", "object_name": "file1.csv"},
            {"bucket_name": "bucket2"},  # Missing object_name
        ]
    }

    response = client.post(
        "/api/batch/process-multiple", data=json.dumps(request_data), content_type="application/json"
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert "Missing bucket_name or object_name in file entry" in data["error"]


def test_process_multiple_files_partial_success(client, mock_batch_processor):
    """Test multiple file processing with partial success."""
    request_data = {
        "files": [
            {"bucket_name": "bucket1", "object_name": "file1.csv"},
            {"bucket_name": "bucket2", "object_name": "file2.csv"},
        ]
    }

    mock_result = {"success": False, "processed_files": 2, "successful_files": 1, "failed_files": 1}  # Partial failure
    mock_batch_processor.process_multiple_files.return_value = mock_result

    response = client.post(
        "/api/batch/process-multiple", data=json.dumps(request_data), content_type="application/json"
    )

    assert response.status_code == 207  # Multi-status
    data = json.loads(response.data)
    assert data["success"] is False


def test_get_processing_stats(client, mock_batch_processor):
    """Test getting processing statistics."""
    mock_stats = {"publisher_stats": {"published_count": 100}, "dlq_stats": {"total_messages": 5}}
    mock_batch_processor.get_processing_stats.return_value = mock_stats

    response = client.get("/api/batch/stats")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["service"] == "batch_ingestion"
    assert data["status"] == "healthy"
    assert data["stats"] == mock_stats


def test_get_published_messages(client, mock_batch_processor):
    """Test getting published messages."""
    mock_messages = [
        {"message_id": "msg1", "data_type": "transaction"},
        {"message_id": "msg2", "data_type": "transaction"},
    ]
    mock_batch_processor.publisher.get_published_messages.return_value = mock_messages

    response = client.get("/api/batch/published?limit=10")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["published_messages"] == mock_messages
    assert data["count"] == 2

    mock_batch_processor.publisher.get_published_messages.assert_called_once_with(limit=10)


def test_get_published_messages_default_limit(client, mock_batch_processor):
    """Test getting published messages with default limit."""
    mock_batch_processor.publisher.get_published_messages.return_value = []

    response = client.get("/api/batch/published")

    assert response.status_code == 200
    mock_batch_processor.publisher.get_published_messages.assert_called_once_with(limit=100)


def test_get_dlq_messages(client, mock_batch_processor):
    """Test getting DLQ messages."""
    mock_messages = [{"message_id": "dlq1", "error_type": "processing_error"}]
    mock_stats = {"total_messages": 1}

    mock_batch_processor.dlq.get_dlq_messages.return_value = mock_messages
    mock_batch_processor.dlq.get_dlq_stats.return_value = mock_stats

    response = client.get("/api/batch/dlq?limit=50")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["dlq_messages"] == mock_messages
    assert data["count"] == 1
    assert data["stats"] == mock_stats

    mock_batch_processor.dlq.get_dlq_messages.assert_called_once_with(limit=50)


def test_cleanup_temp_files(client, mock_batch_processor):
    """Test cleanup temp files endpoint."""
    response = client.post("/api/batch/cleanup")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert "Cleanup completed successfully" in data["message"]

    mock_batch_processor.cleanup_temp_files.assert_called_once()


def test_cleanup_temp_files_error(client, mock_batch_processor):
    """Test cleanup temp files with error."""
    mock_batch_processor.cleanup_temp_files.side_effect = Exception("Cleanup failed")

    response = client.post("/api/batch/cleanup")

    assert response.status_code == 500
    data = json.loads(response.data)
    assert "Cleanup failed" in data["error"]


def test_health_check_endpoint(client):
    """Test batch health check endpoint."""
    response = client.get("/api/batch/health")

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["service"] == "batch_ingestion"
    assert data["status"] == "healthy"
    assert data["version"] == "0.1.0"
