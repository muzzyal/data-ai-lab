"""
Tests for the main batch processor orchestrator.
"""

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock, call
from playground_batch_ingest.src.services.batch_processor import BatchProcessor


@pytest.fixture
def mock_config():
    """Mock configuration for batch processor."""
    return {
        "project_id": "test-project",
        "pubsub_topic": "test-topic",
        "dlq_topic": "test-dlq",
        "use_real_pubsub": False,
        "temp_download_path": "/tmp/test_batch",
        "max_file_size_mb": 10,
        "batch_size": 100,
        "default_encoding": "utf-8",
        "max_retry_attempts": 2,
        "max_workers": 2,
        "processing_timeout": 60,
        "supported_file_types": ["csv"],
    }


@pytest.fixture
def batch_processor(mock_config):
    """Create batch processor instance for testing."""
    with (
        patch("playground_batch_ingest.src.services.batch_processor.GCSFileHandler"),
        patch("playground_batch_ingest.src.services.batch_processor.CSVProcessor"),
        patch("playground_batch_ingest.src.services.batch_processor.BatchPublisher"),
        patch("playground_batch_ingest.src.services.batch_processor.DeadLetterQueue"),
    ):
        return BatchProcessor(mock_config)


def test_batch_processor_initialisation(mock_config):
    """Test batch processor initialisation."""
    with (
        patch("playground_batch_ingest.src.services.batch_processor.GCSFileHandler") as mock_gcs,
        patch("playground_batch_ingest.src.services.batch_processor.CSVProcessor") as mock_csv,
        patch("playground_batch_ingest.src.services.batch_processor.BatchPublisher") as mock_pub,
        patch("playground_batch_ingest.src.services.batch_processor.DeadLetterQueue") as mock_dlq,
    ):

        processor = BatchProcessor(mock_config)

        # Verify all services are initialised
        mock_gcs.assert_called_once()
        mock_csv.assert_called_once()
        mock_pub.assert_called_once()
        mock_dlq.assert_called_once()

        assert processor.max_workers == 2
        assert processor.processing_timeout == 60


def test_process_gcs_event_success(batch_processor):
    """Test successful GCS event processing."""
    event_data = {"bucketId": "test-bucket", "objectId": "test-file.csv"}

    mock_result = {
        "success": True,
        "processing_summary": {"processed_rows": 10},
        "publishing_summary": {"published_count": 10},
    }

    batch_processor.gcs_handler.is_supported_file_type.return_value = True

    with patch.object(batch_processor, "process_file", return_value=mock_result) as mock_process:
        result = batch_processor.process_gcs_event(event_data)

        assert result["success"] is True
        assert "processing_time" in result
        assert result["bucket_name"] == "test-bucket"
        assert result["object_name"] == "test-file.csv"
        mock_process.assert_called_once_with("test-bucket", "test-file.csv")


def test_process_gcs_event_alternative_event_format(batch_processor):
    """Test GCS event processing with alternative event format."""
    event_data = {"bucket": "test-bucket", "name": "test-file.csv"}

    mock_result = {"success": True}
    batch_processor.gcs_handler.is_supported_file_type.return_value = True

    with patch.object(batch_processor, "process_file", return_value=mock_result):
        result = batch_processor.process_gcs_event(event_data)

        assert result["success"] is True
        assert result["bucket_name"] == "test-bucket"
        assert result["object_name"] == "test-file.csv"


def test_process_gcs_event_missing_data(batch_processor):
    """Test GCS event processing with missing data."""
    event_data = {"bucketId": "test-bucket"}  # Missing objectId

    result = batch_processor.process_gcs_event(event_data)

    assert result["success"] is False
    assert "Missing bucket or object name" in result["error"]


def test_process_gcs_event_unsupported_file_type(batch_processor):
    """Test GCS event processing with unsupported file type."""
    event_data = {"bucketId": "test-bucket", "objectId": "test-file.txt"}

    batch_processor.gcs_handler.is_supported_file_type.return_value = False

    result = batch_processor.process_gcs_event(event_data)

    assert result["success"] is False
    assert result["skipped"] is True
    assert "Unsupported file type" in result["error"]


def test_process_gcs_event_exception(batch_processor):
    """Test GCS event processing with exception."""
    event_data = {"bucketId": "test-bucket", "objectId": "test-file.csv"}

    batch_processor.gcs_handler.is_supported_file_type.side_effect = Exception("Test error")

    result = batch_processor.process_gcs_event(event_data)

    assert result["success"] is False
    assert "Error processing GCS event" in result["error"]
    assert "processing_time" in result

    # Verify DLQ was called
    batch_processor.dlq.send_processing_error.assert_called_once()


def test_process_file_success(batch_processor):
    """Test successful file processing."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"
    local_path = "/tmp/test-file.csv"

    # Mock successful download
    batch_processor.gcs_handler.download_file.return_value = local_path
    batch_processor.gcs_handler.get_file_metadata.return_value = {"size": 1024}

    # Mock successful CSV processing
    processed_data = {
        "data_type": "transaction",
        "total_rows": 10,
        "processed_rows": 9,
        "error_count": 1,
        "data": [{"id": "1"}, {"id": "2"}],
        "errors": [{"row": 5, "error": "validation error"}],
    }
    batch_processor.csv_processor.process_csv_file.return_value = processed_data

    # Mock successful publishing
    publishing_result = {"success": True, "published_count": 2, "failed_count": 0, "message_ids": ["msg1", "msg2"]}
    batch_processor.publisher.publish_batch_data.return_value = publishing_result

    result = batch_processor.process_file(bucket_name, object_name)

    assert result["success"] is True
    assert result["processing_summary"]["processed_rows"] == 9
    assert result["publishing_summary"]["published_count"] == 2

    # Verify cleanup was called
    batch_processor.gcs_handler.cleanup_file.assert_called_once_with(local_path)

    # Verify DLQ was called for validation errors
    batch_processor.dlq.send_validation_errors.assert_called_once()


def test_process_file_download_failure(batch_processor):
    """Test file processing with download failure."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"

    # Mock failed download
    batch_processor.gcs_handler.download_file.return_value = None

    result = batch_processor.process_file(bucket_name, object_name)

    assert result["success"] is False
    assert "Failed to download file" in result["error"]

    # Verify DLQ was called
    batch_processor.dlq.send_file_error.assert_called_once()


def test_process_file_processing_exception(batch_processor):
    """Test file processing with exception."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"
    local_path = "/tmp/test-file.csv"

    batch_processor.gcs_handler.download_file.return_value = local_path
    batch_processor.gcs_handler.get_file_metadata.side_effect = Exception("Test error")

    result = batch_processor.process_file(bucket_name, object_name)

    assert result["success"] is False
    assert "Error processing file" in result["error"]

    # Verify cleanup was called
    batch_processor.gcs_handler.cleanup_file.assert_called_once_with(local_path)

    # Verify DLQ was called
    batch_processor.dlq.send_file_error.assert_called_once()


def test_process_multiple_files_success(batch_processor):
    """Test successful multiple file processing."""
    file_list = [
        {"bucket_name": "bucket1", "object_name": "file1.csv"},
        {"bucket_name": "bucket2", "object_name": "file2.csv"},
    ]

    mock_result = {
        "success": True,
        "processing_summary": {"processed_rows": 5},
        "publishing_summary": {"published_count": 5},
    }

    with patch.object(batch_processor, "process_file", return_value=mock_result) as mock_process:
        result = batch_processor.process_multiple_files(file_list)

        assert result["success"] is True
        assert result["processed_files"] == 2
        assert result["successful_files"] == 2
        assert result["failed_files"] == 0
        assert result["total_records_processed"] == 10
        assert result["total_records_published"] == 10

        # Verify all files were processed
        assert mock_process.call_count == 2


def test_process_multiple_files_empty_list(batch_processor):
    """Test multiple file processing with empty list."""
    result = batch_processor.process_multiple_files([])

    assert result["success"] is True
    assert result["processed_files"] == 0


def test_process_multiple_files_with_failures(batch_processor):
    """Test multiple file processing with some failures."""
    file_list = [
        {"bucket_name": "bucket1", "object_name": "file1.csv"},
        {"bucket_name": "bucket2", "object_name": "file2.csv"},
    ]

    def side_effect(bucket, object_name):
        if object_name == "file1.csv":
            return {
                "success": True,
                "processing_summary": {"processed_rows": 5},
                "publishing_summary": {"published_count": 5},
            }
        else:
            return {"success": False, "error": "Processing failed"}

    with patch.object(batch_processor, "process_file", side_effect=side_effect):
        result = batch_processor.process_multiple_files(file_list)

        assert result["success"] is False
        assert result["successful_files"] == 1
        assert result["failed_files"] == 1


def test_get_processing_stats(batch_processor):
    """Test getting processing statistics."""
    batch_processor.publisher.get_topic_info.return_value = {"topic": "test"}
    batch_processor.dlq.get_dlq_stats.return_value = {"dlq": "stats"}
    batch_processor.publisher.get_published_messages.return_value = ["msg1", "msg2"]
    batch_processor.dlq.get_dlq_messages.return_value = ["dlq1"]

    stats = batch_processor.get_processing_stats()

    assert "publisher_stats" in stats
    assert "dlq_stats" in stats
    assert stats["recent_published"] == 2
    assert stats["recent_dlq"] == 1


def test_cleanup_temp_files(batch_processor):
    """Test cleanup of temporary files."""
    batch_processor.cleanup_temp_files()

    batch_processor.gcs_handler.cleanup_temp_directory.assert_called_once()
