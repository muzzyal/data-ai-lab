"""
Tests for Google Cloud Storage file handler.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from google.cloud.exceptions import NotFound, GoogleCloudError
from playground_batch_ingest.src.services.gcs_handler import GCSFileHandler


@pytest.fixture
def temp_dir():
    """Create temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def mock_storage_client():
    """Mock Google Cloud Storage client."""
    with patch("playground_batch_ingest.src.services.gcs_handler.storage.Client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def gcs_handler(temp_dir, mock_storage_client):
    """Create GCS handler with temporary directory."""
    return GCSFileHandler(temp_dir=temp_dir, max_file_size_mb=1)


def test_gcs_handler_initialisation(temp_dir):
    """Test GCS handler initialisation."""
    handler = GCSFileHandler(temp_dir=temp_dir, max_file_size_mb=5)

    assert handler.temp_dir == Path(temp_dir)
    assert handler.max_file_size_bytes == 5 * 1024 * 1024
    assert handler.temp_dir.exists()


def test_download_file_success(gcs_handler, mock_storage_client):
    """Test successful file download."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"

    # Mock bucket and blob
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.size = 1024
    mock_blob.download_to_filename = MagicMock()

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = gcs_handler.download_file(bucket_name, object_name)

    assert result is not None
    assert result.endswith("test-file.csv")

    # Verify calls
    mock_storage_client.bucket.assert_called_once_with(bucket_name)
    mock_bucket.blob.assert_called_once_with(object_name)
    mock_blob.exists.assert_called_once()
    mock_blob.reload.assert_called_once()
    mock_blob.download_to_filename.assert_called_once()


def test_download_file_not_found(gcs_handler, mock_storage_client):
    """Test file download when file doesn't exist."""
    bucket_name = "test-bucket"
    object_name = "nonexistent.csv"

    # Mock bucket and blob
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = gcs_handler.download_file(bucket_name, object_name)

    assert result is None


def test_download_file_too_large(gcs_handler, mock_storage_client):
    """Test file download when file is too large."""
    bucket_name = "test-bucket"
    object_name = "large-file.csv"

    # Mock bucket and blob
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.size = 10 * 1024 * 1024  # 10MB, larger than 1MB limit

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    result = gcs_handler.download_file(bucket_name, object_name)

    assert result is None


def test_download_file_gcs_error(gcs_handler, mock_storage_client):
    """Test file download with GCS error."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"

    mock_storage_client.bucket.side_effect = GoogleCloudError("GCS Error")

    result = gcs_handler.download_file(bucket_name, object_name)

    assert result is None


def test_download_file_not_found_exception(gcs_handler, mock_storage_client):
    """Test file download with NotFound exception."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"

    mock_storage_client.bucket.side_effect = NotFound("File not found")

    result = gcs_handler.download_file(bucket_name, object_name)

    assert result is None


def test_get_file_metadata_success(gcs_handler, mock_storage_client):
    """Test successful file metadata retrieval."""
    bucket_name = "test-bucket"
    object_name = "test-file.csv"

    # Mock bucket and blob
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_blob.name = object_name
    mock_blob.size = 1024
    mock_blob.content_type = "text/csv"
    mock_blob.time_created = None
    mock_blob.updated = None
    mock_blob.md5_hash = "abc123"
    mock_blob.crc32c = "def456"
    mock_blob.etag = "etag123"
    mock_blob.generation = 1
    mock_blob.metadata = {"custom": "value"}

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    metadata = gcs_handler.get_file_metadata(bucket_name, object_name)

    assert metadata is not None
    assert metadata["name"] == object_name
    assert metadata["bucket"] == bucket_name
    assert metadata["size"] == 1024
    assert metadata["content_type"] == "text/csv"
    assert metadata["md5_hash"] == "abc123"
    assert metadata["custom_metadata"] == {"custom": "value"}


def test_get_file_metadata_not_found(gcs_handler, mock_storage_client):
    """Test file metadata retrieval when file doesn't exist."""
    bucket_name = "test-bucket"
    object_name = "nonexistent.csv"

    # Mock bucket and blob
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False

    mock_storage_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    metadata = gcs_handler.get_file_metadata(bucket_name, object_name)

    assert metadata is None


def test_cleanup_file_success(gcs_handler, temp_dir):
    """Test successful file cleanup."""
    # Create a test file
    test_file = os.path.join(temp_dir, "test_file.csv")
    with open(test_file, "w") as f:
        f.write("test content")

    assert os.path.exists(test_file)

    result = gcs_handler.cleanup_file(test_file)

    assert result is True
    assert not os.path.exists(test_file)


def test_cleanup_file_not_exists(gcs_handler):
    """Test file cleanup when file doesn't exist."""
    nonexistent_file = "/tmp/nonexistent_file.csv"

    result = gcs_handler.cleanup_file(nonexistent_file)

    assert result is True  # Should return True for non-existent files


def test_cleanup_file_error(gcs_handler):
    """Test file cleanup with permission error."""
    with (
        patch("os.path.exists", return_value=True),
        patch("os.remove", side_effect=PermissionError("Permission denied")),
    ):

        result = gcs_handler.cleanup_file("/tmp/test_file.csv")

        assert result is False


def test_cleanup_temp_directory(gcs_handler, temp_dir):
    """Test cleanup of entire temp directory."""
    # Create test files
    test_file1 = os.path.join(temp_dir, "file1.csv")
    test_file2 = os.path.join(temp_dir, "file2.csv")
    test_dir = os.path.join(temp_dir, "subdir")

    with open(test_file1, "w") as f:
        f.write("content1")
    with open(test_file2, "w") as f:
        f.write("content2")
    os.makedirs(test_dir)

    assert os.path.exists(test_file1)
    assert os.path.exists(test_file2)
    assert os.path.exists(test_dir)

    gcs_handler.cleanup_temp_directory()

    # Files should be deleted, but directories remain
    assert not os.path.exists(test_file1)
    assert not os.path.exists(test_file2)
    assert os.path.exists(test_dir)  # Directory should remain


def test_sanitize_filename(gcs_handler):
    """Test filename sanitization."""
    # Test normal filename
    result = gcs_handler._sanitize_filename("normal_file.csv")
    assert result == "normal_file.csv"

    # Test filename with path separators
    result = gcs_handler._sanitize_filename("path/to/file.csv")
    assert result == "path_to_file.csv"

    # Test filename with special characters
    result = gcs_handler._sanitize_filename("file@#$%.csv")
    assert result == "file.csv"

    # Test empty filename
    result = gcs_handler._sanitize_filename("")
    assert result == "downloaded_file"

    # Test very long filename
    long_name = "a" * 300 + ".csv"
    result = gcs_handler._sanitize_filename(long_name)
    assert len(result) <= 255
    assert result.endswith(".csv")


def test_is_supported_file_type(gcs_handler):
    """Test file type support checking."""
    # Test supported type
    assert gcs_handler.is_supported_file_type("file.csv") is True
    assert gcs_handler.is_supported_file_type("FILE.CSV") is True

    # Test unsupported type
    assert gcs_handler.is_supported_file_type("file.txt") is False
    assert gcs_handler.is_supported_file_type("file.json") is False

    # Test with custom supported types
    assert gcs_handler.is_supported_file_type("file.json", ["json", "csv"]) is True
    assert gcs_handler.is_supported_file_type("file.xml", ["json", "csv"]) is False

    # Test filename without extension
    assert gcs_handler.is_supported_file_type("filename") is False
