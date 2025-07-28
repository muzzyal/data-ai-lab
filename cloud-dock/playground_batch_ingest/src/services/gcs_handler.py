"""
Google Cloud Storage file handler for downloading and managing batch files.
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError, NotFound

logger = logging.getLogger(__name__)


class GCSFileHandler:
    """Handles file operations with Google Cloud Storage."""

    def __init__(self, temp_dir: str = None, max_file_size_mb: int = 100):
        self.client = storage.Client()
        if temp_dir is None:
            temp_dir = os.path.join(tempfile.gettempdir(), "batch_files")
        self.temp_dir = Path(temp_dir)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024

        # Ensure temp directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def download_file(self, bucket_name: str, object_name: str) -> Optional[str]:
        """
        Download a file from GCS to local temp directory.

        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object in the bucket

        Returns:
            Local file path if successful, None otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)

            # Check if file exists
            if not blob.exists():
                logger.error(f"File {object_name} not found in bucket {bucket_name}")
                return None

            # Check file size
            blob.reload()  # Ensure we have the latest metadata
            if blob.size > self.max_file_size_bytes:
                logger.error(
                    f"File {object_name} size ({blob.size} bytes) exceeds limit " f"({self.max_file_size_bytes} bytes)"
                )
                return None

            # Create local file path
            safe_filename = self._sanitise_filename(object_name)
            local_path = self.temp_dir / safe_filename

            # Download the file
            logger.info(f"Downloading {bucket_name}/{object_name} to {local_path}")
            blob.download_to_filename(str(local_path))

            logger.info(f"Successfully downloaded {object_name} ({blob.size} bytes)")
            return str(local_path)

        except NotFound:
            logger.error(f"File {object_name} not found in bucket {bucket_name}")
            return None
        except GoogleCloudError as e:
            logger.error(f"GCS error downloading {object_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading {object_name}: {e}")
            return None

    def get_file_metadata(self, bucket_name: str, object_name: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a file in GCS.

        Args:
            bucket_name: Name of the GCS bucket
            object_name: Name of the object in the bucket

        Returns:
            Dictionary with file metadata if successful, None otherwise
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)

            if not blob.exists():
                logger.error(f"File {object_name} not found in bucket {bucket_name}")
                return None

            blob.reload()  # Ensure we have the latest metadata

            return {
                "name": blob.name,
                "bucket": bucket_name,
                "size": blob.size,
                "content_type": blob.content_type,
                "created": blob.time_created.isoformat() if blob.time_created else None,
                "updated": blob.updated.isoformat() if blob.updated else None,
                "md5_hash": blob.md5_hash,
                "crc32c": blob.crc32c,
                "etag": blob.etag,
                "generation": blob.generation,
                "custom_metadata": dict(blob.metadata) if blob.metadata else {},
            }

        except NotFound:
            logger.error(f"File {object_name} not found in bucket {bucket_name}")
            return None
        except GoogleCloudError as e:
            logger.error(f"GCS error getting metadata for {object_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting metadata for {object_name}: {e}")
            return None

    def cleanup_file(self, local_path: str) -> bool:
        """
        Clean up a local file after processing.

        Args:
            local_path: Path to the local file to clean up

        Returns:
            True if successful, False otherwise
        """
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info(f"Cleaned up local file: {local_path}")
                return True
            else:
                logger.warning(f"File not found for cleanup: {local_path}")
                return True  # Consider this success since file doesn't exist

        except Exception as e:
            logger.error(f"Error cleaning up file {local_path}: {e}")
            return False

    def cleanup_temp_directory(self) -> None:
        """Clean up all files in the temp directory."""
        try:
            for file_path in self.temp_dir.glob("*"):
                if file_path.is_file():
                    file_path.unlink()
                    logger.debug(f"Cleaned up temp file: {file_path}")
            logger.info("Temp directory cleanup completed")

        except Exception as e:
            logger.error(f"Error cleaning up temp directory: {e}")

    def _sanitise_filename(self, filename: str) -> str:
        """
        Sanitise filename for local filesystem.

        Args:
            filename: Original filename from GCS

        Returns:
            Sanitised filename safe for local filesystem
        """
        # Replace path separators and other problematic characters
        safe_name = filename.replace("/", "_").replace("\\", "_")
        safe_name = "".join(c for c in safe_name if c.isalnum() or c in "._-")

        # Ensure filename is not empty and not too long
        if not safe_name:
            safe_name = "downloaded_file"
        if len(safe_name) > 255:
            name_part, ext = os.path.splitext(safe_name)
            safe_name = name_part[:250] + ext

        return safe_name

    def is_supported_file_type(self, filename: str, supported_types: list = None) -> bool:
        """
        Check if file type is supported for processing.

        Args:
            filename: Name of the file
            supported_types: List of supported file extensions (default: ['csv'])

        Returns:
            True if file type is supported, False otherwise
        """
        if supported_types is None:
            supported_types = ["csv"]

        file_extension = Path(filename).suffix.lower().lstrip(".")
        return file_extension in [ext.lower() for ext in supported_types]
