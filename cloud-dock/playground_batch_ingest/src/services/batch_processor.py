"""
Main batch processor that orchestrates file processing, validation, and publishing.
"""

import json
import logging
import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from playground_batch_ingest.src.services.gcs_handler import GCSFileHandler
from playground_batch_ingest.src.services.csv_processor import CSVProcessor
from playground_batch_ingest.src.services.publisher import BatchPublisher
from playground_batch_ingest.src.services.dlq import DeadLetterQueue


logger = logging.getLogger(__name__)


class BatchProcessor:
    """Main batch processing orchestrator."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # Initialise services
        self.gcs_handler = GCSFileHandler(
            temp_dir=config.get("temp_download_path", "/tmp/batch_files"),
            max_file_size_mb=config.get("max_file_size_mb", 100),
        )

        self.csv_processor = CSVProcessor(
            batch_size=config.get("batch_size", 1000),
            encoding=config.get("default_encoding", "utf-8"),
        )

        self.publisher = BatchPublisher(
            project_id=config.get("project_id"),
            topic_name=config.get("pubsub_topic"),
            use_real_pubsub=config.get("use_real_pubsub", True),
            max_retries=config.get("max_retry_attempts", 3),
        )

        self.dlq = DeadLetterQueue(
            project_id=config.get("project_id"),
            dlq_topic=config.get("dlq_topic"),
            use_real_pubsub=config.get("use_real_pubsub", True),
            max_retries=config.get("max_retry_attempts", 3),
        )

        self.max_workers = config.get("max_workers", 4)
        self.processing_timeout = config.get("processing_timeout", 300)
        self.supported_file_types = config.get("supported_file_types", ["csv"])

    def process_gcs_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a GCS file event from Pub/Sub.

        Args:
            event_data: Event data containing file information

        Returns:
            Processing result summary
        """
        start_time = time.time()

        try:
            # Extract file information from event
            bucket_name = event_data.get("bucketId") or event_data.get("bucket")
            object_name = event_data.get("objectId") or event_data.get("name")

            if not bucket_name or not object_name:
                error_msg = "Missing bucket or object name in event data"
                logger.error(f"{error_msg}: {event_data}")
                return {"success": False, "error": error_msg}

            logger.info(f"Processing GCS event for {bucket_name}/{object_name}")

            # Check if file type is supported
            if not self.gcs_handler.is_supported_file_type(object_name, self.supported_file_types):
                error_msg = f"Unsupported file type: {object_name}"
                logger.warning(error_msg)
                return {"success": False, "error": error_msg, "skipped": True}

            # Process the file
            result = self.process_file(bucket_name, object_name)

            # Add timing information
            result["processing_time"] = time.time() - start_time
            result["bucket_name"] = bucket_name
            result["object_name"] = object_name

            return result

        except Exception as e:
            error_msg = f"Error processing GCS event: {e}"
            logger.error(error_msg)

            # Send error to DLQ
            self.dlq.send_processing_error(
                original_data=event_data,
                error_reason=error_msg,
                error_details={"exception": str(e)},
            )

            return {
                "success": False,
                "error": error_msg,
                "processing_time": time.time() - start_time,
            }

    def process_file(self, bucket_name: str, object_name: str) -> Dict[str, Any]:
        """
        Process a single file from GCS.

        Args:
            bucket_name: GCS bucket name
            object_name: GCS object name

        Returns:
            Processing result summary
        """
        local_file_path = None

        try:
            # Step 1: Download file from GCS
            logger.info(f"Downloading {bucket_name}/{object_name}")
            local_file_path = self.gcs_handler.download_file(bucket_name, object_name)

            if not local_file_path:
                error_msg = f"Failed to download file {bucket_name}/{object_name}"
                self.dlq.send_file_error(
                    file_path=None,
                    bucket_name=bucket_name,
                    object_name=object_name,
                    error_reason=error_msg,
                )
                return {"success": False, "error": error_msg}

            # Step 2: Get file metadata
            file_metadata = self.gcs_handler.get_file_metadata(bucket_name, object_name)

            # Step 3: Process CSV file
            logger.info(f"Processing CSV file {local_file_path}")
            processed_data = self.csv_processor.process_csv_file(local_file_path)

            # Step 4: Handle validation errors
            if processed_data.get("errors"):
                logger.warning(f"Found {len(processed_data['errors'])} validation errors in {object_name}")

                # Send validation errors to DLQ
                self.dlq.send_validation_errors(
                    validation_errors=processed_data["errors"],
                    source_file=f"{bucket_name}/{object_name}",
                    data_type=processed_data.get("data_type", "unknown"),
                )

            # Step 5: Publish processed data if any valid records exist
            publishing_result = {"success": True, "published_count": 0, "failed_count": 0}

            if processed_data.get("data"):
                logger.info(f"Publishing {len(processed_data['data'])} records")
                publishing_result = self.publisher.publish_batch_data(processed_data)

                # Handle publishing failures
                if not publishing_result.get("success") or publishing_result.get("failed_count", 0) > 0:
                    self.dlq.send_publishing_error(
                        processed_data=processed_data,
                        publishing_result=publishing_result,
                        error_reason="Publishing failed for some or all records",
                    )

            # Step 6: Cleanup local file
            if local_file_path:
                self.gcs_handler.cleanup_file(local_file_path)

            # Prepare result summary
            result = {
                "success": True,
                "file_metadata": file_metadata,
                "processing_summary": {
                    "data_type": processed_data.get("data_type"),
                    "total_rows": processed_data.get("total_rows", 0),
                    "processed_rows": processed_data.get("processed_rows", 0),
                    "error_count": processed_data.get("error_count", 0),
                },
                "publishing_summary": {
                    "published_count": publishing_result.get("published_count", 0),
                    "failed_count": publishing_result.get("failed_count", 0),
                    "message_ids": publishing_result.get("message_ids", []),
                },
            }

            logger.info(
                f"Completed processing {object_name}: "
                f"{result['processing_summary']['processed_rows']} processed, "
                f"{result['publishing_summary']['published_count']} published"
            )

            return result

        except Exception as e:
            error_msg = f"Error processing file {bucket_name}/{object_name}: {e}"
            logger.error(error_msg)

            # Cleanup local file on error
            if local_file_path:
                self.gcs_handler.cleanup_file(local_file_path)

            # Send error to DLQ
            self.dlq.send_file_error(
                file_path=local_file_path,
                bucket_name=bucket_name,
                object_name=object_name,
                error_reason=error_msg,
                error_details={"exception": str(e)},
            )

            return {"success": False, "error": error_msg}

    def process_multiple_files(self, file_list: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Process multiple files concurrently.

        Args:
            file_list: List of dicts with 'bucket_name' and 'object_name' keys

        Returns:
            Summary of all processing results
        """
        if not file_list:
            return {"success": True, "processed_files": 0, "results": []}

        logger.info(f"Processing {len(file_list)} files concurrently")

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self.process_file, file_info["bucket_name"], file_info["object_name"]): file_info
                for file_info in file_list
            }

            # Collect results as they complete
            for future in as_completed(future_to_file, timeout=self.processing_timeout):
                file_info = future_to_file[future]
                try:
                    result = future.result()
                    result["file_info"] = file_info
                    results.append(result)

                except Exception as e:
                    error_result = {
                        "success": False,
                        "error": f"Processing timeout or error: {e}",
                        "file_info": file_info,
                    }
                    results.append(error_result)
                    logger.error(f"Error processing {file_info}: {e}")

        # Summary statistics
        successful = sum(1 for r in results if r.get("success"))
        total_processed = sum(r.get("processing_summary", {}).get("processed_rows", 0) for r in results)
        total_published = sum(r.get("publishing_summary", {}).get("published_count", 0) for r in results)

        return {
            "success": successful == len(file_list),
            "processed_files": len(file_list),
            "successful_files": successful,
            "failed_files": len(file_list) - successful,
            "total_records_processed": total_processed,
            "total_records_published": total_published,
            "results": results,
        }

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics from all services."""
        return {
            "publisher_stats": self.publisher.get_topic_info(),
            "dlq_stats": self.dlq.get_dlq_stats(),
            "recent_published": len(self.publisher.get_published_messages()),
            "recent_dlq": len(self.dlq.get_dlq_messages()),
        }

    def cleanup_temp_files(self) -> None:
        """Clean up temporary files."""
        self.gcs_handler.cleanup_temp_directory()
