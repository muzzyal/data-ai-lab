"""
CSV processing service for parsing and transforming CSV data into JSON format.
"""

import csv
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional, Union, Callable

import pandas as pd
from jsonschema import validate, ValidationError, FormatChecker

from playground_batch_ingest.src.schemas.transaction_schema import TRANSACTION_SCHEMA, TRANSACTION_CSV_HEADERS
from playground_batch_ingest.src.schemas.shop_schema import SHOP_SCHEMA, SHOP_CSV_HEADERS
from playground_batch_ingest.src.schemas.product_schema import PRODUCT_SCHEMA, PRODUCT_CSV_HEADERS


logger = logging.getLogger(__name__)


# Custom exceptions
class CSVProcessorError(Exception):
    """Base exception for CSV processor errors."""

    pass


class InvalidDataTypeError(CSVProcessorError):
    """Raised when an unsupported data type is specified."""

    pass


class FileSizeError(CSVProcessorError):
    """Raised when a file exceeds the maximum allowed size."""

    pass


class FileNotFoundError(CSVProcessorError):
    """Raised when a specified file cannot be found."""

    pass


class CSVValidationError(CSVProcessorError):
    """Raised when data validation fails."""

    pass


# Configuration constants
class CSVProcessorConfig:
    """Configuration constants for CSV processing."""

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_ENCODING = "utf-8"
    MAX_FILE_SIZE_MB = 100
    TRANSACTION_DECIMAL_PLACES = 2
    PRODUCT_DECIMAL_PLACES = 2
    MIN_HEADER_MATCH_COUNT = 5

    # Supported data types
    SUPPORTED_DATA_TYPES = {"transaction", "shop", "product"}

    # File validation
    MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


class CSVProcessor:
    """
    Processes CSV files and converts them to validated JSON format.

    This class handles the parsing, transformation, and validation of CSV files
    containing transaction, shop, or product data. It supports batch processing
    for memory efficiency and provides comprehensive error handling.

    Attributes:
        batch_size (int): Number of rows to process in each batch
        encoding (str): Character encoding for CSV files
        schema_mappings (Dict): Mapping of data types to their schemas and transformers
        unique_headers (Dict): Unique headers for data type detection
    """

    def __init__(
        self,
        batch_size: int = CSVProcessorConfig.DEFAULT_BATCH_SIZE,
        encoding: str = CSVProcessorConfig.DEFAULT_ENCODING,
    ) -> None:
        """
        Initialise the CSV processor.

        Args:
            batch_size: Number of rows to process in each batch (default: 1000)
            encoding: Character encoding for CSV files (default: "utf-8")

        Raises:
            ValueError: If batch_size is not positive or encoding is invalid
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if not isinstance(encoding, str) or not encoding.strip():
            raise ValueError("encoding must be a non-empty string")

        self.batch_size = batch_size
        self.encoding = encoding

        # Schema mappings
        self.schema_mappings = {
            "transaction": {
                "schema": TRANSACTION_SCHEMA,
                "headers": TRANSACTION_CSV_HEADERS,
                "transformer": self._transform_transaction_row,
            },
            "shop": {
                "schema": SHOP_SCHEMA,
                "headers": SHOP_CSV_HEADERS,
                "transformer": self._transform_shop_row,
            },
            "product": {
                "schema": PRODUCT_SCHEMA,
                "headers": PRODUCT_CSV_HEADERS,
                "transformer": self._transform_product_row,
            },
        }

        # Generate unique headers for each data type for better detection
        self.unique_headers = self._generate_unique_headers()

    def process_csv_file(self, file_path: str, data_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a CSV file and return structured data.

        Args:
            file_path: Path to the CSV file
            data_type: Type of data (transaction, shop, product) - auto-detected if None

        Returns:
            Dictionary with processing results containing:
            - data_type: Detected or specified data type
            - total_rows: Total number of rows in the file
            - processed_rows: Number of successfully processed rows
            - error_count: Number of rows with errors
            - data: List of processed and validated records
            - errors: List of error details for failed rows
            - file_path: Path to the processed file

        Raises:
            FileNotFoundError: If the specified file doesn't exist
            FileSizeError: If the file exceeds maximum allowed size
            InvalidDataTypeError: If data_type is not supported
            CSVProcessorError: For other processing errors
        """
        try:
            # Input validation
            if not file_path or not isinstance(file_path, str):
                raise ValueError("file_path must be a non-empty string")

            file_path_obj = Path(file_path)
            if not file_path_obj.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # Check file size
            file_size = file_path_obj.stat().st_size
            if file_size > CSVProcessorConfig.MAX_FILE_SIZE_BYTES:
                raise FileSizeError(
                    f"File size {file_size} bytes exceeds maximum allowed size "
                    f"{CSVProcessorConfig.MAX_FILE_SIZE_BYTES} bytes"
                )

            # Validate data type if provided
            if data_type is not None and data_type not in CSVProcessorConfig.SUPPORTED_DATA_TYPES:
                raise InvalidDataTypeError(f"Unsupported data type: {data_type}")

            # Auto-detect data type if not provided
            if data_type is None:
                data_type = self._detect_data_type(file_path)

            if data_type not in self.schema_mappings:
                raise InvalidDataTypeError(f"Unsupported data type: {data_type}")

            logger.info(f"Processing CSV file {file_path} as {data_type} data")

            # Read and process CSV
            processed_data = []
            errors = []
            total_rows = 0

            # Use pandas for efficient CSV reading
            df = pd.read_csv(file_path, encoding=self.encoding, dtype=str, keep_default_na=False)
            total_rows = len(df)

            logger.info(f"Processing {total_rows} rows from {file_path}")

            # Process in batches
            schema_info = self.schema_mappings[data_type]

            for batch_start in range(0, total_rows, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_rows)
                batch_df = df.iloc[batch_start:batch_end]

                batch_data, batch_errors = self._process_batch(batch_df, schema_info, batch_start, data_type)

                processed_data.extend(batch_data)
                errors.extend(batch_errors)

                logger.info(f"Processed batch {batch_start}-{batch_end}")

            success_count = len(processed_data)
            error_count = len(errors)

            logger.info(f"Completed processing {file_path}: " f"{success_count} successful, {error_count} errors")

            return {
                "data_type": data_type,
                "total_rows": total_rows,
                "processed_rows": success_count,
                "error_count": error_count,
                "data": processed_data,
                "errors": errors,
                "file_path": file_path,
            }

        except Exception as e:
            logger.error(f"Error processing CSV file {file_path}: {e}")
            return {
                "data_type": data_type if data_type is not None else "transaction",
                "total_rows": 0,
                "processed_rows": 0,
                "error_count": 1,
                "data": [],
                "errors": [{"row": 0, "error": str(e)}],
                "file_path": file_path,
            }

    def _process_batch(
        self, batch_df: pd.DataFrame, schema_info: Dict[str, Any], batch_offset: int, data_type: str
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process a batch of CSV rows with transformation and validation.

        Transforms each row from CSV format to JSON, applies data type-specific
        validations (e.g., decimal place limits), and validates against the
        corresponding JSON schema.

        Args:
            batch_df: Pandas DataFrame containing the batch of rows to process
            schema_info: Dictionary containing schema, headers, and transformer function
            batch_offset: Starting row number for this batch (for error reporting)
            data_type: Type of data being processed (transaction, shop, product)

        Returns:
            Tuple containing:
            - List of successfully processed and validated JSON records
            - List of error records with row numbers and error messages

        Note:
            This method handles both transformation errors and schema validation
            errors, ensuring that processing continues even when individual rows fail.
        """
        batch_data = []
        batch_errors = []

        transformer = schema_info["transformer"]
        schema = schema_info["schema"]

        for idx, row in batch_df.iterrows():
            json_data = None
            try:
                # Transform CSV row to JSON
                json_data = transformer(row.to_dict())

                # Safeguard for floating point precision issues and manual validation
                if data_type == "transaction":
                    amount = json_data["amount"]
                    json_data["amount"] = float(amount)

                    if not self._validate_amount_decimals(amount, CSVProcessorConfig.TRANSACTION_DECIMAL_PLACES):
                        raise ValidationError(
                            f"Amount {amount} has more than {CSVProcessorConfig.TRANSACTION_DECIMAL_PLACES} decimal places"
                        )

                elif data_type == "product":
                    price_amount = json_data["price"]["amount"]
                    price_discount_amount = json_data["price"].get("discount_amount", 0)

                    if not self._validate_amount_decimals(price_amount, CSVProcessorConfig.PRODUCT_DECIMAL_PLACES):
                        raise ValidationError(
                            f"Price amount {price_amount} has more than {CSVProcessorConfig.PRODUCT_DECIMAL_PLACES} decimal places"
                        )

                    if price_discount_amount and not self._validate_amount_decimals(
                        price_discount_amount, CSVProcessorConfig.PRODUCT_DECIMAL_PLACES
                    ):
                        raise ValidationError(
                            f"Price discount amount {price_discount_amount} has more than {CSVProcessorConfig.PRODUCT_DECIMAL_PLACES} decimal places"
                        )

                # Validate against schema
                validate(instance=json_data, schema=schema, format_checker=FormatChecker())

                batch_data.append(json_data)

            except ValidationError as e:
                batch_errors.append(
                    {
                        "row": batch_offset + idx + 1,
                        "error": f"Schema validation error: {e.message}",
                        "data": row.to_dict(),
                    }
                )
            except Exception as e:
                batch_errors.append(
                    {
                        "row": batch_offset + idx + 1,
                        "error": f"Processing error: {str(e)}",
                        "data": row.to_dict(),
                    }
                )

        return batch_data, batch_errors

    def _detect_data_type(self, file_path: str) -> str:
        """
        Auto-detect data type based on CSV headers using unique header detection.

        Args:
            file_path: Path to the CSV file

        Returns:
            Detected data type (transaction, shop, product)
        """
        try:
            # Read first few rows to check headers
            df = pd.read_csv(file_path, nrows=1, encoding=self.encoding)
            headers = set(df.columns.str.lower())

            # Check for unique headers first for precise detection
            for data_type, unique_headers_list in self.unique_headers.items():
                unique_headers_set = set(unique_headers_list)
                if len(headers.intersection(unique_headers_set)) > 0:
                    return data_type

            # Fallback to general header matching if no unique headers found
            # Check for transaction headers
            transaction_headers = set(h.lower() for h in TRANSACTION_CSV_HEADERS)
            if len(headers.intersection(transaction_headers)) > CSVProcessorConfig.MIN_HEADER_MATCH_COUNT:
                return "transaction"

            # Check for shop headers
            shop_headers = set(h.lower() for h in SHOP_CSV_HEADERS)
            if len(headers.intersection(shop_headers)) > CSVProcessorConfig.MIN_HEADER_MATCH_COUNT:
                return "shop"

            # Check for product headers
            product_headers = set(h.lower() for h in PRODUCT_CSV_HEADERS)
            if len(headers.intersection(product_headers)) > CSVProcessorConfig.MIN_HEADER_MATCH_COUNT:
                return "product"

            # Default to transaction if cannot determine
            logger.warning(f"Could not auto-detect data type for {file_path}, defaulting to transaction")
            return "transaction"

        except Exception as e:
            logger.error(f"Error detecting data type for {file_path}: {e}")
            return "transaction"

    def _transform_transaction_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Transform transaction CSV row to JSON format."""
        json_data = {
            "transaction_id": row.get("transaction_id", ""),
            "customer_id": row.get("customer_id", ""),
            "amount": float(row.get("amount", 0)) if row.get("amount") else 0.0,
            "currency": row.get("currency", "USD"),
            "transaction_type": row.get("transaction_type", ""),
            "timestamp": row.get("timestamp", ""),
            "payment_method": {
                "type": row.get("payment_method_type", ""),
                "last_four": row.get("payment_method_last_four", ""),
                "provider": row.get("payment_method_provider", ""),
            },
        }

        # Optional fields
        if row.get("merchant_id"):
            json_data["merchant_id"] = row["merchant_id"]
        if row.get("description"):
            json_data["description"] = row["description"]

        # Location data
        if any(row.get(f"location_{field}") for field in ["country", "city", "postal_code"]):
            json_data["location"] = {}
            if row.get("location_country"):
                json_data["location"]["country"] = row["location_country"]
            if row.get("location_city"):
                json_data["location"]["city"] = row["location_city"]
            if row.get("location_postal_code"):
                json_data["location"]["postal_code"] = row["location_postal_code"]

        return json_data

    def _transform_shop_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Transform shop CSV row to JSON format."""
        json_data = {
            "shop_id": row.get("shop_id", ""),
            "name": row.get("name", ""),
            "category": row.get("category", ""),
            "status": row.get("status", ""),
            "owner": {
                "name": row.get("owner_name", ""),
                "email": row.get("owner_email", ""),
            },
            "address": {
                "street": row.get("address_street", ""),
                "city": row.get("address_city", ""),
                "country": row.get("address_country", ""),
            },
            "registration_date": row.get("registration_date", ""),
        }

        # Optional fields
        if row.get("description"):
            json_data["description"] = row["description"]
        if row.get("owner_phone"):
            json_data["owner"]["phone"] = row["owner_phone"]
        if row.get("address_state"):
            json_data["address"]["state"] = row["address_state"]
        if row.get("address_postal_code"):
            json_data["address"]["postal_code"] = row["address_postal_code"]

        # Contact information
        if any(row.get(f"contact_{field}") for field in ["phone", "email", "website"]):
            json_data["contact"] = {}
            if row.get("contact_phone"):
                json_data["contact"]["phone"] = row["contact_phone"]
            if row.get("contact_email"):
                json_data["contact"]["email"] = row["contact_email"]
            if row.get("contact_website"):
                json_data["contact"]["website"] = row["contact_website"]

        # Business hours
        business_hours = {}
        for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
            if row.get(f"business_hours_{day}"):
                business_hours[day] = row[f"business_hours_{day}"]
        if business_hours:
            json_data["business_hours"] = business_hours

        if row.get("last_updated"):
            json_data["last_updated"] = row["last_updated"]

        return json_data

    def _transform_product_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Transform product CSV row to JSON format."""
        json_data = {
            "product_id": row.get("product_id", ""),
            "sku": row.get("sku", ""),
            "name": row.get("name", ""),
            "category": row.get("category", ""),
            "price": {
                "amount": float(row.get("price_amount", 0)) if row.get("price_amount") else 0.0,
                "currency": row.get("price_currency", "USD"),
            },
            "inventory": {
                "quantity": int(row.get("inventory_quantity", 0)) if row.get("inventory_quantity") else 0,
            },
            "shop_id": row.get("shop_id", ""),
            "status": row.get("status", ""),
            "created_date": row.get("created_date", ""),
        }

        # Optional fields
        if row.get("description"):
            json_data["description"] = row["description"]
        if row.get("subcategory"):
            json_data["subcategory"] = row["subcategory"]
        if row.get("brand"):
            json_data["brand"] = row["brand"]

        # Price discounts
        if row.get("price_discount_amount"):
            json_data["price"]["discount_amount"] = float(row["price_discount_amount"])
        if row.get("price_discount_percentage"):
            json_data["price"]["discount_percentage"] = float(row["price_discount_percentage"])

        # Inventory details
        if row.get("inventory_reserved"):
            json_data["inventory"]["reserved"] = int(row["inventory_reserved"])
        if row.get("inventory_warehouse_location"):
            json_data["inventory"]["warehouse_location"] = row["inventory_warehouse_location"]

        # Dimensions
        if any(row.get(f"dimensions_{field}") for field in ["length", "width", "height", "weight"]):
            json_data["dimensions"] = {}
            for field in ["length", "width", "height", "weight"]:
                if row.get(f"dimensions_{field}"):
                    json_data["dimensions"][field] = float(row[f"dimensions_{field}"])

        # Attributes
        attributes = {}
        for attr in ["color", "size", "material", "style"]:
            if row.get(f"attributes_{attr}"):
                attributes[attr] = row[f"attributes_{attr}"]
        if attributes:
            json_data["attributes"] = attributes

        # Images and tags (JSON arrays in CSV)
        if row.get("images"):
            try:
                json_data["images"] = json.loads(row["images"])
            except json.JSONDecodeError:
                json_data["images"] = [row["images"]]

        if row.get("tags"):
            try:
                json_data["tags"] = json.loads(row["tags"])
            except json.JSONDecodeError:
                json_data["tags"] = [tag.strip() for tag in row["tags"].split(",")]

        if row.get("last_updated"):
            json_data["last_updated"] = row["last_updated"]

        return json_data

    def _generate_unique_headers(self) -> Dict[str, List[str]]:
        """
        Generate unique headers for each data type by comparing schemas.
        Returns headers that are unique to each data type for better detection.
        """
        # All data types and their headers
        all_headers = {
            "transaction": set(h.lower() for h in TRANSACTION_CSV_HEADERS),
            "shop": set(h.lower() for h in SHOP_CSV_HEADERS),
            "product": set(h.lower() for h in PRODUCT_CSV_HEADERS),
        }

        unique_headers = {}

        # For each data type, find headers that are unique to it
        for data_type, headers in all_headers.items():
            # Get all other headers (union of all other data types)
            other_headers = set()
            for other_type, other_type_headers in all_headers.items():
                if other_type != data_type:
                    other_headers.update(other_type_headers)

            # Find unique headers for this data type
            unique_headers[data_type] = list(headers - other_headers)

        return unique_headers

    def _validate_amount_decimals(self, amount: Union[float, str, None], valid_decimal_places: int) -> bool:
        """
        Validates amount to ensure it has no more than specified decimal places.

        Handles edge cases including None values, empty strings, scientific notation,
        and various numeric formats.

        Args:
            amount: Amount to validate (float, string, or None)
            valid_decimal_places: Maximum allowed decimal places

        Returns:
            bool: True if valid, False otherwise

        Examples:
            >>> processor._validate_amount_decimals(123.45, 2)
            True
            >>> processor._validate_amount_decimals(123.456, 2)
            False
            >>> processor._validate_amount_decimals("123.45", 2)
            True
            >>> processor._validate_amount_decimals(None, 2)
            True
            >>> processor._validate_amount_decimals("", 2)
            True
        """
        # Handle None and empty values
        if amount is None or amount == "" or amount == 0:
            return True

        try:
            # Convert to float if string
            if isinstance(amount, str):
                amount = float(amount)

            # Handle scientific notation by converting to decimal representation
            amount_str = f"{amount:.{valid_decimal_places + 5}f}".rstrip("0").rstrip(".")

            # Check decimal places
            if "." in amount_str:
                decimal_part = amount_str.split(".")[1]
                # Remove trailing zeros for actual decimal place count
                decimal_part = decimal_part.rstrip("0")
                if len(decimal_part) > valid_decimal_places:
                    return False

        except (ValueError, TypeError, OverflowError):
            # If we can't convert to float, consider it invalid
            return False

        return True
