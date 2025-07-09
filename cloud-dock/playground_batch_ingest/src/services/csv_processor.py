"""
CSV processing service for parsing and transforming CSV data into JSON format.
"""

import csv
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import pandas as pd
from jsonschema import validate, ValidationError, FormatChecker

from playground_batch_ingest.src.schemas.transaction_schema import TRANSACTION_SCHEMA, TRANSACTION_CSV_HEADERS
from playground_batch_ingest.src.schemas.shop_schema import SHOP_SCHEMA, SHOP_CSV_HEADERS
from playground_batch_ingest.src.schemas.product_schema import PRODUCT_SCHEMA, PRODUCT_CSV_HEADERS


logger = logging.getLogger(__name__)


class CSVProcessor:
    """Processes CSV files and converts them to validated JSON format."""

    def __init__(self, batch_size: int = 1000, encoding: str = "utf-8"):
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

    def process_csv_file(self, file_path: str, data_type: str = None) -> Dict[str, Any]:
        """
        Process a CSV file and return structured data.

        Args:
            file_path: Path to the CSV file
            data_type: Type of data (transaction, shop, product) - auto-detected if None

        Returns:
            Dictionary with processing results
        """
        try:
            # Auto-detect data type if not provided
            if data_type is None:
                data_type = self._detect_data_type(file_path)

            if data_type not in self.schema_mappings:
                raise ValueError(f"Unsupported data type: {data_type}")

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

                batch_data, batch_errors = self._process_batch(batch_df, schema_info, batch_start)

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
                "data_type": data_type,
                "total_rows": 0,
                "processed_rows": 0,
                "error_count": 1,
                "data": [],
                "errors": [{"row": 0, "error": str(e)}],
                "file_path": file_path,
            }

    def _process_batch(
        self, batch_df: pd.DataFrame, schema_info: Dict[str, Any], batch_offset: int
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process a batch of CSV rows."""
        batch_data = []
        batch_errors = []

        transformer = schema_info["transformer"]
        schema = schema_info["schema"]

        for idx, row in batch_df.iterrows():
            try:
                # Transform CSV row to JSON
                json_data = transformer(row.to_dict())

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
        Auto-detect data type based on CSV headers.

        Args:
            file_path: Path to the CSV file

        Returns:
            Detected data type (transaction, shop, product)
        """
        try:
            # Read first few rows to check headers
            df = pd.read_csv(file_path, nrows=1, encoding=self.encoding)
            headers = set(df.columns.str.lower())

            # Check for transaction headers
            transaction_headers = set(h.lower() for h in TRANSACTION_CSV_HEADERS)
            if len(headers.intersection(transaction_headers)) > 5:
                return "transaction"

            # Check for shop headers
            shop_headers = set(h.lower() for h in SHOP_CSV_HEADERS)
            if len(headers.intersection(shop_headers)) > 5:
                return "shop"

            # Check for product headers
            product_headers = set(h.lower() for h in PRODUCT_CSV_HEADERS)
            if len(headers.intersection(product_headers)) > 5:
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
