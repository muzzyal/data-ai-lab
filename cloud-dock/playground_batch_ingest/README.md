# Batch Ingestion Service

A Cloud Run service for processing CSV files triggered by GCS events via Pub/Sub.

## Features

- **Event-Driven Processing**: Triggered by GCS file creation events via Pub/Sub
- **Multi-Data Type Support**: Handles transaction, shop, and product CSV files
- **Schema Validation**: Comprehensive JSON schema validation with business rules
- **Batch Processing**: Configurable batch sizes for efficient memory usage
- **Error Handling**: Dead Letter Queue for failed processing with detailed error tracking
- **Concurrent Processing**: Multi-threaded file processing with configurable workers
- **Production Ready**: Containerised for Cloud Run with proper scaling and monitoring

## Architecture

This service processes CSV files containing transaction, shop, or product data from Google Cloud Storage when they are uploaded. The processing flow is:

1. **GCS Event** → File uploaded to GCS bucket
2. **Pub/Sub** → Event Arc triggers Pub/Sub message to subscription
3. **Cloud Run** → Service receives Pub/Sub webhook
4. **Download** → File downloaded from GCS to temp directory
5. **Process** → CSV parsed and validated against JSON schemas
6. **Publish** → Valid records published to Pub/Sub topic
7. **DLQ** → Failed records sent to Dead Letter Queue

## Project Structure

```
playground_batch_ingest/
├── src/
│   ├── app.py                    # Flask application factory
│   ├── main.py                   # Application entry point
│   ├── routes/
│   │   └── batch_routes.py       # API route handlers
│   ├── services/
│   │   ├── batch_processor.py    # Main processing orchestrator
│   │   ├── csv_processor.py      # CSV parsing and transformation
│   │   ├── gcs_handler.py        # GCS file operations
│   │   ├── publisher.py          # Pub/Sub publishing service
│   │   └── dlq.py               # Dead Letter Queue service
│   ├── schemas/
│   │   ├── transaction_schema.py # Transaction data schema
│   │   ├── shop_schema.py        # Shop data schema
│   │   └── product_schema.py     # Product data schema
│   └── config_loader/
│       └── loader.py             # Configuration management
├── tests/                        # Comprehensive test suite
├── Dockerfile                    # Container configuration
├── pyproject.toml               # Python dependencies and tools
└── README.md                    # This file
```

## Supported Data Types

### Transaction Data
- CSV headers: `transaction_id`, `customer_id`, `amount`, `currency`, `transaction_type`, `timestamp`, `payment_method_type`, etc.
- Use case: Manual restoration of transaction data when systems are down

### Shop Data
- CSV headers: `shop_id`, `name`, `category`, `status`, `owner_name`, `owner_email`, `address_street`, etc.
- Use case: Bulk shop/merchant onboarding

### Product Data
- CSV headers: `product_id`, `sku`, `name`, `category`, `price_amount`, `price_currency`, `inventory_quantity`, etc.
- Use case: Product catalog updates

## Configuration

Set these environment variables:

```bash
# Google Cloud
GOOGLE_CLOUD_PROJECT=your-project-id
ENVIRONMENT=production

# Pub/Sub
PUBSUB_TOPIC_NAME=batch-processed-data
SUBSCRIPTION_NAME=gcs-file-events
DLQ_TOPIC_NAME=batch-processing-dlq

# Processing
BATCH_SIZE=1000
MAX_WORKERS=4
MAX_FILE_SIZE_MB=100
TEMP_DOWNLOAD_PATH=/tmp/batch_files

# Optional
USE_REAL_PUBSUB=true
LOG_LEVEL=INFO
```

## API Endpoints

### Core Processing
- `POST /api/batch/gcs-event` - Handle GCS events from Pub/Sub
- `POST /api/batch/process-file` - Process single file manually
- `POST /api/batch/process-multiple` - Process multiple files

### Monitoring
- `GET /api/batch/stats` - Processing statistics
- `GET /api/batch/published` - Recently published messages
- `GET /api/batch/dlq` - Dead letter queue messages
- `GET /health` - Health check

### Management
- `POST /api/batch/cleanup` - Clean up temp files

## Local Development

### Setup

Everything is pre-configured in the dev container - just spin it up and you're ready to go.

1. **Open in Dev Container**: Open the repository in VSCode and reopen in container
2. **Navigate to project**: `cd playground_batch_ingest`
3. **Install project dependencies**: `poetry install`
4. **Run the service**: `poetry run python -m src.main`

### Testing

#### VSCode Testing Panel
Use the beaker icon in VSCode - tests are auto-discovered and ready to run.

#### Command Line
```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src --cov-report=html

# Run specific test
poetry run pytest tests/test_csv_processor.py -v
```

### Makefile Commands

From `/workspaces/` (outside dev container for Docker operations):

```bash
# Test coverage
make test-coverage APP=playground_batch_ingest

# Docker build and run
make create-docker-image APP=playground_batch_ingest
make run-docker-image APP=playground_batch_ingest
```

### Development Tools

All pre-installed in dev container:

```bash
# Format and lint
poetry run black src/ tests/
poetry run isort src/ tests/
poetry run flake8 src/ tests/
poetry run mypy src/
```

## CSV Format Examples

### Transaction CSV
```csv
transaction_id,customer_id,amount,currency,transaction_type,timestamp,payment_method_type,payment_method_last_four,payment_method_provider
txn_123,cust_456,99.99,USD,purchase,2024-01-15T10:30:00Z,credit_card,1234,Visa
```

### Shop CSV
```csv
shop_id,name,category,status,owner_name,owner_email,address_street,address_city,address_country,registration_date
shop_123,Tech Store,electronics,active,John Doe,john@example.com,123 Main St,San Francisco,US,2024-01-01T00:00:00Z
```

### Product CSV
```csv
product_id,sku,name,category,price_amount,price_currency,inventory_quantity,shop_id,status,created_date
prod_123,SKU123,Laptop,electronics,999.99,USD,10,shop_123,active,2024-01-01T00:00:00Z
```

## Error Handling

- **Validation Errors**: Invalid CSV rows sent to DLQ with error details
- **File Errors**: File download/processing failures sent to DLQ
- **Publishing Errors**: Failed Pub/Sub publishes sent to DLQ
- **Processing Errors**: Unexpected errors sent to DLQ

## Monitoring

Monitor via:
- Cloud Run logs and metrics
- Pub/Sub topic metrics
- DLQ message counts
- Service endpoints (`/api/batch/stats`)
