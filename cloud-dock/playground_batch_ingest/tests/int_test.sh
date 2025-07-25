#!/bin/bash
set -e

PROJECT_ID="muz-designed-msc-data-ai-2025"
BUCKET_NAME="muz-designed-msc-data-ai-2025-playground_project"
BQ_DATASET="msc_playground_raw"
BQ_TABLE="playground_project"
ENDPOINT="http://localhost:8080/api/batch/process-file"
TEST_PREFIX="test/"

echo "PROJECT_ID=$PROJECT_ID"
echo "BUCKET_NAME=$BUCKET_NAME"
echo "BQ_DATASET=$BQ_DATASET"
echo "BQ_TABLE=$BQ_TABLE"
echo "ENDPOINT=$ENDPOINT"
echo "TEST_PREFIX=$TEST_PREFIX"
echo "GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS"

# Clean up any previous test data in BigQuery (using test/ prefix in filename attribute)
echo "Cleaning up previous test data..."
bq query --use_legacy_sql=false \
"DELETE FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) LIKE '${TEST_PREFIX}%'"

# Clean up any previous test files in GCS bucket
echo "Cleaning up previous test files in GCS..."
gsutil rm -f "gs://${BUCKET_NAME}/${TEST_PREFIX}*.csv" || echo "No previous test files to clean"

PORT=8080

# Start the batch ingest service
docker run -d --name batch-ingest-test -p 8080:${PORT} -e PORT=${PORT} \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/FILE_NAME.json \
-e GOOGLE_CLOUD_PROJECT="${PROJECT_ID}" \
-e BUCKET_NAME="${BUCKET_NAME}" \
-e BQ_DATASET="${BQ_DATASET}" \
-e BQ_TABLE="${BQ_TABLE}" \
-v "${GOOGLE_APPLICATION_CREDENTIALS}:/tmp/keys/FILE_NAME.json:ro" batch-ingest

# Wait for the service to start
sleep 5

echo "Service started"

# Upload test CSV files to GCS bucket with test/ prefix
echo "Uploading test CSV files to GCS..."
gsutil cp test_csvs/products.csv "gs://${BUCKET_NAME}/${TEST_PREFIX}products.csv"
gsutil cp test_csvs/shops.csv "gs://${BUCKET_NAME}/${TEST_PREFIX}shops.csv"
gsutil cp test_csvs/transactions.csv "gs://${BUCKET_NAME}/${TEST_PREFIX}transactions.csv"

echo "Test files uploaded to GCS"

# Test products.csv processing
echo "Testing products.csv processing..."
RESPONSE=$(curl -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{\"bucket_name\": \"${BUCKET_NAME}\", \"object_name\": \"${TEST_PREFIX}products.csv\"}")

echo "Products response: $RESPONSE"

# Test shops.csv processing
echo "Testing shops.csv processing..."
RESPONSE=$(curl -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{\"bucket_name\": \"${BUCKET_NAME}\", \"object_name\": \"${TEST_PREFIX}shops.csv\"}")

echo "Shops response: $RESPONSE"

# Test transactions.csv processing
echo "Testing transactions.csv processing..."
RESPONSE=$(curl -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -d "{\"bucket_name\": \"${BUCKET_NAME}\", \"object_name\": \"${TEST_PREFIX}transactions.csv\"}")

echo "Transactions response: $RESPONSE"

# Show container logs
docker logs batch-ingest-test

# Stop the Docker container
docker stop batch-ingest-test
docker container rm batch-ingest-test

# Wait for BigQuery ingestion via Pub/Sub
echo "Waiting for BigQuery ingestion..."
sleep 15

# Validate data in BigQuery - check for test files using filename attribute
echo "Validating data in BigQuery..."

# Count total rows with test prefix in filename attribute
TOTAL_ROWS=$(bq query --format=json --use_legacy_sql=false \
"SELECT count(*) as cnt FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) LIKE '${TEST_PREFIX}%'" 2>&1 | sed '/^WARNING/d' | jq .[0].cnt)

TOTAL_ROWS_INT=$(bc <<< "$TOTAL_ROWS")

echo "Found ${TOTAL_ROWS_INT} total rows from test files in BigQuery"

# Count products rows
PRODUCTS_ROWS=$(bq query --format=json --use_legacy_sql=false \
"SELECT count(*) as cnt FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) = '${TEST_PREFIX}products.csv'" 2>&1 | sed '/^WARNING/d' | jq .[0].cnt)

PRODUCTS_ROWS_INT=$(bc <<< "$PRODUCTS_ROWS")

echo "Found ${PRODUCTS_ROWS_INT} products rows in BigQuery"

# Count shops rows
SHOPS_ROWS=$(bq query --format=json --use_legacy_sql=false \
"SELECT count(*) as cnt FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) = '${TEST_PREFIX}shops.csv'" 2>&1 | sed '/^WARNING/d' | jq .[0].cnt)

SHOPS_ROWS_INT=$(bc <<< "$SHOPS_ROWS")

echo "Found ${SHOPS_ROWS_INT} shops rows in BigQuery"

# Count transactions rows
TRANSACTIONS_ROWS=$(bq query --format=json --use_legacy_sql=false \
"SELECT count(*) as cnt FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) = '${TEST_PREFIX}transactions.csv'" 2>&1 | sed '/^WARNING/d' | jq .[0].cnt)

TRANSACTIONS_ROWS_INT=$(bc <<< "$TRANSACTIONS_ROWS")

echo "Found ${TRANSACTIONS_ROWS_INT} transactions rows in BigQuery"

# Validate we have data from all test files
if [ "$TOTAL_ROWS_INT" -eq 0 ]; then
  echo "ERROR: No test data found in BigQuery table"
  exit 1
fi

if [ "$PRODUCTS_ROWS_INT" -eq 0 ]; then
  echo "ERROR: No products data found in BigQuery table"
  exit 1
fi

if [ "$SHOPS_ROWS_INT" -eq 0 ]; then
  echo "ERROR: No shops data found in BigQuery table"
  exit 1
fi

if [ "$TRANSACTIONS_ROWS_INT" -eq 0 ]; then
  echo "ERROR: No transactions data found in BigQuery table"
  exit 1
fi

echo "Data validation successful!"

# Clean up test data from BigQuery
echo "Cleaning up test data from BigQuery..."
bq query --use_legacy_sql=false \
"DELETE FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(attributes.filename) LIKE '${TEST_PREFIX}%'"

# Clean up test files from GCS bucket
echo "Cleaning up test files from GCS..."
gsutil rm "gs://${BUCKET_NAME}/${TEST_PREFIX}products.csv"
gsutil rm "gs://${BUCKET_NAME}/${TEST_PREFIX}shops.csv"
gsutil rm "gs://${BUCKET_NAME}/${TEST_PREFIX}transactions.csv"

echo "Integration test completed successfully! All test data cleaned up."
