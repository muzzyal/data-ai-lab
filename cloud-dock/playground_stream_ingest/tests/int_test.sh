#!/bin/bash
set -e

PROJECT_ID="muz-designed-msc-data-ai-2025"
PUBSUB_TOPIC="playground_project_topic"
DLQ_TOPIC_NAME="playground_project_dlq"
SECRET_ID="playground_project_stream_secret"
BQ_DATASET="msc_playground_raw"
BQ_TABLE="playground_project"
ENDPOINT="http://localhost:8080/api/transactions"
SECRET_VERSION="latest"
SIGNATURE_ALG="SHA512"

echo "PROJECT_ID=$PROJECT_ID"
echo "PUBSUB_TOPIC=$PUBSUB_TOPIC"
echo "DLQ_TOPIC_NAME=$DLQ_TOPIC_NAME"
echo "SECRET_ID=$SECRET_ID"
echo "BQ_DATASET=$BQ_DATASET"
echo "BQ_TABLE=$BQ_TABLE"
echo "ENDPOINT=$ENDPOINT"
echo "SECRET_VERSION=$SECRET_VERSION"
echo "SIGNATURE_ALG=$SIGNATURE_ALG"
echo "GOOGLE_APPLICATION_CREDENTIALS=$GOOGLE_APPLICATION_CREDENTIALS"

# Clean up any previous test data in BigQuery
bq query --use_legacy_sql=false \
"DELETE FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(data.transaction_id) = \"intTest_123456789\""

PORT=8080

docker run -d --name mock-api -p 8080:${PORT} -e PORT=${PORT} \
-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/FILE_NAME.json \
-e GOOGLE_CLOUD_PROJECT="${PROJECT_ID}" \
-e PUBSUB_TOPIC_NAME="${PUBSUB_TOPIC}" \
-e DLQ_TOPIC_NAME="${DLQ_TOPIC_NAME}" \
-e SECRET_ID="${SECRET_ID}" \
-v "${GOOGLE_APPLICATION_CREDENTIALS}:/tmp/keys/FILE_NAME.json:ro" test

# Wait for the service to start
sleep 3

echo "sleep finished"

SECRET=$(gcloud secrets versions access "$SECRET_VERSION" --secret="$SECRET_ID" --project="$PROJECT_ID")

if [ -z "$SECRET" ]; then
    echo "Failed to retrieve secret."
    exit 1
fi

# Testing transaction ingestion
JSON_FILE="tests/payload_example/transaction_example.json"

if [ ! -f "$JSON_FILE" ]; then
    echo "JSON file $JSON_FILE not found!"
    exit 1
fi

echo "Reading payload content from $JSON_FILE"
PAYLOAD=$(tr -d '\n' < "$JSON_FILE")

# Sign the payload using OpenSSL and the secret as the key
SIGNATURE=$(echo -n "$PAYLOAD" | openssl dgst -"$SIGNATURE_ALG" -hmac "$(echo -n "$SECRET" | xxd -r -p)" | awk '{print $2}')

if [ -z "$SIGNATURE" ]; then
    echo "Failed to create signature."
    exit 1
fi

# mock webhook
RESPONSE=$(curl -X POST "$ENDPOINT" \
    -H "Content-Type: application/json" \
    -H "X-Signature: $SIGNATURE" \
    -d "$PAYLOAD")

echo "$RESPONSE"
echo

docker logs mock-api

echo
echo

# Stop the Docker container
docker stop mock-api
docker container rm mock-api

# wait for pubsub to ingest to BQ
echo "Waiting for ingestion..."
sleep 10

# validate webhooks
ROWS=$(bq query --format=json --use_legacy_sql=false \
"SELECT count(*) as cnt FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE JSON_VALUE(data.transaction_id) = \"intTest_123456789\"" 2>&1 | sed '/^WARNING/d' | jq .[0].cnt)

ROWSINT=$(bc <<< "$ROWS")

echo "Identified ${ROWSINT} rows in raw transactions table"

# throw an error if number of rows is not greater than 0
if [ "$ROWSINT" -eq 0 ]; then
  echo "Table does not contain rows"
  exit 1
fi
