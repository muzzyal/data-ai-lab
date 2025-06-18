#!/bin/bash
set -e

# CONFIGURATION
PROJECT_ID="your-gcp-project"
PUBSUB_TOPIC="your-pubsub-topic"
BQ_DATASET="your_bq_dataset"
BQ_TABLE="your_bq_table"
API_URL="http://localhost:8080/api/transactions"
TEST_TRANSACTION_ID="ci_cd_test_$(date +%s)"

# 1. Publish a transaction via your API
echo "Publishing test transaction to API..."
RESPONSE=$(curl -s -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -H "X-Signature: dummy" \
  -d '{
    "transaction_id": "'"$TEST_TRANSACTION_ID"'",
    "customer_id": "ci_cd_test",
    "amount": 1.23,
    "currency": "USD",
    "transaction_type": "purchase",
    "timestamp": "'"$(date -u +"%Y-%m-%dT%H:%M:%SZ")"'",
    "payment_method": {"type": "credit_card"}
  }')

echo "API response: $RESPONSE"
MESSAGE_ID=$(echo "$RESPONSE" | jq -r '.message_id')

if [[ "$MESSAGE_ID" == "null" || -z "$MESSAGE_ID" ]]; then
  echo "Failed to get message_id from API response."
  exit 1
fi

# 2. Wait for ingestion (may need to increase sleep for your pipeline)
echo "Waiting for ingestion..."
sleep 30

# 3. Check for the record in BigQuery
echo "Checking for record in BigQuery..."
RECORD_COUNT=$(bq --project_id="$PROJECT_ID" query --nouse_legacy_sql --format=csv \
  "SELECT COUNT(*) FROM \`$PROJECT_ID.$BQ_DATASET.$BQ_TABLE\` WHERE transaction_id = '$TEST_TRANSACTION_ID'" | tail -n1)

if [[ "$RECORD_COUNT" -eq 1 ]]; then
  echo "Record found in BigQuery."
else
  echo "Record NOT found in BigQuery."
  exit 1
fi

# 4. Clean up: delete the test record from BigQuery
echo "Cleaning up test record..."
bq --project_id="$PROJECT_ID" query --nouse_legacy_sql \
  "DELETE FROM \`$PROJECT_ID.$BQ_DATASET.$BQ_TABLE\` WHERE transaction_id = '$TEST_TRANSACTION_ID'"

echo "Integration test completed successfully."
