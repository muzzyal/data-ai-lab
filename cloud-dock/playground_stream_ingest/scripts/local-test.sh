#!/bin/bash

# Local testing script for Transaction Ingestion Service
set -e

echo "🧪 Testing Transaction Ingestion Service Locally"
echo ""

# Check if the service is running
SERVICE_URL="http://localhost:5000"

# Function to test endpoint
test_endpoint() {
    local method=$1
    local endpoint=$2
    local data=$3
    local expected_status=$4

    echo "Testing $method $endpoint..."

    if [ -n "$data" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$SERVICE_URL$endpoint")
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
            "$SERVICE_URL$endpoint")
    fi

    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n -1)

    if [ "$http_code" = "$expected_status" ]; then
        echo "✅ $method $endpoint - Status: $http_code"
        echo "   Response: $(echo "$body" | jq -c . 2>/dev/null || echo "$body")"
    else
        echo "❌ $method $endpoint - Expected: $expected_status, Got: $http_code"
        echo "   Response: $body"
    fi
    echo ""
}

# Health check
test_endpoint "GET" "/health" "" "200"

# Service status
test_endpoint "GET" "/api/status" "" "200"

# Valid transaction
valid_transaction='{
    "transaction_id": "txn_test_'$(date +%s)'",
    "customer_id": "cust_test_123",
    "amount": 99.99,
    "currency": "USD",
    "transaction_type": "purchase",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
    "payment_method": {
        "type": "credit_card",
        "last_four": "1234",
        "provider": "Visa"
    },
    "description": "Test transaction"
}'

test_endpoint "POST" "/api/transactions" "$valid_transaction" "200"

# Validation only endpoint
test_endpoint "POST" "/api/transactions/validate" "$valid_transaction" "200"

# Invalid transaction (negative amount)
invalid_transaction='{
    "transaction_id": "txn_invalid_'$(date +%s)'",
    "customer_id": "cust_test_123",
    "amount": -50.00,
    "currency": "USD",
    "transaction_type": "purchase",
    "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
    "payment_method": {
        "type": "credit_card"
    }
}'

test_endpoint "POST" "/api/transactions" "$invalid_transaction" "400"

# DLQ messages endpoint
test_endpoint "GET" "/api/dlq/messages" "" "200"

# Published messages endpoint
test_endpoint "GET" "/api/published/messages" "" "200"

echo "🎯 Testing complete!"
echo ""
echo "To run the full test suite:"
echo "  pytest tests/ -v --cov=. --cov-report=html"
