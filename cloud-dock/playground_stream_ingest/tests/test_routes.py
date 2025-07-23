import json
from unittest.mock import MagicMock, patch

import pytest
from playground_stream_ingest.tests.conftest import create_signature_and_body


class TestTransactionRoutes:
    """Test cases for transaction API routes."""

    def test_health_endpoint(self, client):
        """Test the health check endpoint."""
        response = client.get("/health")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert data["service"] == "transaction-ingestion"

    def test_index_route(self, client):
        """Test the index route returns API information."""
        response = client.get("/")

        assert response.status_code == 200
        assert response.content_type.startswith("application/json")
        data = response.get_json()
        assert "service" in data
        assert "version" in data

    def test_ingest_transaction_success(self, client, sample_transaction):
        """Test successful transaction ingestion."""
        signature, body = create_signature_and_body(sample_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"
        assert "message_id" in data
        assert data["transaction_id"] == sample_transaction["transaction_id"]

    def test_ingest_transaction_no_transaction_data(self, client, sample_transaction):
        """Test successful transaction ingestion."""
        signature, body = create_signature_and_body({})
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "Request body must contain valid JSON" in data["message"]

    def test_ingest_transaction_invalid_content_type(self, client, sample_transaction):
        """Test transaction ingestion with invalid content type."""
        response = client.post(
            "/api/transactions",
            data=json.dumps(sample_transaction),
            content_type="text/plain",
            headers={"X-Signature": "mock-signature"},
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "Content-Type" in data["message"]

    def test_ingest_transaction_empty_body(self, client):
        """Test transaction ingestion with empty request body."""
        response = client.post(
            "/api/transactions", data="", content_type="application/json", headers={"X-Signature": "mock-signature"}
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "valid JSON" in data["message"]

    def test_ingest_transaction_invalid_json(self, client):
        """Test transaction ingestion with invalid JSON."""
        response = client.post(
            "/api/transactions",
            data='{"invalid": json,}',
            content_type="application/json",
            headers={"X-Signature": "mock-signature"},
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"

    def test_ingest_transaction_validation_failure(self, client, invalid_transaction):
        """Test transaction ingestion with validation failure."""
        signature, body = create_signature_and_body(invalid_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "validation failed" in data["message"].lower()
        assert "validation_error" in data
        assert "dlq_message_id" in data

    def test_ingest_transaction_unexpected_error(self, client, sample_transaction):
        # Patch validator.full_validation to raise an unexpected exception
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.validator.full_validation",
            side_effect=Exception("Simulated unexpected error"),
        ):
            signature, body = create_signature_and_body(sample_transaction)
            response = client.post(
                "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
            )
            assert response.status_code == 500
            assert b"Unexpected error occurred" in response.data
            assert b"Simulated unexpected error" in response.data

    def test_ingest_transaction_dlq_fails_in_error_handler(self, client, sample_transaction):
        """Covers the except Exception: return jsonify(..., 500) when DLQ fails in error handler."""
        # Patch validator.full_validation to raise an unexpected error
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.validator.full_validation",
            side_effect=Exception("Simulated unexpected error"),
        ), patch(
            "playground_stream_ingest.src.routes.transaction_routes.dlq.send_to_dlq",
            side_effect=Exception("DLQ totally failed"),
        ):
            signature, body = create_signature_and_body(sample_transaction)
            response = client.post(
                "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
            )
            assert response.status_code == 500
            assert b"Critical system error" in response.data
            assert b"Simulated unexpected error" in response.data

    def test_validate_transaction_success(self, client, sample_transaction):
        """Test successful transaction validation."""
        signature, body = create_signature_and_body(sample_transaction)
        response = client.post(
            "/api/transactions/validate", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"
        assert "Transaction data is valid" == data["message"]
        assert data["transaction_id"] == sample_transaction["transaction_id"]

    def test_validate_transaction_failure(self, client, invalid_transaction):
        """Test transaction validation failure."""
        signature, body = create_signature_and_body(invalid_transaction)
        response = client.post(
            "/api/transactions/validate", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "validation failed" in data["message"].lower()
        assert "validation_error" in data

    def test_validate_transaction_invalid_content_type(self, client):
        """Test validation endpoint with invalid content type."""
        response = client.post(
            "/api/transactions/validate",
            data="test data",
            content_type="text/plain",
            headers={"X-Signature": "mock-signature"},
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "Content-Type" in data["message"]

    def test_validate_transaction_empty_body(self, client):
        """Test validation endpoint with empty body."""
        response = client.post(
            "/api/transactions/validate",
            data="",
            content_type="application/json",
            headers={"X-Signature": "mock-signature"},
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "valid JSON" in data["message"]

    def test_validate_transaction_unexpected_error(self, client, sample_transaction):
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.validator.full_validation",
            side_effect=Exception("Simulated validation error"),
        ):
            signature, body = create_signature_and_body(sample_transaction)
            response = client.post(
                "/api/transactions/validate",
                data=body,
                content_type="application/json",
                headers={"X-Signature": signature},
            )
            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"
            assert "Unexpected error during validation" in data["message"]
            assert "Simulated validation error" in data["error"]

    def test_validate_transaction_missing_transaction_data(self, client, sample_transaction):
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.validator.full_validation",
            side_effect=Exception("Simulated validation error"),
        ):
            signature, body = create_signature_and_body({})
            response = client.post(
                "/api/transactions/validate",
                data=body,
                content_type="application/json",
                headers={"X-Signature": signature},
            )
            assert response.status_code == 400
            data = json.loads(response.data)
            assert data["status"] == "error"
            assert "Request body must contain valid JSON" in data["message"]

    def test_service_status_exception(self, client):
        """Test /api/status returns 500 and error message if an exception occurs."""
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.dlq.get_dlq_stats",
            side_effect=Exception("Simulated status error"),
        ):
            response = client.get("/api/status")
            assert response.status_code == 500
            data = response.get_json()
            assert data["status"] == "error"
            assert "Failed to get service status" in data["message"]
            assert "Simulated status error" in data["error"]

    def test_service_status_endpoint(self, client):
        """Test the service status endpoint."""
        response = client.get("/api/status")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "healthy"
        assert data["service"] == "transaction-ingestion"
        assert "statistics" in data
        assert "published_messages" in data["statistics"]
        assert "dlq_stats" in data["statistics"]

    def test_get_dlq_messages_endpoint(self, client):
        """Test the DLQ messages endpoint."""
        response = client.get("/api/dlq/messages")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"
        assert "dlq_messages" in data
        assert "count" in data
        assert isinstance(data["dlq_messages"], list)
        assert isinstance(data["count"], int)

    def test_get_dlq_messages_exception(self, client):
        """Test /api/dlq/messages returns 500 and error message if an exception occurs."""
        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.dlq.get_dlq_messages",
            side_effect=Exception("Simulated status error"),
        ):
            response = client.get("/api/dlq/messages")
            assert response.status_code == 500
            data = response.get_json()
            assert data["status"] == "error"
            assert "Failed to get DLQ messages" in data["message"]
            assert "Simulated status error" in data["error"]

    def test_get_published_messages_endpoint(self, client):
        """Test the published messages endpoint."""
        response = client.get("/api/published/messages")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"
        assert "published_messages" in data
        assert "count" in data
        assert isinstance(data["published_messages"], list)
        assert isinstance(data["count"], int)

    def test_get_published_messages_exception(self, client):

        with patch(
            "playground_stream_ingest.src.routes.transaction_routes.publisher.get_published_messages",
            side_effect=Exception("Simulated status error"),
        ):
            response = client.get("/api/published/messages")
            assert response.status_code == 500
            data = response.get_json()
            assert data["status"] == "error"
            assert "Failed to get published messages" in data["message"]
            assert "Simulated status error" in data["error"]

    def test_ingest_transaction_minimal_valid(self, client, minimal_transaction):
        """Test transaction ingestion with minimal valid transaction."""
        signature, body = create_signature_and_body(minimal_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"
        assert "message_id" in data

    @patch("playground_stream_ingest.src.routes.transaction_routes.publisher.publish_with_retry")
    def test_ingest_transaction_publish_failure(self, mock_publish, client, sample_transaction):
        """Test transaction ingestion when publishing fails."""
        from playground_stream_ingest.src.services.publisher import PublishError

        mock_publish.side_effect = PublishError("Simulated publish failure")

        signature, body = create_signature_and_body(sample_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["status"] == "error"
        assert "failed to publish" in data["message"].lower()
        assert "dlq_message_id" in data
        assert "publish_error" in data

    def test_nonexistent_endpoint(self, client):
        """Test accessing a non-existent endpoint."""
        response = client.get("/api/nonexistent")

        assert response.status_code == 404

    def test_method_not_allowed(self, client):
        """Test accessing endpoint with wrong HTTP method."""
        response = client.get("/api/transactions")  # Should be POST

        assert response.status_code == 405

    def test_transaction_with_extra_fields(self, client, sample_transaction):
        """Test transaction with additional fields not in schema."""
        transaction_with_extra = sample_transaction.copy()
        transaction_with_extra["extra_field"] = "should_be_rejected"

        signature, body = create_signature_and_body(transaction_with_extra)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        response = client.post(
            "/api/transactions", data=json.dumps(transaction_with_extra), content_type="application/json"
        )

        # Should fail validation due to additionalProperties: false
        assert response.status_code == 400
        data = json.loads(response.data)
        assert data["status"] == "error"

    def test_large_transaction_payload(self, client):
        """Test handling of large transaction payloads."""
        large_transaction = {
            "transaction_id": "txn_large_test",
            "customer_id": "cust_large_test",
            "amount": 100.00,
            "currency": "USD",
            "transaction_type": "purchase",
            "timestamp": "2024-01-15T10:30:00Z",
            "payment_method": {"type": "credit_card"},
            "description": "x" * 499,  # Just under the 500 char limit
            "metadata": {"large_field": "y" * 1000},
        }

        signature, body = create_signature_and_body(large_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["status"] == "success"

    def test_concurrent_requests_simulation(self, client, sample_transaction):
        """Test multiple concurrent requests to simulate load."""
        responses = []

        # Send multiple requests
        for i in range(5):
            transaction = sample_transaction.copy()
            transaction["transaction_id"] = f"txn_concurrent_{i}"

            signature, body = create_signature_and_body(transaction)
            response = client.post(
                "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
            )
            responses.append(response)

        # All should succeed
        for response in responses:
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["status"] == "success"

    def test_api_endpoints_response_headers(self, client):
        """Test that API endpoints return appropriate headers."""
        response = client.get("/api/status")

        assert response.status_code == 200
        assert response.content_type == "application/json"

    def test_error_response_format_consistency(self, client):
        """Test that error responses follow consistent format."""
        # Test various error scenarios
        error_responses = [
            client.post(
                "/api/transactions", data="", content_type="application/json", headers={"X-Signature": "mock-signature"}
            ),
            client.post(
                "/api/transactions",
                data="invalid json",
                content_type="application/json",
                headers={"X-Signature": "mock-signature"},
            ),
            client.post(
                "/api/transactions", data="{}", content_type="text/plain", headers={"X-Signature": "mock-signature"}
            ),
        ]

        for response in error_responses:
            assert response.status_code >= 400
            data = json.loads(response.data)

            # All error responses should have consistent structure
            assert "status" in data
            assert "message" in data
            assert data["status"] == "error"
            assert isinstance(data["message"], str)
            assert len(data["message"]) > 0

    def test_success_response_format_consistency(self, client, sample_transaction):
        """Test that success responses follow consistent format."""

        signature, body = create_signature_and_body(sample_transaction)
        response = client.post(
            "/api/transactions", data=body, content_type="application/json", headers={"X-Signature": signature}
        )

        assert response.status_code == 200
        data = json.loads(response.data)

        # Success response should have consistent structure
        assert "status" in data
        assert "message" in data
        assert data["status"] == "success"
        assert isinstance(data["message"], str)
        assert len(data["message"]) > 0
