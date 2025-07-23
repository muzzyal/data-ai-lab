from unittest.mock import patch

import flask as Flask
import pytest
from playground_stream_ingest.tests.conftest import failed_retrieve_secret_key


@pytest.mark.usefixtures("mock_env_retrieval")
@patch("playground_stream_ingest.src.config_loader.loader.get_secret_key", failed_retrieve_secret_key)
def test_create_app_secret_manager_failure():
    """Test that create_app raises ValueError if secret retrieval fails."""
    with pytest.raises(ValueError, match="Failed to retrieve secret key from Secret Manager"):
        from playground_stream_ingest.src.app import create_app
