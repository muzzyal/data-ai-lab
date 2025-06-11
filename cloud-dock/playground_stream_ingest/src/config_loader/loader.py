import os
import logging
from google.cloud import secretmanager

logger = logging.getLogger(__name__)


def retrieve_environment_variables() -> tuple[str, str, str]:
    """Retrieve and validate required environment variables for Pub/Sub and DLQ configuration.

    Returns:
        str: Project ID
        str: Pub/Sub Topic Name
        str: Pub/Sub Dead Letter Queue Topic Name
    """

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    topic_name = os.environ.get("PUBSUB_TOPIC_NAME", "")
    dlq_topic_name = os.environ.get("DLQ_TOPIC_NAME", "")
    secret_id = os.environ.get("SECRET_ID", "")

    if not project_id or not topic_name or not dlq_topic_name:
        logger.error("Environment variables for Pub/Sub or Secret Manager are not set properly.")
        raise ValueError("Missing required environment variables for Pub/Sub or Secret Manager configuration.")

    return project_id, topic_name, dlq_topic_name, secret_id


def get_secret_key():
    """Retrieve the secret key from Google Secret Manager.

    Returns:
        str: The secret key value.
        bool: True if retrieval was successful, False otherwise.
        str: Error message if retrieval failed, empty string otherwise.
    """

    project_id, topic_name, dlq_topic_name, secret_id = retrieve_environment_variables()

    if not secret_id:
        logger.error("SECRET_ID environment variable is not set.")
        raise ValueError("Missing required environment variable: SECRET_ID")

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8"), True, ""
    except Exception as e:
        logging.error("Retrieving secret failed with following error log:")
        logging.error(e)
        return "", False, f"Failed to retrieve secret {secret_id} for project {project_id}."
