# Grant permission to receive Eventarc events
resource "google_project_iam_member" "eventreceiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = var.service_account_member
}

# Grant the service account permission to publish pub/sub topics
resource "google_project_iam_member" "pubsubpublisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = var.service_account_member
}

# Create an Eventarc trigger, routing Cloud Storage events to Cloud Run
resource "google_eventarc_trigger" "gcs_cloud_run_trigger" {
  name     = "${var.data_product_name}-trigger-storage-cloudrun"
  location = var.location

  # Capture objects changed in the bucket
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = var.cloud_storage_bucket
  }

  # Send events to Cloud Run
  destination {
    cloud_run_service {
      service = var.cloud_run_service_name
      region  = var.location
    }
  }

  service_account = var.service_account_email
  depends_on = [
    google_project_iam_member.eventreceiver,
    google_project_iam_member.pubsubpublisher
  ]
}
