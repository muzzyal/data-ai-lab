resource "google_cloud_run_service" "stream_ingest" {
  name     = "playground-stream-ingest"
  location = var.location
  project  = var.project_id

  template {
    spec {
      containers {
        image = "europe-west2-docker.pkg.dev/muz-designed-msc-data-ai-2025/cloud-dock/playground_stream_ingest:${var.cloud_run_stream_ingest_version}"
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_TOPIC_NAME"
          value = var.pubsub_topic_name
        }
        env {
          name  = "DLQ_TOPIC_NAME"
          value = var.dlq_topic_name
        }
        env {
          name  = "SECRET_ID"
          value = var.secret_id
        }
        env {
          name  = "USE_REAL_PUBSUB"
          value = "true"
        }
      }
      service_account_name = var.service_account_email
    }
  }
  metadata {
    annotations = {
      "run.googleapis.com/ingress" = "all"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  # ignore noisy metadata change
  lifecycle {
    ignore_changes = [
      metadata[0].annotations
    ]
  }
}

# invoker
resource "google_cloud_run_service_iam_binding" "run_stream_ingest_invoker" {
  location = google_cloud_run_service.stream_ingest.location
  project  = google_cloud_run_service.stream_ingest.project
  service  = google_cloud_run_service.stream_ingest.name
  role     = "roles/run.invoker"
  members  = ["allUsers"]
}


resource "google_cloud_run_service" "batch_ingest" {
  name     = "playground-batch-ingest"
  location = var.location
  project  = var.project_id

  template {
    spec {
      containers {
        image = "europe-west2-docker.pkg.dev/muz-designed-msc-data-ai-2025/cloud-dock/playground_batch_ingest:${var.cloud_run_batch_ingest_version}"
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }
        env {
          name  = "PUBSUB_TOPIC_NAME"
          value = var.pubsub_topic_name
        }
        env {
          name  = "DLQ_TOPIC_NAME"
          value = var.dlq_topic_name
        }
        env {
          name  = "BUCKET_NAME"
          value = google_storage_bucket.playground_bucket.name
        }
      }
      service_account_name = var.service_account_email
    }
  }
  metadata {
    annotations = {
      "run.googleapis.com/ingress" = "all"
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  # ignore noisy metadata change
  lifecycle {
    ignore_changes = [
      metadata[0].annotations
    ]
  }
}

# invoker
resource "google_cloud_run_service_iam_binding" "run_batch_ingest_invoker" {
  location = google_cloud_run_service.batch_ingest.location
  project  = google_cloud_run_service.batch_ingest.project
  service  = google_cloud_run_service.batch_ingest.name
  role     = "roles/run.invoker"
  members  = [var.service_account_member]
}
