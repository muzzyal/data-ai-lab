resource "google_pubsub_topic" "pubsub_bq" {
  project = var.project_id
  name    = "${var.data_product_name}_topic"
}

resource "google_pubsub_topic" "pubsub_bq_dlq" {
  project = var.project_id
  name    = "${var.data_product_name}_dlq"
}

resource "google_pubsub_subscription" "pubsub_bq_sub" {
  project = var.project_id
  name    = "${var.data_product_name}_subscription"
  topic   = google_pubsub_topic.pubsub_bq.name
  expiration_policy {
    ttl = ""
  }

  message_retention_duration = "1200s"

  ack_deadline_seconds = 180

  bigquery_config {
    table            = "${google_bigquery_table.pubsub_raw_table.project}:${google_bigquery_table.pubsub_raw_table.dataset_id}.${google_bigquery_table.pubsub_raw_table.table_id}"
    use_topic_schema = false
    write_metadata   = true
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.pubsub_bq_dlq.id
    max_delivery_attempts = 5
  }

  lifecycle {
    replace_triggered_by = [
      google_pubsub_topic.pubsub_bq
    ]
  }

  depends_on = [
    google_pubsub_topic.pubsub_bq
  ]

}

resource "google_pubsub_subscription" "pubsub_bq_dlq_sub" {
  project = var.project_id
  name    = "${var.data_product_name}_dlq_subscription"
  topic   = google_pubsub_topic.pubsub_bq_dlq.name

  # set at 3 days just in case we can't extract them for some reason
  message_retention_duration = "259200s"

  expiration_policy {
    ttl = ""
  }

  bigquery_config {
    table            = "${google_bigquery_table.pubsub_raw_dlq_table.project}:${google_bigquery_table.pubsub_raw_dlq_table.dataset_id}.${google_bigquery_table.pubsub_raw_dlq_table.table_id}"
    use_topic_schema = false
    write_metadata   = true
  }

  lifecycle {
    replace_triggered_by = [
      google_pubsub_topic.pubsub_bq_dlq
    ]
  }

  depends_on = [
    google_pubsub_topic.pubsub_bq_dlq
  ]

}

resource "google_pubsub_topic_iam_member" "pubsub_bq_publisher" {
  project = google_pubsub_topic.pubsub_bq.project
  topic   = google_pubsub_topic.pubsub_bq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${var.project_no}@gcp-sa-pubsub.iam.gserviceaccount.com"

  lifecycle {
    replace_triggered_by = [
      google_pubsub_topic.pubsub_bq
    ]
  }

  depends_on = [
    google_pubsub_topic.pubsub_bq
  ]

}

resource "google_pubsub_topic_iam_member" "pubsub_bq_dlq_publisher" {
  project = google_pubsub_topic.pubsub_bq_dlq.project
  topic   = google_pubsub_topic.pubsub_bq_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${var.project_no}@gcp-sa-pubsub.iam.gserviceaccount.com"

  lifecycle {
    replace_triggered_by = [
      google_pubsub_topic.pubsub_bq_dlq
    ]
  }

  depends_on = [
    google_pubsub_topic.pubsub_bq_dlq
  ]

}

resource "google_pubsub_subscription_iam_member" "dlq_subscriber" {
  subscription = google_pubsub_subscription.pubsub_bq_sub.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${var.project_no}@gcp-sa-pubsub.iam.gserviceaccount.com"

  lifecycle {
    replace_triggered_by = [
      google_pubsub_subscription.pubsub_bq_sub
    ]
  }

  depends_on = [
    google_pubsub_subscription.pubsub_bq_dlq_sub,
    google_pubsub_subscription.pubsub_bq_sub
  ]

}

resource "google_pubsub_topic_iam_member" "topic_publisher" {
  for_each = toset(var.topic_publisher_members)
  project  = google_pubsub_topic.pubsub_bq.project
  topic    = google_pubsub_topic.pubsub_bq.name
  role     = "roles/pubsub.publisher"
  member   = each.value

  lifecycle {
    replace_triggered_by = [
      google_pubsub_topic.pubsub_bq
    ]
  }

  depends_on = [
    google_pubsub_topic.pubsub_bq
  ]

}
