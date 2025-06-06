locals {
  unstructured_schema = <<EOF
  [
  {
    "name":"subscription_name",
    "type":"STRING",
    "mode":"REQUIRED"
  },
  {
    "name":"message_id",
    "type":"STRING",
    "mode":"REQUIRED"
  },
  {
    "name": "publish_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "data",
    "type": "JSON",
    "mode": "REQUIRED"
  },
  {
    "name":"attributes",
    "type":"STRING",
    "mode":"REQUIRED"
  }
  ]
  EOF
}

resource "google_bigquery_table" "pubsub_raw_table" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = var.data_product_name
  schema              = local.unstructured_schema
  deletion_protection = false

  time_partitioning {
    field         = "publish_time"
    type          = "DAY"
    expiration_ms = var.default_table_partition_expiry
  }
}

# create a table to receive the DLQ events
resource "google_bigquery_table" "pubsub_raw_dlq_table" {
  project             = var.project_id
  dataset_id          = var.dataset_id
  table_id            = "${var.data_product_name}_dlq"
  deletion_protection = false
  schema              = local.unstructured_schema

  time_partitioning {
    field = "publish_time"
    type  = "DAY"
  }
}
