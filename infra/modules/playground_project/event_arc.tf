module "event_arc_pubsub_bigquery" {
  source = "../gcs_trigger_event_arc"

  location               = var.location
  project_id             = var.project_id
  service_account_member = var.service_account_member
  data_product_name      = var.product_name
  cloud_storage_bucket   = google_storage_bucket.playground_bucket.name
  cloud_run_service_name = google_cloud_run_service.batch_ingest.name
  service_account_email  = var.service_account_email

}
