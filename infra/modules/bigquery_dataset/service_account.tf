resource "google_service_account" "dataset_service_account" {
  count        = var.dataset_specific_sa == true ? 1 : 0
  project      = var.dataset_project
  account_id   = "${replace(var.dataset_name, "_", "-")}-sa"
  display_name = "The service account for ${var.dataset_name}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  count   = var.dataset_specific_sa == true ? 1 : 0
  project = google_service_account.dataset_service_account[0].project
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.dataset_service_account[0].member
}
