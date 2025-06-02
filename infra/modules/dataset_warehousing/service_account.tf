resource "google_service_account" "zone_default_account" {
  project      = var.project_id
  account_id   = "${var.domain_name}-default-account"
  display_name = "Default service account - ${var.domain_name}"
}

resource "google_service_account_iam_member" "zone_default_account_service_account_user" {
  for_each = {
    self = google_service_account.zone_default_account.member
  }
  service_account_id = google_service_account.zone_default_account.name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.zone_default_account.member
}
