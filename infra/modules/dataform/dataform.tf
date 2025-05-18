module "github_token_secret" {
  source = "../secrets"

  project_id = var.project_id
  secret_id  = var.secret_id
  members    = var.secret_members
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:service-${var.project_no}@gcp-sa-dataform.iam.gserviceaccount.com"
}

resource "google_dataform_repository" "dataform_repository" {
  provider = google-beta

  project      = var.project_id
  region       = var.region
  name         = "data_ai_lab_repo"
  display_name = "data_ai_lab_repo"

  git_remote_settings {
    url                                 = var.git_repo_url
    default_branch                      = "main"
    authentication_token_secret_version = "${module.github_token_secret.secret_id}/versions/latest"
  }

  depends_on = [
    module.github_token_secret
  ]
}
