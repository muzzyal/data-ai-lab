resource "google_secret_manager_secret" "secret" {
  provider  = google-beta
  project   = var.project_id
  secret_id = var.secret_id

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_binding" "secret_iam_binding" {
  provider  = google-beta
  secret_id = google_secret_manager_secret.secret.id
  role      = "roles/secretmanager.secretAccessor"

  members = var.members
}
