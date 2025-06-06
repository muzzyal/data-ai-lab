resource "google_project_iam_member" "storage_object_download" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = var.service_account_member
}
