resource "google_service_account" "build_admin" {
  account_id   = "build-admin"
  display_name = "Service account for Terraform builds"
  project      = var.project_id
}

resource "google_project_iam_member" "build_admin_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_project_iam_member" "build_admin_editor" {
  project = var.project_id
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

module "project_services" {
  source     = "./modules/project_services"
  project_id = var.project_id
  services   = local.required_services
}
