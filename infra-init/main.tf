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

resource "google_project_iam_member" "project_iam_admin" {
  project = var.project_id
  role    = "roles/resourcemanager.projectIamAdmin"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_project_iam_member" "secret_manager_admin" {
  project = var.project_id
  role    = "roles/secretmanager.admin"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_project_iam_member" "dataform_admin" {
  project = var.project_id
  role    = "roles/dataform.admin"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_project_iam_member" "service_account_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_project_iam_member" "pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.build_admin.email}"
}

resource "google_service_account_key" "build_service_account_key" {
  service_account_id = google_service_account.build_admin.id
}

module "project_services" {
  source     = "../infra/modules/project_services"
  project_id = var.project_id
  services   = local.required_services
}

#tfsec:ignore:google-storage-bucket-encryption-customer-key
resource "google_storage_bucket" "terraform_state" {
  name          = "${var.project_id}-tfstate"
  location      = "EU"
  storage_class = "STANDARD"
  project       = var.project_id

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }
}

#tfsec:ignore:google-storage-bucket-encryption-customer-key
resource "google_storage_bucket" "infra_init_terraform_state" {
  name          = "${var.project_id}-infra-init-tfstate"
  location      = "EU" # or your preferred region
  storage_class = "STANDARD"
  project       = var.project_id

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }
}
