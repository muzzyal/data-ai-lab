#tfsec:ignore:google-storage-bucket-encryption-customer-key
resource "google_storage_bucket" "playground_bucket" {
  name          = "${var.project_id}-${var.product_name}"
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  public_access_prevention = "enforced"
}
