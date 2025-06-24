resource "google_artifact_registry_repository" "cloud_dock_repository" {
  location      = var.region
  repository_id = var.repository_id
  description   = var.description
  format        = "DOCKER"
}
