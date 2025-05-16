output "secret_id" {
  description = "The ID of the latest secret version"
  value       = google_secret_manager_secret.secret.id
}
