output "landing_zone_default_sa_email" {
  value = google_service_account.zone_default_account.email
}

output "landing_zone_default_sa_member" {
  value = google_service_account.zone_default_account.member
}

output "landing_zone_default_sa_name" {
  description = "Fully qualified name for SA"
  value       = google_service_account.zone_default_account.name
}

output "raw_dataset_name" {
  value = module.raw_dataset.dataset_name
}
