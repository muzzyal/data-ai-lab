locals {
  required_services = [
    "cloudresourcemanager",
    "dataform",
    "secretmanager"
  ]

  builder_sa_email = "build-admin@muz-designed-msc-data-ai-2025.iam.gserviceaccount.com"

  default_dataform_sa_member = "serviceAccount:service-${var.project_no}@gcp-sa-dataform.iam.gserviceaccount.com"
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
variable "project_no" {
  description = "GCP Project Number"
  type        = number
}
variable "default_region" {
  description = "Default GCP region"
  type        = string
}
variable "default_zone" {
  description = "Default GCP zone"
  type        = string
}
