locals {
  required_services = [
    "dataform.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com"
  ]
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
