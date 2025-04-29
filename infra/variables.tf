locals {
  required_services = [
    "dataform"
  ]
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
variable "default_region" {
  description = "Default GCP region"
  type        = string
}
variable "default_zone" {
  description = "Default GCP zone"
  type        = string
}
