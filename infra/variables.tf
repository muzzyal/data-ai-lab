locals {
  required_services = [
    "cloudresourcemanager",
    "dataform",
    "secretmanager"
  ]
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
