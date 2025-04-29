variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
variable "services" {
  description = "List of APIs (services) to enable"
  type        = list(string)
}
