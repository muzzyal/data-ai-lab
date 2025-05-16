variable "secret_id" {
  description = "ID for the secret"
  type        = string
}

variable "members" {
  description = "List of members to assign access to"
  type        = list(string)
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}
