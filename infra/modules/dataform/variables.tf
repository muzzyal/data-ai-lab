variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "secret_id" {
  description = "ID for the secret"
  type        = string
}

variable "secret_members" {
  description = "List of members to assign access to"
  type        = list(string)
}

variable "git_repo_url" {
  description = "Git repo URL to link to dataform"
  type        = string
}

variable "region" {
  description = "Dataform region"
  type        = string
}

variable "project_no" {
  description = "GCP Project Number"
  type        = number
}
