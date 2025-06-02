variable "dataset_name" {
  type = string
}

variable "dataset_location" {
  type = string
}

variable "dataset_project" {
  type = string
}

variable "dataset_description" {
  type    = string
  default = ""
}

variable "dataset_editor_members" {
  type = list(string)
}

variable "dataset_viewer_members" {
  type    = list(string)
  default = []
}

variable "builder_sa_email" {
  type = string
}

variable "default_table_expiration_ms" {
  type    = number
  default = null
}

variable "dataset_specific_sa" {
  type    = bool
  default = false
}

variable "delete_contents_on_destroy" {
  type    = bool
  default = false
}

variable "extra_owner_members" {
  type    = list(string)
  default = []
}
