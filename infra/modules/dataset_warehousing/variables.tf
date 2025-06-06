variable "domain_name" {
  type = string
}

variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "landing_zone_editor_members" {
  type = list(string)
}

variable "landing_zone_viewer_members" {
  type    = list(string)
  default = []
}

variable "curated_layer_viewer_members" {
  type    = list(string)
  default = []
}

variable "builder_sa_email" {
  type = string
}

variable "dataform_sa_member" {
  type = string
}

variable "delete_contents_on_destroy" {
  type    = bool
  default = false
}
