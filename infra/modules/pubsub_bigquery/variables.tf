variable "project_id" {
  type = string
}

variable "dataset_id" {
  type = string
}

variable "data_product_name" {
  type = string
}

variable "project_no" {
  type = string
}

variable "topic_publisher_members" {
  type    = list(string)
  default = []
}

variable "default_table_partition_expiry" {
  type    = number
  default = null
}
