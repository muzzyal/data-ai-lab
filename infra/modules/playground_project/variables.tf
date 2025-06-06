variable "location" {
  type = string
}

variable "project_id" {
  type = string
}

variable "project_no" {
  type = string
}

variable "cloud_run_batch_ingest_version" {
  type = string
}

variable "cloud_run_stream_ingest_version" {
  type = string
}

variable "service_account_member" {
  type = string
}

variable "service_account_email" {
  type = string
}

variable "product_name" {
  type = string
}

variable "builder_sa_email" {
  type = string
}

variable "dataset_id" {
  type = string
}

variable "topic_publisher_members" {
  type = list(string)
}
