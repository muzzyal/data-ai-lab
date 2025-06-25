variable "location" {
  type = string
}

variable "project_id" {
  type = string
}

variable "project_no" {
  type = string
}

variable "secret_id" {
  description = "ID for the secret"
  type        = string
}

variable "pubsub_topic_name" {
  description = "ID for the secret"
  type        = string
}

variable "dlq_topic_name" {
  description = "ID for the secret"
  type        = string
}

variable "secret_members" {
  description = "List of members to assign access to"
  type        = list(string)
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
