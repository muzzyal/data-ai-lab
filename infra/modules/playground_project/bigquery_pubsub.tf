module "playground_bq_pubsub" {
  source = "../pubsub_bigquery"

  project_id              = var.project_id
  data_product_name       = var.product_name
  dataset_id              = var.dataset_id
  project_no              = var.project_no
  topic_publisher_members = var.topic_publisher_members

}
