module "raw_dataset" {
  source                      = "../bigquery_dataset"
  dataset_name                = replace("${var.domain_name}_raw", "-", "_")
  dataset_location            = var.location
  dataset_description         = "${var.domain_name} raw dataset."
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.delete_contents_on_destroy
  dataset_editor_members = concat(
    var.landing_zone_editor_members,
    [var.dataform_sa_member]
  )
  dataset_project = var.project_id
  dataset_viewer_members = concat(
    var.landing_zone_viewer_members
  )
  builder_sa_email    = var.builder_sa_email
  extra_owner_members = [var.dataform_sa_member]
}

module "curated_dataset" {
  source                      = "../bigquery_dataset"
  dataset_name                = replace("${var.domain_name}_curated", "-", "_")
  dataset_location            = var.location
  dataset_description         = "${var.domain_name} curated dataset."
  dataset_project             = var.project_id
  default_table_expiration_ms = null
  delete_contents_on_destroy  = var.delete_contents_on_destroy
  dataset_editor_members = concat(
    [var.dataform_sa_member]
  )
  dataset_viewer_members = concat(
    var.curated_layer_viewer_members
  )
  builder_sa_email    = var.builder_sa_email
  extra_owner_members = [var.dataform_sa_member]
}

module "analytics_dataset" {
  source                      = "../bigquery_dataset"
  dataset_name                = replace("${var.domain_name}_analytic", "-", "_")
  dataset_location            = var.location
  dataset_description         = "${var.domain_name} analytic dataset."
  dataset_project             = var.project_id
  delete_contents_on_destroy  = var.delete_contents_on_destroy
  default_table_expiration_ms = null
  dataset_editor_members = concat(
    var.landing_zone_editor_members,
    [var.dataform_sa_member]
  )
  dataset_viewer_members = concat(
    var.landing_zone_viewer_members,
    var.curated_layer_viewer_members
  )
  builder_sa_email    = var.builder_sa_email
  extra_owner_members = [var.dataform_sa_member]
}
