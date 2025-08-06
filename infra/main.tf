module "project_services" {
  source     = "./modules/project_services"
  project_id = var.project_id
  services   = local.required_services
}

module "dataform" {
  source = "./modules/dataform"

  project_id   = var.project_id
  secret_id    = "git-hub-token"
  region       = "europe-west2"
  project_no   = var.project_no
  git_repo_url = "https://github.com/muzzyal/data-ai-lab.git"
  secret_members = [
    "serviceAccount:service-${var.project_no}@gcp-sa-dataform.iam.gserviceaccount.com"
  ]
}

module "artefact_registry" {
  source = "./modules/artefact_registry"

  region        = "europe-west2"
  repository_id = "cloud-dock"
  description   = "Cloud Dock Repository"
}

# playground datasets
module "playground_datasets" {
  source = "./modules/dataset_warehousing"

  domain_name                  = "msc_playground"
  project_id                   = var.project_id
  location                     = var.default_region
  landing_zone_editor_members  = ["serviceAccount:service-${var.project_no}@gcp-sa-pubsub.iam.gserviceaccount.com"]
  landing_zone_viewer_members  = []
  curated_layer_viewer_members = []
  builder_sa_email             = local.builder_sa_email
  dataform_sa_member           = local.default_dataform_sa_member
  delete_contents_on_destroy   = true

}

# playground project deployment
module "playground_project" {
  source = "./modules/playground_project"

  product_name                    = "playground_project"
  location                        = var.default_region
  project_id                      = var.project_id
  project_no                      = var.project_no
  dataset_id                      = module.playground_datasets.raw_dataset_name
  cloud_run_batch_ingest_version  = "0.1.0"
  cloud_run_stream_ingest_version = "0.1.0"
  pubsub_topic_name               = "playground_project_topic"
  dlq_topic_name                  = "playground_project_dlq"
  service_account_member          = module.playground_datasets.landing_zone_default_sa_member
  service_account_email           = module.playground_datasets.landing_zone_default_sa_email
  builder_sa_email                = local.builder_sa_email
  topic_publisher_members         = [module.playground_datasets.landing_zone_default_sa_member]
  secret_id                       = "playground_project_stream_secret"
  secret_members                  = [module.playground_datasets.landing_zone_default_sa_member]
}
