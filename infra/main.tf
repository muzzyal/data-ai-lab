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
  git_repo_url = "https://github.com/muzzyal/data-ai-lab.git"
  secret_members = [
    "serviceAccount:service-${var.project_no}@gcp-sa-dataform.iam.gserviceaccount.com"
  ]
}
