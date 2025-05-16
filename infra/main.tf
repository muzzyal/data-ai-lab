module "project_services" {
  source     = "./modules/project_services"
  project_id = var.project_id
  services   = local.required_services
}

module "dataform" {
  source = "./modules/dataform"

  secret_id    = "git-hub-token"
  git_repo_url = "https://github.com/muzzyal/data-ai-lab.git"
  secret_members = [
    "serviceAccount:service-${var.project_no}@gcp-sa-dataform.iam.gserviceaccount.com"
  ]
}
