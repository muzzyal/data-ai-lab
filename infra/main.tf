module "project_services" {
  source     = "./modules/project_services"
  project_id = var.project_id
  services = local.required_services
}
