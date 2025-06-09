module "ingest_service_secret" {
  source = "../secrets"

  project_id = var.project_id
  secret_id  = var.secret_id
  members    = var.secret_members
}
