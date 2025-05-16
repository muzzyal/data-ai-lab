module "github_token_secret" {
  source    = "../modules/secrets"
  secret_id = var.secret_id
  members   = var.secret_members
}

# resource "google_dataform_repository" "dataform_repository" {
#   provider = google-beta
#   name = "data_ai_lab_repo"
#   display_name = "data_ai_lab_repo"

#   git_remote_settings {
#       url = var.git_repo_url
#       default_branch = "main"
#       authentication_token_secret_version = "${module.github_token_secret.secret_id}/versions/latest"
#   }

#   depends_on = [
#     moduel.github_token_secret
#   ]
# }
