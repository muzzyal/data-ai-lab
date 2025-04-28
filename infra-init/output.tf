output "service_account_key_base64" {
  value     = base64encode(google_service_account_key.build_service_account_key.private_key)
  sensitive = true
}

resource "local_file" "service_account_key_file" {
  content  = base64encode(google_service_account_key.build_service_account_key.private_key)
  filename = "${path.module}/sa-key-base64.txt"
}
