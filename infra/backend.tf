terraform {
  backend "gcs" {
    bucket = "muz-designed-msc-data-ai-2025-tfstate"
    prefix = "terraform/state"
  }
}
