terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.38.0"
    }
  }
  required_version = ">= 1.7.5"
}

provider "google" {
  project = var.project_id
  region  = var.default_region
}

provider "google-beta" {
  project = var.project_id
  region  = var.default_region
}
