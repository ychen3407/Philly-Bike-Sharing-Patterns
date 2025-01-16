terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.11.2"
    }
  }
}

provider "google" {
  credentials = file(var.credential)
  project     = var.gcp_project_id
  region      = "us-east4"
}


resource "google_storage_bucket" "gcp-de-bucket" {
  name          = var.gcp_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}


resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.location
  delete_contents_on_destroy = true
}