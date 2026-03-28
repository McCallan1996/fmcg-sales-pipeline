terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# --- GCS bucket (data lake) ---

resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-fmcg-data-lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }
}

# --- BigQuery datasets ---

resource "google_bigquery_dataset" "raw" {
  dataset_id    = "fmcg_raw"
  friendly_name = "FMCG Raw Data"
  location      = var.region
  description   = "Landing zone for raw CSV data from Kaggle"
}

resource "google_bigquery_dataset" "processed" {
  dataset_id    = "fmcg_processed"
  friendly_name = "FMCG Processed"
  location      = var.region
  description   = "Spark-enriched data"
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id    = "fmcg_analytics"
  friendly_name = "FMCG Analytics"
  location      = var.region
  description   = "dbt mart tables — this is what the dashboard reads from"
}
