variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "europe-central2" # Warsaw — closest to the dataset's context (Poland)
}

variable "credentials_file" {
  description = "Path to GCP service account key JSON"
  type        = string
  default     = "../credentials/google_credentials.json"
}
