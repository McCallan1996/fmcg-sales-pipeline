output "bucket_name" {
  value       = google_storage_bucket.data_lake.name
  description = "GCS data lake bucket"
}

output "raw_dataset" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "processed_dataset" {
  value = google_bigquery_dataset.processed.dataset_id
}

output "analytics_dataset" {
  value = google_bigquery_dataset.analytics.dataset_id
}
