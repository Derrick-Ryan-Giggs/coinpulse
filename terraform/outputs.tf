output "project_id" {
  description = "GCP Project ID"
  value       = google_project.coinpulse.project_id
}

output "gcs_bucket_name" {
  description = "GCS data lake bucket name"
  value       = google_storage_bucket.data_lake.name
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  description = "BigQuery staging dataset"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_mart" {
  description = "BigQuery mart dataset"
  value       = google_bigquery_dataset.mart.dataset_id
}

output "service_account_email" {
  description = "Service account email"
  value       = google_service_account.coinpulse_sa.email
}

output "service_account_key" {
  description = "Service account key (base64 encoded) — save as secrets/gcp-key.json"
  value       = google_service_account_key.coinpulse_sa_key.private_key
  sensitive   = true
}