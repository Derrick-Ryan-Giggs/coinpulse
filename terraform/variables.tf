variable "gcp_project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "gcp_billing_account" {
  description = "GCP Billing Account ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for batch data lake"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery raw dataset ID"
  type        = string
}

variable "bq_dataset_staging" {
  description = "BigQuery staging dataset ID"
  type        = string
}

variable "bq_dataset_mart" {
  description = "BigQuery mart dataset ID"
  type        = string
}