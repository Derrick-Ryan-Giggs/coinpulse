terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# ──────────────────────────────────────────────
# GCP PROJECT
# ──────────────────────────────────────────────
resource "google_project" "coinpulse" {
  name            = "CoinPulse"
  project_id      = var.gcp_project_id
  billing_account = var.gcp_billing_account
}

# ──────────────────────────────────────────────
# ENABLE REQUIRED APIS
# ──────────────────────────────────────────────
resource "google_project_service" "bigquery" {
  project            = google_project.coinpulse.project_id
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "bigquerystorage" {
  project            = google_project.coinpulse.project_id
  service            = "bigquerystorage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = google_project.coinpulse.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "resource_manager" {
  project            = google_project.coinpulse.project_id
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

# ──────────────────────────────────────────────
# SERVICE ACCOUNT
# ──────────────────────────────────────────────
resource "google_service_account" "coinpulse_sa" {
  account_id   = "coinpulse-sa"
  display_name = "CoinPulse Service Account"
  project      = google_project.coinpulse.project_id

  depends_on = [google_project_service.resource_manager]
}

resource "google_project_iam_member" "bq_data_editor" {
  project = google_project.coinpulse.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.coinpulse_sa.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = google_project.coinpulse.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.coinpulse_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = google_project.coinpulse.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.coinpulse_sa.email}"
}

resource "google_service_account_key" "coinpulse_sa_key" {
  service_account_id = google_service_account.coinpulse_sa.name
}

# ──────────────────────────────────────────────
# GCS BUCKET
# ──────────────────────────────────────────────
resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_bucket_name
  location      = var.gcp_region
  project       = google_project.coinpulse.project_id
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "Delete" }
  }

  depends_on = [google_project_service.storage]
}

# ──────────────────────────────────────────────
# BIGQUERY DATASETS
# ──────────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_dataset_raw
  location   = var.gcp_region
  project    = google_project.coinpulse.project_id
  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = var.bq_dataset_staging
  location   = var.gcp_region
  project    = google_project.coinpulse.project_id
  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = var.bq_dataset_mart
  location   = var.gcp_region
  project    = google_project.coinpulse.project_id
  depends_on = [google_project_service.bigquery]
}

# ──────────────────────────────────────────────
# BIGQUERY TABLE — STREAMING (hourly partitioned)
# ──────────────────────────────────────────────
resource "google_bigquery_table" "stream_prices" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "stream_prices"
  project             = google_project.coinpulse.project_id
  deletion_protection = false

  time_partitioning {
    type  = "HOUR"
    field = "event_timestamp"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",          type = "STRING",    mode = "REQUIRED" },
    { name = "price_usd",       type = "FLOAT64",   mode = "REQUIRED" },
    { name = "avg_price",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "min_price",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "max_price",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "price_stddev",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "open_price",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close_price",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "record_count",    type = "INT64",     mode = "NULLABLE" },
    { name = "window_start",    type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "window_end",      type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "REQUIRED" }
  ])
}

# ──────────────────────────────────────────────
# BIGQUERY TABLE — BATCH OHLCV (daily partitioned)
# ──────────────────────────────────────────────
resource "google_bigquery_table" "coingecko_ohlcv" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "coingecko_ohlcv"
  project             = google_project.coinpulse.project_id
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "snapshot_date"
  }

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",        type = "STRING",    mode = "REQUIRED" },
    { name = "name",          type = "STRING",    mode = "NULLABLE" },
    { name = "price_usd",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "market_cap",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "volume_24h",    type = "FLOAT64",   mode = "NULLABLE" },
    { name = "price_change",  type = "FLOAT64",   mode = "NULLABLE" },
    { name = "rank",          type = "INT64",     mode = "NULLABLE" },
    { name = "fdv",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "snapshot_date", type = "DATE",      mode = "REQUIRED" },
    { name = "ingested_at",   type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}
# ──────────────────────────────────────────────
# BIGQUERY TABLE — OHLC CANDLES
# ──────────────────────────────────────────────
resource "google_bigquery_table" "ohlc_candles" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "coingecko_ohlc_candles"
  project             = google_project.coinpulse.project_id
  deletion_protection = false

  clustering = ["symbol"]

  schema = jsonencode([
    { name = "symbol",        type = "STRING",    mode = "REQUIRED" },
    { name = "candle_time",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "open",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "high",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "low",           type = "FLOAT64",   mode = "NULLABLE" },
    { name = "close",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "snapshot_date", type = "STRING",    mode = "NULLABLE" },
    { name = "ingested_at",   type = "TIMESTAMP", mode = "NULLABLE" }
  ])
}
