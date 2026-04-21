import os
import logging
from datetime import datetime, timedelta

import requests
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

COINS         = ["bitcoin", "ethereum", "solana", "binancecoin", "cardano"]
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
OHLC_URL      = "https://api.coingecko.com/api/v3/coins/{id}/ohlc"

# ── COIN ID → TICKER SYMBOL MAP ──────────────────────────
COIN_SYMBOL_MAP = {
    "bitcoin":     "BTC",
    "ethereum":    "ETH",
    "solana":      "SOL",
    "binancecoin": "BNB",
    "cardano":     "ADA",
}

# ── EXPLICIT BQ SCHEMAS ──────────────────────────────────
MARKETS_SCHEMA = [
    bigquery.SchemaField("symbol",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("name",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("price_usd",     "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("market_cap",    "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("volume_24h",    "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("price_change",  "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("rank",          "INT64",     mode="NULLABLE"),
    bigquery.SchemaField("fdv",           "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date", "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",   "STRING",    mode="NULLABLE"),
]

OHLC_SCHEMA = [
    bigquery.SchemaField("symbol",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("candle_time",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("open",          "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("high",          "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("low",           "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("close",         "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("snapshot_date", "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",   "STRING",    mode="NULLABLE"),
]

def get_credentials():
    return service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
        ]
    )

def ensure_table_exists(client, table_ref, schema, clustering_fields=None):
    """Create table with explicit schema if it doesn't exist."""
    try:
        client.get_table(table_ref)
        log.info(f"Table {table_ref} exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        if clustering_fields:
            table.clustering_fields = clustering_fields
        client.create_table(table)
        log.info(f"Created table {table_ref}")

# ── TASK 1: FETCH MARKETS ────────────────────────────────
def fetch_coingecko(**context):
    log.info("Fetching market data from CoinGecko...")
    headers = {"x-cg-demo-api-key": os.environ["COINGECKO_API_KEY"]}
    params  = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": 10,
        "page": 1,
        "sparkline": False,
        "price_change_percentage": "24h",
    }
    response = requests.get(COINGECKO_URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    log.info(f"Fetched {len(data)} coins")
    context["ti"].xcom_push(key="raw_data", value=data)

# ── TASK 2: TRANSFORM MARKETS ────────────────────────────
def transform_to_parquet(**context):
    raw_data      = context["ti"].xcom_pull(key="raw_data", task_ids="fetch_coingecko")
    snapshot_date = context["ds"]

    rows = [{
        "symbol":        c.get("symbol", "").upper(),
        "name":          c.get("name", ""),
        "price_usd":     float(c.get("current_price") or 0),
        "market_cap":    float(c.get("market_cap") or 0),
        "volume_24h":    float(c.get("total_volume") or 0),
        "price_change":  float(c.get("price_change_percentage_24h") or 0),
        "rank":          int(c.get("market_cap_rank") or 0),
        "fdv":           float(c.get("fully_diluted_valuation") or 0),
        "snapshot_date": str(snapshot_date),
        "ingested_at":   datetime.utcnow().isoformat(),
    } for c in raw_data]

    df = pd.DataFrame(rows)
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    df["ingested_at"]   = df["ingested_at"].astype(str)
    df["symbol"]        = df["symbol"].astype(str)

    local_path = f"/tmp/coingecko_{snapshot_date}.parquet"
    df.to_parquet(local_path, index=False)
    log.info(f"Saved {len(df)} rows → {local_path}")

    context["ti"].xcom_push(key="local_path",    value=local_path)
    context["ti"].xcom_push(key="snapshot_date", value=str(snapshot_date))

# ── TASK 3: UPLOAD MARKETS TO GCS ────────────────────────
def upload_to_gcs(**context):
    local_path    = context["ti"].xcom_pull(key="local_path",    task_ids="transform_to_parquet")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="transform_to_parquet")
    bucket_name   = os.environ["GCS_BUCKET_NAME"]
    project_id    = os.environ["GCP_PROJECT_ID"]

    date_parts = snapshot_date.split("-")
    gcs_path = f"raw/coingecko/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/coingecko_{snapshot_date}.parquet"

    client = storage.Client(project=project_id, credentials=get_credentials())
    client.bucket(bucket_name).blob(gcs_path).upload_from_filename(local_path)

    log.info(f"Uploaded → gs://{bucket_name}/{gcs_path}")
    context["ti"].xcom_push(key="gcs_path", value=gcs_path)

# ── TASK 4: BQ LOAD MARKETS ──────────────────────────────
def load_to_bigquery(**context):
    gcs_path      = context["ti"].xcom_pull(key="gcs_path",      task_ids="upload_to_gcs")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="transform_to_parquet")
    bucket_name   = os.environ["GCS_BUCKET_NAME"]
    project_id    = os.environ["GCP_PROJECT_ID"]
    dataset_raw   = os.environ["BQ_DATASET_RAW"]
    table_ref     = f"{project_id}.{dataset_raw}.coingecko_ohlcv"

    client = bigquery.Client(project=project_id, credentials=get_credentials())
    ensure_table_exists(client, table_ref, MARKETS_SCHEMA, clustering_fields=["symbol"])

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=MARKETS_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields=["symbol"],
    )

    load_job = client.load_table_from_uri(f"gs://{bucket_name}/{gcs_path}", table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    log.info(f"Loaded → {table_ref} | rows: {table.num_rows}")

# ── TASK 5: FETCH OHLC ───────────────────────────────────
def fetch_ohlc(**context):
    snapshot_date = context["ds"]
    headers       = {"x-cg-demo-api-key": os.environ["COINGECKO_API_KEY"]}
    all_rows      = []

    for coin_id in COINS:
        try:
            url      = OHLC_URL.format(id=coin_id)
            response = requests.get(url, headers=headers, params={"vs_currency": "usd", "days": 1}, timeout=30)
            response.raise_for_status()
            candles  = response.json()

            # Use ticker symbol (BTC, ETH etc.) instead of full coin name
            ticker = COIN_SYMBOL_MAP.get(coin_id, coin_id.upper())

            for candle in candles:
                ts_ms, open_, high, low, close = candle
                all_rows.append({
                    "symbol":        ticker,
                    "candle_time":   datetime.utcfromtimestamp(ts_ms / 1000).isoformat(),
                    "open":          float(open_),
                    "high":          float(high),
                    "low":           float(low),
                    "close":         float(close),
                    "snapshot_date": str(snapshot_date),
                    "ingested_at":   datetime.utcnow().isoformat(),
                })
            log.info(f"{coin_id} ({ticker}): {len(candles)} candles")
        except Exception as e:
            log.error(f"Failed OHLC for {coin_id}: {e}")

    if not all_rows:
        raise ValueError("No OHLC data fetched")

    df = pd.DataFrame(all_rows)
    for col in ["symbol", "candle_time", "snapshot_date", "ingested_at"]:
        df[col] = df[col].astype(str)

    local_path = f"/tmp/ohlc_{snapshot_date}.parquet"
    df.to_parquet(local_path, index=False)
    log.info(f"Saved {len(df)} OHLC rows → {local_path}")

    context["ti"].xcom_push(key="ohlc_local_path", value=local_path)
    context["ti"].xcom_push(key="snapshot_date",   value=str(snapshot_date))

# ── TASK 6: UPLOAD OHLC TO GCS ───────────────────────────
def upload_ohlc_to_gcs(**context):
    local_path    = context["ti"].xcom_pull(key="ohlc_local_path", task_ids="fetch_ohlc")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date",   task_ids="fetch_ohlc")
    bucket_name   = os.environ["GCS_BUCKET_NAME"]
    project_id    = os.environ["GCP_PROJECT_ID"]

    date_parts = snapshot_date.split("-")
    gcs_path = f"raw/ohlc/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/ohlc_{snapshot_date}.parquet"

    client = storage.Client(project=project_id, credentials=get_credentials())
    client.bucket(bucket_name).blob(gcs_path).upload_from_filename(local_path)

    log.info(f"Uploaded OHLC → gs://{bucket_name}/{gcs_path}")
    context["ti"].xcom_push(key="ohlc_gcs_path", value=gcs_path)

# ── TASK 7: BQ LOAD OHLC ─────────────────────────────────
def load_ohlc_to_bigquery(**context):
    gcs_path      = context["ti"].xcom_pull(key="ohlc_gcs_path",  task_ids="upload_ohlc_to_gcs")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date",  task_ids="fetch_ohlc")
    bucket_name   = os.environ["GCS_BUCKET_NAME"]
    project_id    = os.environ["GCP_PROJECT_ID"]
    dataset_raw   = os.environ["BQ_DATASET_RAW"]
    table_ref     = f"{project_id}.{dataset_raw}.coingecko_ohlc_candles"

    client = bigquery.Client(project=project_id, credentials=get_credentials())
    ensure_table_exists(client, table_ref, OHLC_SCHEMA, clustering_fields=["symbol"])

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=OHLC_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields=["symbol"],
    )

    load_job = client.load_table_from_uri(f"gs://{bucket_name}/{gcs_path}", table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    log.info(f"Loaded → {table_ref} | rows: {table.num_rows}")

# ── DAG ──────────────────────────────────────────────────
default_args = {
    "owner":            "coinpulse",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="coingecko_batch_etl",
    default_args=default_args,
    description="Fetch CoinGecko market data + OHLC candles → GCS → BigQuery (daily)",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["coinpulse", "batch", "coingecko"],
) as dag:

    t1 = PythonOperator(task_id="fetch_coingecko",       python_callable=fetch_coingecko)
    t2 = PythonOperator(task_id="transform_to_parquet",  python_callable=transform_to_parquet)
    t3 = PythonOperator(task_id="upload_to_gcs",         python_callable=upload_to_gcs)
    t4 = PythonOperator(task_id="load_to_bigquery",      python_callable=load_to_bigquery)
    t5 = PythonOperator(task_id="fetch_ohlc",            python_callable=fetch_ohlc)
    t6 = PythonOperator(task_id="upload_ohlc_to_gcs",    python_callable=upload_ohlc_to_gcs)
    t7 = PythonOperator(task_id="load_ohlc_to_bigquery", python_callable=load_ohlc_to_bigquery)

    t1 >> t2 >> t3 >> t4
    t5 >> t6 >> t7