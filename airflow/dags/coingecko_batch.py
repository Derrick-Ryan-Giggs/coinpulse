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

GCP_PROJECT_ID    = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET_NAME   = os.environ["GCS_BUCKET_NAME"]
BQ_DATASET_RAW    = os.environ["BQ_DATASET_RAW"]
GCP_CREDENTIALS   = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
COINGECKO_API_KEY = os.environ["COINGECKO_API_KEY"]

COINS            = ["bitcoin", "ethereum", "solana", "binancecoin", "cardano"]
COINGECKO_URL    = "https://api.coingecko.com/api/v3/coins/markets"
OHLC_URL         = "https://api.coingecko.com/api/v3/coins/{id}/ohlc"
BQ_TABLE_MARKETS = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.coingecko_ohlcv"
BQ_TABLE_OHLC    = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.coingecko_ohlc_candles"

def get_gcp_credentials():
    return service_account.Credentials.from_service_account_file(
        GCP_CREDENTIALS,
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
        ]
    )

def fetch_coingecko(**context):
    log.info("Fetching market data from CoinGecko...")
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}
    params = {
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
    log.info(f"Fetched {len(data)} coins from CoinGecko")
    context["ti"].xcom_push(key="raw_data", value=data)

def transform_to_parquet(**context):
    raw_data      = context["ti"].xcom_pull(key="raw_data", task_ids="fetch_coingecko")
    snapshot_date = context["ds"]

    rows = []
    for coin in raw_data:
        rows.append({
            "symbol":        coin.get("symbol", "").upper(),
            "name":          coin.get("name", ""),
            "price_usd":     float(coin.get("current_price") or 0),
            "market_cap":    float(coin.get("market_cap") or 0),
            "volume_24h":    float(coin.get("total_volume") or 0),
            "price_change":  float(coin.get("price_change_percentage_24h") or 0),
            "rank":          int(coin.get("market_cap_rank") or 0),
            "fdv":           float(coin.get("fully_diluted_valuation") or 0),
            "snapshot_date": snapshot_date,
            "ingested_at":   datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(rows)
    log.info(f"Transformed {len(df)} rows for {snapshot_date}")

    local_path = f"/tmp/coingecko_{snapshot_date}.parquet"
    df.to_parquet(local_path, index=False)

    context["ti"].xcom_push(key="local_path", value=local_path)
    context["ti"].xcom_push(key="snapshot_date", value=snapshot_date)

def upload_to_gcs(**context):
    local_path    = context["ti"].xcom_pull(key="local_path", task_ids="transform_to_parquet")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="transform_to_parquet")

    gcs_bucket = os.environ["GCS_BUCKET_NAME"]
    date_parts = snapshot_date.split("-")
    gcs_path = f"raw/coingecko/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/coingecko_{snapshot_date}.parquet"

    credentials = get_gcp_credentials()
    client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=credentials)
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    log.info(f"Uploaded → gs://{gcs_bucket}/{gcs_path}")
    context["ti"].xcom_push(key="gcs_path", value=gcs_path)

def load_to_bigquery(**context):
    gcs_path      = context["ti"].xcom_pull(key="gcs_path", task_ids="upload_to_gcs")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="transform_to_parquet")
    gcs_bucket    = os.environ["GCS_BUCKET_NAME"]
    gcs_uri       = f"gs://{gcs_bucket}/{gcs_path}"

    credentials = get_gcp_credentials()
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], credentials=credentials)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="snapshot_date",
        ),
        clustering_fields=["symbol"],
    )

    load_job = client.load_table_from_uri(gcs_uri, BQ_TABLE_MARKETS, job_config=job_config)
    load_job.result()
    table = client.get_table(BQ_TABLE_MARKETS)
    log.info(f"BQ Load complete → {BQ_TABLE_MARKETS} | rows: {table.num_rows} | partitioned: DAY | clustered: symbol")

def fetch_ohlc(**context):
    snapshot_date = context["ds"]
    headers = {"x-cg-demo-api-key": os.environ["COINGECKO_API_KEY"]}
    all_rows = []

    for coin_id in COINS:
        log.info(f"Fetching OHLC for {coin_id}...")
        url = OHLC_URL.format(id=coin_id)
        params = {"vs_currency": "usd", "days": 1}

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            candles = response.json()

            for candle in candles:
                ts_ms, open_, high, low, close = candle
                all_rows.append({
                    "symbol":        coin_id.upper(),
                    "candle_time":   datetime.utcfromtimestamp(ts_ms / 1000).isoformat(),
                    "open":          float(open_),
                    "high":          float(high),
                    "low":           float(low),
                    "close":         float(close),
                    "snapshot_date": snapshot_date,
                    "ingested_at":   datetime.utcnow().isoformat(),
                })
            log.info(f"Got {len(candles)} candles for {coin_id}")

        except Exception as e:
            log.error(f"Failed to fetch OHLC for {coin_id}: {e}")
            continue

    if not all_rows:
        raise ValueError("No OHLC data fetched")

    df = pd.DataFrame(all_rows)
    local_path = f"/tmp/ohlc_{snapshot_date}.parquet"
    df.to_parquet(local_path, index=False)

    context["ti"].xcom_push(key="ohlc_local_path", value=local_path)
    context["ti"].xcom_push(key="snapshot_date", value=snapshot_date)

def upload_ohlc_to_gcs(**context):
    local_path    = context["ti"].xcom_pull(key="ohlc_local_path", task_ids="fetch_ohlc")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="fetch_ohlc")
    gcs_bucket    = os.environ["GCS_BUCKET_NAME"]

    date_parts = snapshot_date.split("-")
    gcs_path = f"raw/ohlc/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}/ohlc_{snapshot_date}.parquet"

    credentials = get_gcp_credentials()
    client = storage.Client(project=os.environ["GCP_PROJECT_ID"], credentials=credentials)
    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    log.info(f"Uploaded OHLC → gs://{gcs_bucket}/{gcs_path}")
    context["ti"].xcom_push(key="ohlc_gcs_path", value=gcs_path)

def load_ohlc_to_bigquery(**context):
    gcs_path      = context["ti"].xcom_pull(key="ohlc_gcs_path", task_ids="upload_ohlc_to_gcs")
    snapshot_date = context["ti"].xcom_pull(key="snapshot_date", task_ids="fetch_ohlc")
    gcs_bucket    = os.environ["GCS_BUCKET_NAME"]
    gcs_uri       = f"gs://{gcs_bucket}/{gcs_path}"

    credentials = get_gcp_credentials()
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"], credentials=credentials)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields=["symbol"],
    )

    load_job = client.load_table_from_uri(gcs_uri, BQ_TABLE_OHLC, job_config=job_config)
    load_job.result()
    table = client.get_table(BQ_TABLE_OHLC)
    log.info(f"BQ Load complete → {BQ_TABLE_OHLC} | rows: {table.num_rows} | clustered: symbol")

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

    t1 = PythonOperator(task_id="fetch_coingecko",      python_callable=fetch_coingecko)
    t2 = PythonOperator(task_id="transform_to_parquet",  python_callable=transform_to_parquet)
    t3 = PythonOperator(task_id="upload_to_gcs",         python_callable=upload_to_gcs)
    t4 = PythonOperator(task_id="load_to_bigquery",      python_callable=load_to_bigquery)
    t5 = PythonOperator(task_id="fetch_ohlc",            python_callable=fetch_ohlc)
    t6 = PythonOperator(task_id="upload_ohlc_to_gcs",    python_callable=upload_ohlc_to_gcs)
    t7 = PythonOperator(task_id="load_ohlc_to_bigquery", python_callable=load_ohlc_to_bigquery)

    t1 >> t2 >> t3 >> t4
    t5 >> t6 >> t7
