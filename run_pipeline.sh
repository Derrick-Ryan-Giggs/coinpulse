#!/bin/bash
cd /mnt/storage/Desktop/coinpulse/airflow
export $(grep -v '^#' /mnt/storage/Desktop/coinpulse/.env | xargs)
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler postgres
sleep 25
docker exec coinpulse-airflow-scheduler airflow dags trigger coingecko_batch_etl
echo "Pipeline triggered for $(date +%Y-%m-%d)"
