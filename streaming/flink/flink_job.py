import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import WindowFunction, MapFunction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FLINK] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── CONFIG FROM ENV ──────────────────────────────────────
KAFKA_BROKER    = os.environ["KAFKA_BROKER"]
GCP_PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
BQ_DATASET_RAW  = os.environ["BQ_DATASET_RAW"]
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
GCP_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
TOPIC           = "crypto-prices"

# ── PARSE FUNCTION ───────────────────────────────────────
def parse_message(raw: str):
    try:
        msg = json.loads(raw)
        return (
            msg["symbol"],
            float(msg["price_usd"]),
            msg["event_timestamp"],
        )
    except Exception:
        return None

# ── WINDOW AGGREGATION ───────────────────────────────────
class CryptoWindowFunction(WindowFunction):
    def apply(self, key, window, inputs):
        # PyFlink 2.2.0 — no collector arg, use yield instead
        inputs = list(inputs)
        prices = [item[1] for item in inputs]

        if not prices:
            return

        avg_price   = sum(prices) / len(prices)
        min_price   = min(prices)
        max_price   = max(prices)
        open_price  = prices[0]
        close_price = prices[-1]
        count       = len(prices)

        stddev = 0.0
        if count > 1:
            variance = sum((p - avg_price) ** 2 for p in prices) / count
            stddev = variance ** 0.5

        window_start = datetime.fromtimestamp(
            window.start / 1000, tz=timezone.utc
        ).isoformat()
        window_end = datetime.fromtimestamp(
            window.end / 1000, tz=timezone.utc
        ).isoformat()

        yield {
            "symbol":          key,
            "price_usd":       close_price,
            "avg_price":       round(avg_price, 6),
            "min_price":       round(min_price, 6),
            "max_price":       round(max_price, 6),
            "price_stddev":    round(stddev, 6),
            "open_price":      round(open_price, 6),
            "close_price":     round(close_price, 6),
            "record_count":    count,
            "window_start":    window_start,
            "window_end":      window_end,
            "event_timestamp": window_end,
        }

# ── GCS SINK VIA MapFunction ─────────────────────────────
class GCSWriterMap(MapFunction):
    def __init__(self):
        self._client = None
        self._bucket = None

    def _get_client(self):
        if self._client is None:
            from google.cloud import storage
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_file(
                GCP_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
            )
            self._client = storage.Client(
                project=GCP_PROJECT_ID,
                credentials=credentials
            )
            self._bucket = self._client.bucket(GCS_BUCKET_NAME)
            log.info(f"GCS client initialized → gs://{GCS_BUCKET_NAME}/streaming/")
        return self._client, self._bucket

    def map(self, value):
        try:
            client, bucket = self._get_client()
            now = datetime.now(timezone.utc)
            path = (
                f"streaming/"
                f"{now.strftime('%Y/%m/%d/%H')}/"
                f"stream_{value['symbol']}_{now.strftime('%Y%m%d_%H%M%S%f')}.jsonl"
            )
            content = json.dumps(value).encode("utf-8")
            blob = bucket.blob(path)
            blob.upload_from_file(BytesIO(content), content_type="application/jsonl")
            log.info(
                f"Written {value['symbol']} "
                f"avg=${value['avg_price']:,.4f} "
                f"→ gs://{GCS_BUCKET_NAME}/{path}"
            )
        except Exception as e:
            log.error(f"GCS write failed: {e}")
        return value

# ── MAIN ─────────────────────────────────────────────────
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.get_config().set_auto_watermark_interval(1000)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(TOPIC)
        .set_group_id("coinpulse-flink-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(
            lambda event, _: (
                int(datetime.fromisoformat(
                    event[2].replace("Z", "+00:00")
                ).timestamp() * 1000)
                if event else 0
            )
        )
    )

    (
        env
        .from_source(kafka_source, watermark_strategy, "Redpanda Source")
        .map(parse_message)
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .apply(CryptoWindowFunction())
        .map(GCSWriterMap())
    )

    log.info("Starting CoinPulse Flink job → GCS sink")
    env.execute("CoinPulse Crypto Stream")

if __name__ == "__main__":
    main()
