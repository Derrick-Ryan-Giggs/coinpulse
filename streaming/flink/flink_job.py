import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.datastream.functions import WindowFunction, MapFunction
from pyflink.common.watermark_strategy import TimestampAssigner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FLINK] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────
KAFKA_BROKER    = os.environ["KAFKA_BROKER"]
GCP_PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET_NAME = os.environ["GCS_BUCKET_NAME"]
GCP_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
TOPIC           = "crypto-prices"

# ── PARSE ────────────────────────────────────────────────
def parse_message(raw: str):
    try:
        msg = json.loads(raw)
        return {
            "symbol":          msg["symbol"],
            "price_usd":       float(msg["price_usd"]),
            "event_timestamp": msg["event_timestamp"],
        }
    except Exception:
        return None

# ── WATERMARK ASSIGNER ───────────────────────────────────
# Must be a class implementing TimestampAssigner — not a bare function.
class CryptoTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        try:
            return int(
                datetime.fromisoformat(
                    value["event_timestamp"].replace("Z", "+00:00")
                ).timestamp() * 1000
            )
        except Exception:
            return 0

# ── WINDOW AGGREGATION ───────────────────────────────────
class CryptoWindowFunction(WindowFunction):
    def apply(self, key, window, inputs):
        inputs = list(inputs)
        prices = [item["price_usd"] for item in inputs]
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
            stddev   = variance ** 0.5

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

# ── GCS SINK ─────────────────────────────────────────────
class GCSWriterMap(MapFunction):
    def __init__(self):
        self._bucket = None

    def _get_bucket(self):
        if self._bucket is None:
            from google.cloud import storage
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                GCP_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/devstorage.read_write"],
            )
            client = storage.Client(project=GCP_PROJECT_ID, credentials=creds)
            self._bucket = client.bucket(GCS_BUCKET_NAME)
            log.info(f"GCS client initialised → gs://{GCS_BUCKET_NAME}/streaming/")
        return self._bucket

    def map(self, value: dict) -> dict:
        try:
            bucket = self._get_bucket()

            window_end_dt = datetime.fromisoformat(
                value["window_end"].replace("Z", "+00:00")
            )
            path = (
                f"streaming/"
                f"{window_end_dt.strftime('%Y/%m/%d/%H')}/"
                f"stream_{value['symbol']}_{window_end_dt.strftime('%Y%m%d_%H%M%S')}.jsonl"
            )

            content = (json.dumps(value) + "\n").encode("utf-8")
            bucket.blob(path).upload_from_file(
                BytesIO(content), content_type="application/jsonl"
            )

            log.info(
                f"GCS ✓ {value['symbol']} "
                f"avg=${value['avg_price']:,.4f} "
                f"n={value['record_count']} "
                f"→ {path}"
            )
        except Exception as e:
            log.error(f"GCS write failed for {value.get('symbol')}: {e}")

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

    parsed = (
        env
        .from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Redpanda Source")
        .map(parse_message)
        .filter(lambda x: x is not None)
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(CryptoTimestampAssigner())  
    )

    (
        parsed
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda x: x["symbol"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .apply(CryptoWindowFunction())
        .map(GCSWriterMap())
    )

    log.info("Starting CoinPulse Flink job → GCS sink")
    env.execute("CoinPulse Crypto Stream")

if __name__ == "__main__":
    main()