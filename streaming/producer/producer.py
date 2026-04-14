import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

# ── LOGGING ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── CONFIG FROM ENV ──────────────────────────────────────
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
BINANCE_WS_URL = os.environ["BINANCE_WS_URL"]
TOPIC = "crypto-prices"

# Symbol mapping: Binance pair → clean symbol name
SYMBOL_MAP = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "ADAUSDT": "cardano",
}

# ── KAFKA PRODUCER ───────────────────────────────────────
producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "coinpulse-producer",
    "acks": "all",
    "retries": 3,
})

def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")
    else:
        log.debug(f"Delivered {msg.key()} to {msg.topic()}[{msg.partition()}]")

# ── GRACEFUL SHUTDOWN ────────────────────────────────────
shutdown = False

def handle_signal(*_):
    global shutdown
    log.info("Shutdown signal received")
    shutdown = True

# ── MAIN STREAMING LOOP ──────────────────────────────────
async def stream():
    log.info(f"Connecting to Binance WebSocket...")
    log.info(f"Publishing to Redpanda topic: {TOPIC}")

    async for websocket in websockets.connect(BINANCE_WS_URL):
        try:
            log.info("WebSocket connected")
            async for raw_msg in websocket:

                if shutdown:
                    log.info("Shutting down stream loop")
                    return

                try:
                    # Binance format:
                    # {"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "p": "45231.12", ...}}
                    envelope = json.loads(raw_msg)
                    data = envelope.get("data", {})

                    binance_symbol = data.get("s", "")
                    price_str = data.get("p", "")
                    trade_time = data.get("T", None)  # trade timestamp ms

                    if not binance_symbol or not price_str:
                        continue

                    symbol = SYMBOL_MAP.get(binance_symbol)
                    if not symbol:
                        continue

                    try:
                        price_usd = float(price_str)
                    except (ValueError, TypeError):
                        continue

                    # Use Binance trade timestamp if available, else now
                    if trade_time:
                        event_ts = datetime.fromtimestamp(
                            trade_time / 1000, tz=timezone.utc
                        ).isoformat()
                    else:
                        event_ts = datetime.now(timezone.utc).isoformat()

                    message = {
                        "symbol": symbol,
                        "price_usd": price_usd,
                        "event_timestamp": event_ts,
                    }

                    producer.produce(
                        topic=TOPIC,
                        key=symbol,
                        value=json.dumps(message).encode("utf-8"),
                        callback=delivery_report,
                    )
                    producer.poll(0)

                    log.info(f"{symbol}: ${price_usd:,.4f}")

                except json.JSONDecodeError as e:
                    log.warning(f"Failed to parse message: {e}")
                    continue

        except websockets.ConnectionClosed as e:
            if shutdown:
                return
            log.warning(f"WebSocket closed: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
            continue

        except Exception as e:
            if shutdown:
                return
            log.error(f"Unexpected error: {e} — reconnecting in 5s")
            await asyncio.sleep(5)
            continue

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            signal.signal(sig, handle_signal)

    await stream()

    log.info("Flushing remaining messages to Redpanda...")
    producer.flush(timeout=10)
    log.info("Producer shut down cleanly")

if __name__ == "__main__":
    asyncio.run(main())
