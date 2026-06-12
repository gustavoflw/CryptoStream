"""Finnhub WebSocket producer — publishes live crypto trades to Kafka."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypedDict

import websocket
from dotenv import load_dotenv

if TYPE_CHECKING:
    from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

TOPIC = "trades"
SYMBOLS: list[str] = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:SOLUSDT",
    "BINANCE:BNBUSDT",
    "BINANCE:XRPUSDT",
]

_SCHEMA_PATH = Path(__file__).parent / "schemas" / "trade.avsc"


class TradeRecord(TypedDict):
    """Schema for a normalized trade event published to Kafka."""

    symbol: str
    price: float
    volume: float
    timestamp: int


class _Producer(Protocol):
    def send(self, topic: str, *, key: str, value: TradeRecord) -> object: ...
    def flush(self) -> None: ...


class TradePublisher:
    """Handles Finnhub WebSocket events and publishes trades to Kafka."""

    def __init__(
        self,
        kafka_producer: _Producer,
        topic: str,
        symbols: list[str],
    ) -> None:
        """Initialise with a Kafka producer, target topic, and symbol list."""
        self.producer = kafka_producer
        self.topic = topic
        self.symbols = symbols

    def on_open(self, _ws: websocket.WebSocketApp) -> None:
        """Subscribe to all configured symbols once the connection is open."""
        log.info("Connected — subscribing to %d symbols", len(self.symbols))
        for symbol in self.symbols:
            _ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))

    def on_message(self, _ws: websocket.WebSocketApp, raw: str) -> None:
        """Parse a Finnhub message and publish each trade event to Kafka."""
        msg = json.loads(raw)
        if msg.get("type") != "trade":
            return
        for t in msg["data"]:
            record: TradeRecord = {
                "symbol": t["s"],
                "price": t["p"],
                "volume": t["v"],
                "timestamp": t["t"],
            }
            self.producer.send(self.topic, key=record["symbol"], value=record)
            log.info("%s  price=%.2f  vol=%s", record["symbol"], record["price"], record["volume"])

    def on_error(self, _ws: websocket.WebSocketApp, error: Exception) -> None:
        """Log WebSocket errors."""
        log.error("WebSocket error: %s", error)

    def on_close(self, _ws: websocket.WebSocketApp, code: int | None, _msg: str | None) -> None:
        """Flush the Kafka producer on connection close."""
        self.producer.flush()
        log.info("Connection closed (%s)", code)


if __name__ == "__main__":  # pragma: no cover
    import io
    import struct

    import fastavro
    import requests
    from kafka import KafkaProducer

    load_dotenv()
    api_key = os.environ["FINNHUB_API_KEY"]
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    # Register schema and obtain its ID from the registry.
    schema_str = _SCHEMA_PATH.read_text()
    resp = requests.post(
        f"{registry_url}/subjects/{TOPIC}-value/versions",
        json={"schema": schema_str},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )
    resp.raise_for_status()
    schema_id: int = resp.json()["id"]

    parsed_schema = fastavro.parse_schema(json.loads(schema_str))

    def _avro_serializer(record: TradeRecord) -> bytes:
        """Encode a record using the Confluent wire format: magic + schema ID + Avro binary."""
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        fastavro.schemaless_writer(buf, parsed_schema, record)
        return buf.getvalue()

    kafka_producer: KafkaProducer = KafkaProducer(  # type: ignore[type-arg]
        bootstrap_servers=bootstrap,
        value_serializer=_avro_serializer,
        key_serializer=str.encode,
    )

    publisher = TradePublisher(kafka_producer, TOPIC, SYMBOLS)
    log.info("Publishing to %s → topic '%s' (schema_id=%d)", bootstrap, TOPIC, schema_id)

    ws_app = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={api_key}",
        on_open=publisher.on_open,
        on_message=publisher.on_message,
        on_error=publisher.on_error,
        on_close=publisher.on_close,
    )
    ws_app.run_forever(reconnect=5)
