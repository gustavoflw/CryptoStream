"""Finnhub WebSocket producer — publishes live crypto trades to Kafka."""

from __future__ import annotations

import json
import logging
import os
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


class TradeRecord(TypedDict):
    """Schema for a normalized trade event published to Kafka."""

    symbol: str
    price: float
    volume: float
    timestamp: int


class _Producer(Protocol):
    def send(self, topic: str, *, key: str, value: TradeRecord) -> object: ...
    def flush(self) -> None: ...


def _encode_json(value: TradeRecord) -> bytes:
    return json.dumps(value).encode()


def _encode_key(key: str) -> bytes:
    return key.encode()


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
    from kafka import KafkaProducer

    load_dotenv()
    api_key = os.environ["FINNHUB_API_KEY"]
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    kafka_producer: KafkaProducer = KafkaProducer(  # type: ignore[type-arg]
        bootstrap_servers=bootstrap,
        value_serializer=_encode_json,
        key_serializer=_encode_key,
    )

    publisher = TradePublisher(kafka_producer, TOPIC, SYMBOLS)
    log.info("Publishing to %s → topic '%s'", bootstrap, TOPIC)

    ws_app = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={api_key}",
        on_open=publisher.on_open,
        on_message=publisher.on_message,
        on_error=publisher.on_error,
        on_close=publisher.on_close,
    )
    ws_app.run_forever(reconnect=5)
