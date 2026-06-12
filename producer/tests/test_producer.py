import json
from unittest.mock import MagicMock, call

import pytest

from producer.producer import TradePublisher, _encode_json, _encode_key

SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]


@pytest.fixture
def publisher() -> tuple[TradePublisher, MagicMock]:
    mock_kafka = MagicMock()
    return TradePublisher(mock_kafka, "trades", SYMBOLS), mock_kafka


def test_on_message_publishes_trade(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, kafka = publisher
    raw = json.dumps({
        "type": "trade",
        "data": [{"s": "BINANCE:BTCUSDT", "p": 50000.0, "v": 0.1, "t": 1_000_000}],
    })
    pub.on_message(MagicMock(), raw)
    kafka.send.assert_called_once_with(
        "trades",
        key="BINANCE:BTCUSDT",
        value={
            "symbol": "BINANCE:BTCUSDT",
            "price": 50000.0,
            "volume": 0.1,
            "timestamp": 1_000_000,
        },
    )


def test_on_message_ignores_non_trade(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, kafka = publisher
    pub.on_message(MagicMock(), json.dumps({"type": "ping"}))
    kafka.send.assert_not_called()


def test_on_message_batch(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, kafka = publisher
    raw = json.dumps({
        "type": "trade",
        "data": [
            {"s": "BINANCE:BTCUSDT", "p": 50000.0, "v": 0.1, "t": 1_000_000},
            {"s": "BINANCE:ETHUSDT", "p": 3000.0, "v": 0.5, "t": 1_000_001},
        ],
    })
    pub.on_message(MagicMock(), raw)
    assert kafka.send.call_count == 2


def test_on_open_subscribes_to_all_symbols(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, _ = publisher
    ws = MagicMock()
    pub.on_open(ws)
    expected = [call(json.dumps({"type": "subscribe", "symbol": s})) for s in SYMBOLS]
    ws.send.assert_has_calls(expected, any_order=False)


def test_on_close_flushes_producer(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, kafka = publisher
    pub.on_close(MagicMock(), 1000, "normal")
    kafka.flush.assert_called_once()


def test_on_error_logs(publisher: tuple[TradePublisher, MagicMock]) -> None:
    pub, _ = publisher
    pub.on_error(MagicMock(), ValueError("connection reset"))


def test_encode_json_returns_bytes() -> None:
    record = {"symbol": "BINANCE:BTCUSDT", "price": 50000.0, "volume": 0.1, "timestamp": 1_000_000}
    result = _encode_json(record)
    assert result == json.dumps(record).encode()


def test_encode_key_returns_bytes() -> None:
    assert _encode_key("BINANCE:BTCUSDT") == b"BINANCE:BTCUSDT"
