"""Coinbase Advanced Trade WebSocket messages, shaped like the documented public channels."""

import json

SUBSCRIPTIONS = {
    "channel": "subscriptions",
    "client_id": "",
    "timestamp": "2026-10-15T01:00:00.000000000Z",
    "sequence_num": 0,
    "events": [{"subscriptions": {"level2": ["BTC-USD"], "market_trades": ["BTC-USD"]}}],
}
L2_SNAPSHOT = {
    "channel": "l2_data",
    "client_id": "",
    "timestamp": "2026-10-15T01:00:00.123456789Z",
    "sequence_num": 1,
    "events": [
        {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "updates": [
                {
                    "side": "bid",
                    "event_time": "2026-10-15T01:00:00.100000Z",
                    "price_level": "67012.34",
                    "new_quantity": "0.10000000",
                },
                {
                    "side": "offer",
                    "event_time": "2026-10-15T01:00:00.100000Z",
                    "price_level": "67012.35",
                    "new_quantity": "1.25",
                },
            ],
        }
    ],
}
TRADES = {
    "channel": "market_trades",
    "client_id": "",
    "timestamp": "2026-10-15T01:00:00.200000000Z",
    "sequence_num": 2,
    "events": [
        {
            "type": "update",
            "trades": [
                {
                    "trade_id": "123456",
                    "product_id": "ETH-USD",
                    "price": "2450.10",
                    "size": "0.00012000",
                    "side": "BUY",
                    "time": "2026-10-15T01:00:00.190000Z",
                }
            ],
        }
    ],
}
HEARTBEAT = {
    "channel": "heartbeats",
    "client_id": "",
    "timestamp": "2026-10-15T01:00:01.000000000Z",
    "sequence_num": 3,
    "events": [{"current_time": "2026-10-15 01:00:01 +0000 UTC", "heartbeat_counter": 42}],
}


def raw(message: dict, sequence_num: int | None = None) -> str:
    """Serialize like Coinbase does: compact JSON, optionally with a new sequence number."""
    if sequence_num is not None:
        message = {**message, "sequence_num": sequence_num}
    return json.dumps(message, separators=(",", ":"))


# Coinbase is research-only (ADR 0010): it publishes to research.coinbase.md.*.
L2_TOPIC = "research.coinbase.md.level2"
TRADES_TOPIC = "research.coinbase.md.trades"
HEARTBEATS_TOPIC = "research.coinbase.md.heartbeats"
GAPS_TOPIC = "research.coinbase.md.gaps"


def build_envelopes(text: str, message: dict, received_at_ns: int, awaiting=()):
    """The envelopes the ingestor publishes for one Coinbase message."""
    from simex.ingest.coinbase import exchange_timestamp_ns, message_items
    from simex.ingest.envelope import build_envelope, topics_for

    return [
        build_envelope(
            text,
            item,
            venue="coinbase",
            topics=topics_for("coinbase"),
            received_at_ns=received_at_ns,
            exchange_ts_ns=exchange_timestamp_ns(message),
            awaiting_snapshot=awaiting,
        )
        for item in message_items(message)
    ]
