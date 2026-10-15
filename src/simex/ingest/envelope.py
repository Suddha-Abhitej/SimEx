"""Wrap raw Coinbase messages in the SimEx envelope published to Kafka (ADR 0007, ADR 0008).

The envelope is JSON with a few metadata fields and the original WebSocket text spliced in
byte for byte under "payload". The payload is never re-serialized, so prices and sizes stay the
exact strings Coinbase sent and no value ever passes through a float.

Version 2 adds "type": "data" for market data and "resync" for the markers published on
md.level2 after a gap. Consumers must skip types they don't know. Gap records go to md.gaps.
"""

from __future__ import annotations

import json
from collections.abc import Container, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

ENVELOPE_VERSION = 2
GAP_RECORD_VERSION = 1
HEARTBEAT_KEY = "heartbeat"
GAPS_TOPIC = "md.gaps"
GAPS_KEY = b"gap"

# Coinbase names a subscription channel differently from the "channel" field of its messages.
MESSAGE_CHANNELS = {
    "l2_data": "level2",
    "market_trades": "market_trades",
    "heartbeats": "heartbeats",
}
TOPICS = {
    "level2": "md.level2",
    "market_trades": "md.trades",
    "heartbeats": "md.heartbeats",
}


@dataclass(frozen=True)
class Envelope:
    topic: str
    key: bytes
    value: bytes
    channel: str
    product_id: str
    received_at_ns: int
    exchange_ts_ns: int | None
    sequence_num: int | None

    @property
    def lag_ns(self) -> int | None:
        return feed_lag_ns(self.received_at_ns, self.exchange_ts_ns)


def parse_timestamp_ns(value: str) -> int:
    """Parse a Coinbase RFC 3339 timestamp (up to nanoseconds, Z or offset) to epoch nanoseconds.

    datetime only keeps microseconds, so the fractional part is handled separately and exactly.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text, offset = text[:-1], "+00:00"
    elif len(text) > 6 and text[-6] in "+-" and text[-3] == ":":
        text, offset = text[:-6], text[-6:]
    else:
        raise ValueError(f"timestamp without a UTC offset: {value!r}")
    whole, _, fraction = text.partition(".")
    if fraction and not fraction.isdigit():
        raise ValueError(f"bad fractional seconds in {value!r}")
    seconds = datetime.fromisoformat(whole + offset).astimezone(UTC)
    nanos = int((fraction + "000000000")[:9]) if fraction else 0
    return int(seconds.timestamp()) * 1_000_000_000 + nanos


def feed_lag_ns(received_at_ns: int, exchange_ts_ns: int | None) -> int | None:
    """Local receive time minus exchange send time. Negative values mean the clocks disagree."""
    if exchange_ts_ns is None:
        return None
    return received_at_ns - exchange_ts_ns


def product_ids(message: dict) -> list[str]:
    """Every product a market-data message carries, in first-seen order."""
    seen: dict[str, None] = {}
    for event in message.get("events") or []:
        if not isinstance(event, dict):
            continue
        if product := event.get("product_id"):
            seen[product] = None
        for trade in event.get("trades") or []:
            if isinstance(trade, dict) and (product := trade.get("product_id")):
                seen[product] = None
    return list(seen)


def snapshot_products(message: dict) -> list[str]:
    """Products that a level2 message carries a full snapshot for."""
    if MESSAGE_CHANNELS.get(message.get("channel", "")) != "level2":
        return []
    return [
        event["product_id"]
        for event in message.get("events") or []
        if isinstance(event, dict) and event.get("type") == "snapshot" and event.get("product_id")
    ]


def exchange_timestamp_ns(message: dict) -> int | None:
    timestamp = message.get("timestamp")
    return parse_timestamp_ns(timestamp) if isinstance(timestamp, str) else None


def _header(**fields: object) -> bytes:
    return json.dumps({"v": ENVELOPE_VERSION, **fields}, separators=(",", ":")).encode()


def build_envelopes(
    raw: str | bytes,
    message: dict,
    received_at_ns: int,
    awaiting_snapshot: Container[str] = (),
) -> list[Envelope]:
    """Envelopes to publish for one WebSocket message; empty if it is not market data.

    A message normally carries one product. If it carries several, the unchanged message is
    published once per product, keyed by that product, so every product's partition sees it.
    Level2 envelopes say whether their product is still waiting for a snapshot after a gap.
    """
    channel = MESSAGE_CHANNELS.get(message.get("channel", ""))
    if channel is None:
        return []

    exchange_ts_ns = exchange_timestamp_ns(message)
    sequence_num = message.get("sequence_num")
    if not isinstance(sequence_num, int) or isinstance(sequence_num, bool):
        sequence_num = None

    raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    if channel == "heartbeats":
        keys = [HEARTBEAT_KEY]
    else:
        keys = product_ids(message) or [""]

    envelopes = []
    for product in keys:
        product_id = "" if channel == "heartbeats" else product
        fields: dict[str, object] = {
            "type": "data",
            "received_at_ns": received_at_ns,
            "exchange_ts_ns": exchange_ts_ns,
            "channel": channel,
            "product_id": product_id or None,
            "sequence_num": sequence_num,
        }
        if channel == "level2":
            fields["awaiting_snapshot"] = product_id in awaiting_snapshot
        header = _header(**fields)
        value = header[:-1] + b',"payload":' + raw_bytes + b"}"
        envelopes.append(
            Envelope(
                topic=TOPICS[channel],
                key=product.encode(),
                value=value,
                channel=channel,
                product_id=product_id,
                received_at_ns=received_at_ns,
                exchange_ts_ns=exchange_ts_ns,
                sequence_num=sequence_num,
            )
        )
    return envelopes


def build_resync_marker(
    product_id: str, resync_id: str, reason: str, detected_at_ns: int
) -> Envelope:
    """Tell md.level2 consumers to discard this product's book and wait for the next snapshot."""
    value = _header(
        type="resync",
        received_at_ns=detected_at_ns,
        exchange_ts_ns=None,
        channel="level2",
        product_id=product_id,
        sequence_num=None,
        resync_id=resync_id,
        reason=reason,
    )
    return Envelope(
        topic=TOPICS["level2"],
        key=product_id.encode(),
        value=value,
        channel="level2",
        product_id=product_id,
        received_at_ns=detected_at_ns,
        exchange_ts_ns=None,
        sequence_num=None,
    )


def build_gap_record(
    *,
    gap_id: str,
    reason: str,
    products: Iterable[str],
    channels: Iterable[str],
    expected_sequence: int | None,
    received_sequence: int | None,
    start: tuple[int | None, int | None],
    end: tuple[int | None, int | None],
    detected_at_ns: int,
    resync_id: str | None,
    resync_action: str,
    fault_injected: bool,
) -> bytes:
    """A record for md.gaps. start and end are (exchange_ts_ns, received_at_ns) of the last
    message before the gap and the first message after it; anything between them was lost."""
    missing = (
        received_sequence - expected_sequence
        if expected_sequence is not None and received_sequence is not None
        else None
    )
    return json.dumps(
        {
            "v": GAP_RECORD_VERSION,
            "type": "gap",
            "gap_id": gap_id,
            "reason": reason,
            "products": list(products),
            "channels": list(channels),
            "expected_sequence": expected_sequence,
            "received_sequence": received_sequence,
            "missing_messages": missing,
            "start_exchange_ts_ns": start[0],
            "start_received_at_ns": start[1],
            "end_exchange_ts_ns": end[0],
            "end_received_at_ns": end[1],
            "detected_at_ns": detected_at_ns,
            "resync_id": resync_id,
            "resync_action": resync_action,
            "fault_injected": fault_injected,
        },
        separators=(",", ":"),
    ).encode()
