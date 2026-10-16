"""What one recorded Kafka record looks like on disk (ADR 0013).

`value` is the Kafka record value byte for byte: the whole envelope, with the venue's message
spliced in as its payload (ADR 0007), so replay hands decode() exactly what Kafka held and prices
stay strings (ADR 0004). The other columns are copies of header fields, for queries and for
finding windows without parsing JSON.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import pyarrow as pa

SCHEMA_VERSION = 1

# Topic -> channel directory. md.* carries the live venue only (ADR 0011); research topics are
# never recorded.
TOPIC_CHANNELS = {"md.level2": "level2", "md.trades": "market_trades", "md.gaps": "gaps"}
GAPS_PRODUCT = "all"  # gap records cover several products
RECORDER_TOPIC = "recorder"  # rows the recorder writes itself (type "recorder_gap")

SCHEMA = pa.schema(
    [
        ("kafka_topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("kafka_ts_ms", pa.int64()),
        ("key", pa.binary()),
        ("received_at_ns", pa.int64()),
        ("exchange_ts_ns", pa.int64()),
        ("venue", pa.string()),
        ("channel", pa.string()),
        ("product_id", pa.string()),
        ("type", pa.string()),
        ("snapshot", pa.bool_()),
        ("update_first", pa.int64()),
        ("update_last", pa.int64()),
        ("awaiting_snapshot", pa.bool_()),
        ("update_id", pa.int64()),
        ("aligned", pa.bool_()),
        ("reason", pa.string()),
        ("value", pa.binary()),
    ]
)
COLUMNS = SCHEMA.names

_PRODUCT = re.compile(r"[A-Za-z0-9._-]{1,32}")
_PAYLOAD = b',"payload":'


class Unrecordable(ValueError):
    """A record that can't be placed: not JSON, no timestamp, or an unsafe product name."""


@dataclass(frozen=True)
class Placed:
    """A record's row and where it goes."""

    row: dict
    venue: str
    channel: str
    product: str
    time_ns: int  # decides the file's date and hour


def header(value: bytes) -> dict:
    """The envelope's header fields. Envelopes put the header before the payload, so a large
    snapshot's payload isn't parsed; anything else is parsed whole."""
    cut = value.find(_PAYLOAD)
    try:
        parsed = json.loads(value[:cut] + b"}" if cut > 0 else value)
    except ValueError:
        try:
            parsed = json.loads(value)
        except ValueError as exc:
            raise Unrecordable(f"not JSON: {exc}") from None
    if not isinstance(parsed, dict):
        raise Unrecordable("not a JSON object")
    return parsed


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def place(
    topic: str,
    partition: int,
    offset: int,
    kafka_ts_ms: int | None,
    key: bytes | None,
    value: bytes,
) -> Placed:
    channel = TOPIC_CHANNELS[topic]
    h = header(value)
    kind = str(h.get("type") or "data")
    if channel == "gaps":
        product = GAPS_PRODUCT
        time_ns = _int(h.get("detected_at_ns"))
    else:
        product = h.get("product_id") or (key.decode("utf-8", "replace") if key else "")
        time_ns = _int(h.get("received_at_ns"))
    if time_ns is None and kafka_ts_ms is not None:
        time_ns = kafka_ts_ms * 1_000_000
    if time_ns is None:
        raise Unrecordable("no timestamp")
    if not isinstance(product, str) or not _PRODUCT.fullmatch(product):
        raise Unrecordable(f"unsafe product id {product!r}")
    update_ids = h.get("update_ids")
    first = last = None
    if isinstance(update_ids, list) and len(update_ids) == 2:
        first, last = _int(update_ids[0]), _int(update_ids[1])
    venue = str(h.get("venue") or "")
    row = {
        "kafka_topic": topic,
        "partition": partition,
        "offset": offset,
        "kafka_ts_ms": kafka_ts_ms,
        "key": key,
        "received_at_ns": time_ns,
        "exchange_ts_ns": _int(h.get("exchange_ts_ns")),
        "venue": venue,
        "channel": channel,
        "product_id": None if channel == "gaps" else product,
        "type": kind,
        "snapshot": _bool(h.get("snapshot")),
        "update_first": first,
        "update_last": last,
        "awaiting_snapshot": _bool(h.get("awaiting_snapshot")),
        "update_id": _int(h.get("update_id")),
        "aligned": _bool(h.get("aligned")),
        "reason": None if h.get("reason") is None else str(h.get("reason")),
        "value": value,
    }
    return Placed(row, venue, channel, product, time_ns)
