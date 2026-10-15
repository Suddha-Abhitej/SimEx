"""The SimEx market-data envelope and topics (ADR 0007, ADR 0008, ADR 0011).

The envelope is JSON with a few metadata fields and the original WebSocket text spliced in byte
for byte under "payload". The payload is never re-serialized, so prices and sizes stay the exact
strings the venue sent and no value ever passes through a float.

Version 2 added "type": "data" for market data and "resync" for the markers published on the
level2 topic after a gap. Version 3 adds "venue", and on level2 "snapshot" (this message is a
full book) and "update_ids" (the venue's [first, last] update IDs, or null). Type "audit"
(ADR 0012) carries a venue's top levels at an exact update ID. Consumers must skip types they
don't know and should read payloads through simex.marketdata.decode.

Gemini, the live venue, publishes to md.*. Any other venue publishes to research.<venue>.md.*,
so research data (Coinbase, ADR 0010) can't reach the book or matching engine by mistake.
"""

from __future__ import annotations

import json
from collections.abc import Container, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from .venue import HEARTBEATS, LEVEL2, TRADES, Item

ENVELOPE_VERSION = 3
GAP_RECORD_VERSION = 2
LIVE_VENUE = "gemini"
HEARTBEAT_KEY = "heartbeat"
GAPS_KEY = b"gap"


@dataclass(frozen=True)
class Topics:
    level2: str
    trades: str
    heartbeats: str
    gaps: str

    def for_channel(self, channel: str) -> str:
        return {LEVEL2: self.level2, TRADES: self.trades, HEARTBEATS: self.heartbeats}[channel]

    def for_channels(self, channels: Iterable[str]) -> list[str]:
        """The data topics for these channels, then the gaps topic."""
        return [self.for_channel(channel) for channel in channels] + [self.gaps]


def topics_for(venue: str) -> Topics:
    prefix = "" if venue == LIVE_VENUE else f"research.{venue}."
    return Topics(
        level2=f"{prefix}md.level2",
        trades=f"{prefix}md.trades",
        heartbeats=f"{prefix}md.heartbeats",
        gaps=f"{prefix}md.gaps",
    )


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
    """Parse an RFC 3339 timestamp (up to nanoseconds, Z or offset) to epoch nanoseconds.

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


def _header(**fields: object) -> bytes:
    return json.dumps({"v": ENVELOPE_VERSION, **fields}, separators=(",", ":")).encode()


def build_envelope(
    raw: str | bytes,
    item: Item,
    *,
    venue: str,
    topics: Topics,
    received_at_ns: int,
    exchange_ts_ns: int | None,
    awaiting_snapshot: Container[str] = (),
) -> Envelope:
    """The envelope for one item of a received message, with the raw text as its payload."""
    fields: dict[str, object] = {
        "type": "data",
        "venue": venue,
        "received_at_ns": received_at_ns,
        "exchange_ts_ns": exchange_ts_ns,
        "channel": item.channel,
        "product_id": item.product_id or None,
        "sequence_num": item.sequence_num,
    }
    if item.channel == LEVEL2:
        fields["update_ids"] = list(item.update_ids) if item.update_ids else None
        fields["snapshot"] = item.snapshot
        fields["awaiting_snapshot"] = item.product_id in awaiting_snapshot
    raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    header = _header(**fields)
    key = HEARTBEAT_KEY if item.channel == HEARTBEATS else item.product_id
    return Envelope(
        topic=topics.for_channel(item.channel),
        key=key.encode(),
        value=header[:-1] + b',"payload":' + raw_bytes + b"}",
        channel=item.channel,
        product_id=item.product_id,
        received_at_ns=received_at_ns,
        exchange_ts_ns=exchange_ts_ns,
        sequence_num=item.sequence_num,
    )


def build_resync_marker(
    product_id: str,
    resync_id: str,
    reason: str,
    detected_at_ns: int,
    *,
    venue: str,
    topics: Topics,
) -> Envelope:
    """Tell level2 consumers to discard this product's book and wait for the next snapshot."""
    value = _header(
        type="resync",
        venue=venue,
        received_at_ns=detected_at_ns,
        exchange_ts_ns=None,
        channel=LEVEL2,
        product_id=product_id,
        sequence_num=None,
        resync_id=resync_id,
        reason=reason,
    )
    return Envelope(
        topic=topics.level2,
        key=product_id.encode(),
        value=value,
        channel=LEVEL2,
        product_id=product_id,
        received_at_ns=detected_at_ns,
        exchange_ts_ns=None,
        sequence_num=None,
    )


def build_audit_envelope(
    raw: str | bytes,
    *,
    product_id: str,
    update_id: int,
    aligned: bool,
    reason: str,
    received_at_ns: int,
    venue: str,
    topics: Topics,
) -> Envelope:
    """A book audit on the level2 topic (ADR 0012): the venue's top levels at update_id, with
    the venue's response spliced in byte for byte. Only an aligned audit sits right after the
    message whose last update ID is update_id; books count unaligned ones as unaligned."""
    fields = _header(
        type="audit",
        venue=venue,
        received_at_ns=received_at_ns,
        exchange_ts_ns=None,
        channel=LEVEL2,
        product_id=product_id,
        sequence_num=None,
        update_id=update_id,
        aligned=aligned,
        reason=reason,
    )
    raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    return Envelope(
        topic=topics.level2,
        key=product_id.encode(),
        value=fields[:-1] + b',"payload":' + raw_bytes + b"}",
        channel=LEVEL2,
        product_id=product_id,
        received_at_ns=received_at_ns,
        exchange_ts_ns=None,
        sequence_num=None,
    )


def build_gap_record(
    *,
    venue: str,
    gap_id: str,
    reason: str,
    products: Iterable[str],
    channels: Iterable[str],
    expected_sequence: int | None,
    received_sequence: int | None,
    missing_messages: int | None,
    id_jump: int | None,
    start: tuple[int | None, int | None],
    end: tuple[int | None, int | None],
    detected_at_ns: int,
    resync_id: str | None,
    resync_action: str,
    fault_injected: bool,
) -> bytes:
    """A record for the gaps topic. start and end are (exchange_ts_ns, received_at_ns) of the
    last message before the gap and the first message after it; anything between them was lost.

    missing_messages is null when the count can't be known: Gemini's update IDs are shared by
    every market on the exchange, so id_jump says how far the IDs moved, not how many of this
    product's updates were lost.
    """
    return json.dumps(
        {
            "v": GAP_RECORD_VERSION,
            "type": "gap",
            "venue": venue,
            "gap_id": gap_id,
            "reason": reason,
            "products": list(products),
            "channels": list(channels),
            "expected_sequence": expected_sequence,
            "received_sequence": received_sequence,
            "missing_messages": missing_messages,
            "id_jump": id_jump,
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
