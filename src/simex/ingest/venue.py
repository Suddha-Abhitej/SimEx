"""The venue-adapter interface: what the ingestor core needs from each exchange (ADR 0011).

The core (simex.ingest.client.Ingestor) owns everything venue-neutral: the connection loop and
backoff, liveness, the resync rate limiter, resync markers and gap records (ADR 0008), snapshot
refresh (ADR 0009), the fault injector, publishing and metrics. An adapter only knows its
exchange: the URL, what to send to subscribe and resync, how to read a message, and its gap rule.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

LEVEL2 = "level2"
TRADES = "market_trades"
HEARTBEATS = "heartbeats"


@dataclass(frozen=True)
class Item:
    """One envelope to publish for a received message (a message may carry several products)."""

    channel: str  # LEVEL2, TRADES or HEARTBEATS
    product_id: str  # normalized, e.g. "BTC-USD"; "" for heartbeats
    sequence_num: int | None = None  # Coinbase's per-connection counter
    update_ids: tuple[int, int] | None = None  # Gemini's (U, u) on level2
    snapshot: bool = False  # level2 only: this message is a full book for product_id


@dataclass(frozen=True)
class Anomaly:
    """Something the venue's gap rule or protocol checks found in a message."""

    kind: str  # "gap", "out_of_order" (Coinbase), or a protocol-drift kind (Gemini)
    products: tuple[str, ...]  # books it affects
    reason: str = ""  # gap reason recorded on the markers and md.gaps
    expected: int | None = None  # sequence_num or update ID expected
    received: int | None = None  # and the one received
    missing: int | None = None  # messages known to be lost; None when it can't be counted
    id_jump: int | None = None  # received - expected, when the IDs aren't a message count
    injected: bool = False  # caused by the dev fault injector
    alarm: bool = True  # drift kinds: log an error (False: expected now and then, log info)


@dataclass
class Parsed:
    """What the core should do with one received message."""

    exchange_ts_ns: int | None = None
    items: list[Item] = field(default_factory=list)
    anomalies: list[Anomaly] = field(default_factory=list)
    subscribed: bool = False  # the venue confirmed our subscriptions
    pong: bool = False  # the answer to our liveness ping
    error: str | None = None  # the venue reported an error
    # The answer to an audit_frame() request: (product, the update ID the levels are at).
    # The raw message is published as the audit's payload (ADR 0012).
    audit: tuple[str, int] | None = None
    invalid: str | None = None  # drop the message after its gap check, for this reason


class VenueAdapter(Protocol):
    venue: str
    default_url: str
    # Channels whose data may be lost when the connection drops (recorded on md.gaps).
    channels: tuple[str, ...]
    # Channels a gap found by check() may have affected.
    gap_channels: tuple[str, ...]
    # Whether audit_frame() works: the venue can return top levels at an exact update ID.
    supports_audit: bool

    def reset(self) -> None:
        """Forget per-connection state; called before each new connection."""

    def connect_frames(self) -> list[str]:
        """Messages to send right after connecting: the subscriptions."""

    def resync_frames(self, products: tuple[str, ...]) -> list[str]:
        """Messages that make the venue send fresh snapshots for these products."""

    def ping_frame(self) -> str | None:
        """An application-level ping to keep traffic flowing, or None if the venue has none."""

    def audit_frame(self, product: str) -> str | None:
        """A one-shot request for the product's top levels at an exact update ID (the book
        audit, ADR 0012), or None if the venue has none."""

    def parse(self, message: dict) -> Parsed:
        """Read one decoded message, run the gap rule, and say what to publish."""

    def note_dropped(self, message: dict) -> None:
        """The fault injector dropped this message; remember it to label the gap it causes."""
