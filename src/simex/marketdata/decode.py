"""Decode an ingestor envelope (ADR 0007, v1 to v3) into venue-independent events.

Prices and sizes become Decimal from the exact strings the venue sent (ADR 0004); JSON numbers
are never read as floats. A level2 size of 0 means the level is gone. Level sizes are the full
size at that price, not a change.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal

Level = tuple[Decimal, Decimal]  # (price, size)


@dataclass(frozen=True)
class BookUpdate:
    venue: str
    product_id: str
    snapshot: bool  # True: a full book that replaces everything before it
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    update_ids: tuple[int, int] | None  # the venue's (first, last) IDs, when it has them
    awaiting_snapshot: bool  # sent between a resync marker and the next snapshot: skip it
    exchange_ts_ns: int | None
    received_at_ns: int


@dataclass(frozen=True)
class Trade:
    venue: str
    product_id: str
    trade_id: str
    price: Decimal
    size: Decimal
    aggressor: str | None  # "buy" or "sell" when the venue says which side took liquidity
    exchange_ts_ns: int | None
    received_at_ns: int


@dataclass(frozen=True)
class ResyncMarker:
    """Discard this product's book and wait for the next snapshot (ADR 0008)."""

    venue: str
    product_id: str
    resync_id: str
    reason: str
    received_at_ns: int


@dataclass(frozen=True)
class BookAudit:
    """The venue's own top levels at one update ID, to check the book against (ADR 0012).

    Only an aligned audit sits right after the message whose last update ID is update_id; an
    unaligned one (the stream was already past it, or it couldn't be placed) proves nothing.
    """

    venue: str
    product_id: str
    update_id: int
    aligned: bool
    reason: str  # "aligned", or why not: stream_past, timeout, resync, superseded
    bids: tuple[Level, ...]  # best first, as the venue sent them
    asks: tuple[Level, ...]
    received_at_ns: int


Event = BookUpdate | Trade | ResyncMarker | BookAudit


def _decimal(value: object) -> Decimal:
    if isinstance(value, float):
        raise ValueError("price or size arrived as a float")
    return Decimal(str(value))


def _levels(pairs: object) -> tuple[Level, ...]:
    return tuple((_decimal(p), _decimal(q)) for p, q in pairs or [])


def decode(value: bytes | str) -> list[Event]:
    """Events in one envelope; empty for heartbeats and types this version doesn't know."""
    envelope = json.loads(value, parse_float=Decimal)
    kind = envelope.get("type", "data")  # v1 envelopes had no type
    venue = envelope.get("venue", "coinbase")  # v1 and v2 envelopes were all Coinbase
    product = envelope.get("product_id") or ""
    received = envelope["received_at_ns"]
    if kind == "resync":
        return [ResyncMarker(venue, product, envelope["resync_id"], envelope["reason"], received)]
    if kind == "audit":
        return _audit(envelope, venue, product, received)
    if kind != "data":
        return []
    payload = envelope.get("payload") or {}
    channel = envelope.get("channel")
    if venue == "gemini":
        return _gemini(envelope, payload, channel, product, received)
    if venue == "coinbase":
        return _coinbase(envelope, payload, channel, product, received)
    return []


def _audit(envelope: dict, venue: str, product: str, received: int) -> list:
    payload = envelope.get("payload") or {}
    if venue != "gemini":
        return []
    result = payload.get("result") or {}  # Gemini's depth response
    return [
        BookAudit(
            venue=venue,
            product_id=product,
            update_id=int(envelope["update_id"]),
            aligned=bool(envelope.get("aligned")),
            reason=str(envelope.get("reason", "")),
            bids=_levels(result.get("bids")),
            asks=_levels(result.get("asks")),
            received_at_ns=received,
        )
    ]


def _gemini(envelope: dict, payload: dict, channel: str, product: str, received: int) -> list:
    exchange_ts = payload.get("E")
    if channel == "level2":
        update_ids = envelope.get("update_ids")
        return [
            BookUpdate(
                venue="gemini",
                product_id=product,
                snapshot=bool(envelope.get("snapshot")),
                bids=_levels(payload.get("b")),
                asks=_levels(payload.get("a")),
                update_ids=tuple(update_ids) if update_ids else None,
                awaiting_snapshot=bool(envelope.get("awaiting_snapshot")),
                exchange_ts_ns=exchange_ts,
                received_at_ns=received,
            )
        ]
    if channel == "market_trades":
        # m: the buyer was the maker, so the seller took liquidity.
        maker_buyer = payload.get("m")
        aggressor = None if maker_buyer is None else ("sell" if maker_buyer else "buy")
        return [
            Trade(
                venue="gemini",
                product_id=product,
                trade_id=str(payload["t"]),
                price=_decimal(payload["p"]),
                size=_decimal(payload["q"]),
                aggressor=aggressor,
                exchange_ts_ns=exchange_ts,
                received_at_ns=received,
            )
        ]
    return []


def _coinbase(envelope: dict, payload: dict, channel: str, product: str, received: int) -> list:
    # Research only (ADR 0010). A message may carry several products; the envelope names the one
    # it was published for, so only that product's events are read.
    exchange_ts = envelope.get("exchange_ts_ns")
    events = [e for e in payload.get("events") or [] if isinstance(e, dict)]
    if channel == "level2":
        mine = [e for e in events if e.get("product_id") == product]
        if not mine:
            return []
        updates = [u for e in mine for u in e.get("updates") or []]
        snapshot = envelope.get("snapshot")
        if snapshot is None:  # v1 and v2 envelopes: read it from the payload
            snapshot = any(e.get("type") == "snapshot" for e in mine)
        return [
            BookUpdate(
                venue="coinbase",
                product_id=product,
                snapshot=bool(snapshot),
                bids=tuple(
                    (_decimal(u["price_level"]), _decimal(u["new_quantity"]))
                    for u in updates
                    if u.get("side") == "bid"
                ),
                asks=tuple(
                    (_decimal(u["price_level"]), _decimal(u["new_quantity"]))
                    for u in updates
                    if u.get("side") in ("offer", "ask")
                ),
                update_ids=None,
                awaiting_snapshot=bool(envelope.get("awaiting_snapshot")),
                exchange_ts_ns=exchange_ts,
                received_at_ns=received,
            )
        ]
    if channel == "market_trades":
        return [
            Trade(
                venue="coinbase",
                product_id=product,
                trade_id=str(t["trade_id"]),
                price=_decimal(t["price"]),
                size=_decimal(t["size"]),
                aggressor=None,  # Coinbase's "side" field is not documented clearly enough
                exchange_ts_ns=exchange_ts,
                received_at_ns=received,
            )
            for e in events
            for t in e.get("trades") or []
            if isinstance(t, dict) and t.get("product_id") == product
        ]
    return []


class TradeDeduper:
    """Remembers recent trade IDs per (venue, product). Gemini's trade IDs are exchange-wide and
    not always in order, so dedupe by ID and never assume ordering (ADR 0011)."""

    def __init__(self, capacity: int = 10_000) -> None:
        self.capacity = capacity
        self._seen: dict[tuple[str, str], OrderedDict[str, None]] = {}

    def is_new(self, trade: Trade) -> bool:
        seen = self._seen.setdefault((trade.venue, trade.product_id), OrderedDict())
        if trade.trade_id in seen:
            return False
        seen[trade.trade_id] = None
        if len(seen) > self.capacity:
            seen.popitem(last=False)
        return True
