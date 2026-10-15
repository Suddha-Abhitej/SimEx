"""The book record published to Valkey for readers (MCP quotes, dashboard). ADR 0012.

One JSON string per product under simex:book:<venue>:<product_id>, written with a TTL so a dead
service's books vanish. Prices and sizes are Decimal strings (ADR 0004).

A reader may use a book only if all hold:
- the key exists (a missing key means the service is down or the key was evicted);
- state == "SYNCED" and catching_up is false;
- now - received_at_ns <= the stale threshold (5 s): received_at_ns is when the ingestor received
  the last update applied to this book, on the same host clock as the reader in production.
"""

from __future__ import annotations

import json
from decimal import Decimal

from .orderbook import OrderBook

RECORD_VERSION = 1


def book_key(venue: str, product_id: str) -> str:
    return f"simex:book:{venue}:{product_id}"


def _level(level: tuple[Decimal, Decimal] | None) -> list[str] | None:
    return None if level is None else [str(level[0]), str(level[1])]


def _decimal(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def book_record(book: OrderBook, *, depth: int, published_at_ns: int, catching_up: bool) -> bytes:
    bid_count, ask_count = book.level_counts()
    record: dict[str, object] = {
        "v": RECORD_VERSION,
        "venue": book.venue,
        "product_id": book.product_id,
        "state": str(book.state),
        "catching_up": catching_up,
        "best_bid": None,
        "best_ask": None,
        "mid": None,
        "spread": None,
        "bids": [],
        "asks": [],
        "levels": {"bids": bid_count, "asks": ask_count},
        "last_update_id": book.last_update_id,
        "exchange_ts_ns": book.exchange_ts_ns,
        "received_at_ns": book.received_at_ns,
        "last_snapshot_at_ns": book.snapshot_received_at_ns,
        "published_at_ns": published_at_ns,
    }
    if book.ready:
        bids, asks = book.depth(depth)
        record.update(
            best_bid=_level(book.best_bid()),
            best_ask=_level(book.best_ask()),
            mid=_decimal(book.mid()),
            spread=_decimal(book.spread()),
            bids=[_level(level) for level in bids],
            asks=[_level(level) for level in asks],
        )
    return json.dumps(record, separators=(",", ":")).encode()


def usable(record: dict, now_ns: int, stale_after_seconds: float = 5.0) -> bool:
    """The reader's rule: is this decoded record (or None for a missing key) safe to quote from?"""
    if not record or record.get("state") != "SYNCED" or record.get("catching_up"):
        return False
    received = record.get("received_at_ns")
    return received is not None and now_ns - received <= stale_after_seconds * 1e9
