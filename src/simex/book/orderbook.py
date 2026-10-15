"""A level-2 order book for one product, with no I/O (ADR 0012).

Input is only the venue-independent events from simex.marketdata.decode, so the book never
depends on which exchange the data came from. The book service feeds it from md.level2, and the
matching engine can embed it in-process later (one writer per pair, CLAUDE.md).

States:
- EMPTY: no snapshot yet.
- SYNCED: built from a snapshot and every update since; the only state that answers queries.
- RESYNCING: a resync marker, a crossed book or a failed audit invalidated it; it waits for the
  next snapshot.

Rules: a snapshot replaces the whole book; a level's size is the full size at that price, and 0
removes it; updates flagged awaiting_snapshot are skipped; updates while not SYNCED are skipped.
The crossed check runs after a whole message (one BookUpdate) is applied, never mid-message,
because a venue may move both sides in one message.

Prices and sizes are Decimal (ADR 0004). Queries on a book that isn't SYNCED raise BookNotReady,
so stale data never leaves the book.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from sortedcontainers import SortedDict

from simex.marketdata import BookUpdate, ResyncMarker

Level = tuple[Decimal, Decimal]  # (price, size)
ZERO = Decimal(0)


class BookState(StrEnum):
    EMPTY = "EMPTY"
    SYNCED = "SYNCED"
    RESYNCING = "RESYNCING"


class Outcome(StrEnum):
    """What apply() did with an event."""

    SNAPSHOT = "snapshot"  # the book was replaced and is SYNCED
    APPLIED = "applied"
    SKIPPED_AWAITING = "skipped_awaiting"  # flagged awaiting_snapshot by the ingestor
    SKIPPED_NOT_SYNCED = "skipped_not_synced"  # an update while EMPTY or RESYNCING
    RESYNC = "resync"  # a resync marker: the book was dropped
    CROSSED = "crossed"  # the message left best bid >= best ask: the book was dropped
    IGNORED = "ignored"  # not a book event (trades)


class BookNotReady(Exception):
    """The book is not SYNCED, so it has no trustworthy answer."""


@dataclass(frozen=True)
class Fill:
    """The result of walking the book with a market order."""

    side: str  # "buy" walks the asks, "sell" walks the bids
    requested: Decimal
    filled: Decimal
    notional: Decimal  # sum of price * size over the levels taken
    average_price: Decimal | None  # notional / filled; None if nothing filled
    levels: int  # price levels touched
    complete: bool  # filled == requested


def _descending(price: Decimal) -> Decimal:
    return -price


class OrderBook:
    def __init__(self, venue: str, product_id: str) -> None:
        self.venue = venue
        self.product_id = product_id
        self.state = BookState.EMPTY
        self.bids: SortedDict = SortedDict(_descending)  # best (highest) bid first
        self.asks: SortedDict = SortedDict()  # best (lowest) ask first
        self.last_update_id: int | None = None
        self.exchange_ts_ns: int | None = None
        self.received_at_ns: int | None = None
        self.snapshot_received_at_ns: int | None = None
        self.crossed = 0  # times a message left the book crossed

    # Applying events ---------------------------------------------------------------------------

    def apply(self, event: object) -> Outcome:
        if isinstance(event, ResyncMarker):
            self._check_product(event.venue, event.product_id)
            self._drop()
            return Outcome.RESYNC
        if not isinstance(event, BookUpdate):
            return Outcome.IGNORED
        self._check_product(event.venue, event.product_id)

        if event.snapshot:
            self.bids.clear()
            self.asks.clear()
            self._set_levels(event)
            self.state = BookState.SYNCED
            self.snapshot_received_at_ns = event.received_at_ns
            outcome = Outcome.SNAPSHOT
        elif event.awaiting_snapshot:
            return Outcome.SKIPPED_AWAITING
        elif self.state is not BookState.SYNCED:
            return Outcome.SKIPPED_NOT_SYNCED
        else:
            self._set_levels(event)
            outcome = Outcome.APPLIED

        if event.update_ids is not None:
            self.last_update_id = event.update_ids[1]
        self.exchange_ts_ns = event.exchange_ts_ns
        self.received_at_ns = event.received_at_ns
        # Only after the whole message: a venue may move both sides at once.
        if self.is_crossed():
            self.crossed += 1
            self._drop()
            return Outcome.CROSSED
        return outcome

    def invalidate(self) -> None:
        """Drop the book and wait for a snapshot (e.g. a failed audit)."""
        self._drop()

    def _check_product(self, venue: str, product_id: str) -> None:
        if (venue, product_id) != (self.venue, self.product_id):
            mine = f"{self.venue}/{self.product_id}"
            raise ValueError(f"event for {venue}/{product_id} sent to the book for {mine}")

    def _drop(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.state = BookState.RESYNCING

    def _set_levels(self, event: BookUpdate) -> None:
        _set(self.bids, event.bids)
        _set(self.asks, event.asks)

    def is_crossed(self) -> bool:
        return bool(self.bids and self.asks) and self.bids.keys()[0] >= self.asks.keys()[0]

    # Queries -----------------------------------------------------------------------------------

    def _ready(self) -> None:
        if self.state is not BookState.SYNCED:
            raise BookNotReady(f"{self.venue}/{self.product_id} is {self.state}")

    @property
    def ready(self) -> bool:
        return self.state is BookState.SYNCED

    def best_bid(self) -> Level | None:
        self._ready()
        return self.bids.peekitem(0) if self.bids else None

    def best_ask(self) -> Level | None:
        self._ready()
        return self.asks.peekitem(0) if self.asks else None

    def spread(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        return None if bid is None or ask is None else ask[0] - bid[0]

    def mid(self) -> Decimal | None:
        bid, ask = self.best_bid(), self.best_ask()
        return None if bid is None or ask is None else (bid[0] + ask[0]) / 2

    def depth(self, n: int) -> tuple[list[Level], list[Level]]:
        """The best n levels per side: (bids from the highest, asks from the lowest)."""
        self._ready()
        return list(self.bids.items()[:n]), list(self.asks.items()[:n])

    def level_counts(self) -> tuple[int, int]:
        return len(self.bids), len(self.asks)

    def walk(self, side: str, quantity: Decimal) -> Fill:
        """Fill a market order of `quantity` against the book, best price first.

        side "buy" takes the asks, "sell" takes the bids. The book is not changed.
        """
        self._ready()
        if side not in ("buy", "sell"):
            raise ValueError(f"side must be 'buy' or 'sell', not {side!r}")
        if quantity <= ZERO:
            raise ValueError("quantity must be positive")
        levels = self.asks if side == "buy" else self.bids
        remaining, notional, touched = quantity, ZERO, 0
        for price, size in levels.items():
            if remaining <= ZERO:
                break
            take = min(size, remaining)
            notional += price * take
            remaining -= take
            touched += 1
        filled = quantity - remaining
        return Fill(
            side=side,
            requested=quantity,
            filled=filled,
            notional=notional,
            average_price=notional / filled if filled > ZERO else None,
            levels=touched,
            complete=remaining == ZERO,
        )


def _set(levels: SortedDict, updates: Iterable[Level]) -> None:
    for price, size in updates:
        if size == ZERO:
            levels.pop(price, None)
        else:
            levels[price] = size
