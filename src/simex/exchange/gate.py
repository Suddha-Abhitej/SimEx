"""Pure stale-data decisions over immutable per-pair market snapshots (#9)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from simex.book.orderbook import BookState

SECOND = 1_000_000_000


class GateState(StrEnum):
    OPEN = "open"
    BOOK_NOT_SYNCED = "book_not_synced"
    CATCHING_UP = "catching_up"
    STALE_LEVEL2 = "stale_level2"
    FEED_LAGGING = "feed_lagging"


@dataclass(frozen=True)
class MarketStatus:
    book_state: BookState
    last_level2_received_ns: int | None
    lag_ns: int | None
    excess_lag_ns: int | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    catching_up: bool = False
    baseline_established: bool = False


@dataclass(frozen=True)
class GateDecision:
    open: bool
    reason: str | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


class StaleDataGate:
    """A pure function of the latest engine snapshots and the caller's market-data time."""

    def __init__(
        self,
        statuses: Mapping[str, MarketStatus] | Mapping[str, object],
        *,
        stale_after_ns: int = 5 * SECOND,
        max_excess_lag_ns: int = 2 * SECOND,
        max_absolute_lag_ns: int = 30 * SECOND,
    ) -> None:
        self.statuses = statuses
        self.stale_after_ns = stale_after_ns
        self.max_excess_lag_ns = max_excess_lag_ns
        self.max_absolute_lag_ns = max_absolute_lag_ns

    def _status(self, product_id: str) -> MarketStatus:
        value = self.statuses[product_id]
        return value if isinstance(value, MarketStatus) else value.status

    def check(self, product_id: str, now_ns: int) -> GateDecision:
        status = self._status(product_id)
        if status.catching_up:
            return GateDecision(False, GateState.CATCHING_UP.value)
        if status.book_state is not BookState.SYNCED:
            return GateDecision(False, GateState.BOOK_NOT_SYNCED.value)
        age = (
            now_ns - status.last_level2_received_ns
            if status.last_level2_received_ns is not None
            else self.stale_after_ns + 1
        )
        if age > self.stale_after_ns:
            return GateDecision(False, f"{GateState.STALE_LEVEL2.value}:age_ns={max(0, age)}")
        lag = status.lag_ns
        if lag is not None and abs(lag) > self.max_absolute_lag_ns:
            excess = status.excess_lag_ns if status.excess_lag_ns is not None else lag
            return GateDecision(False, f"{GateState.FEED_LAGGING.value}:excess_ns={excess}")
        if (
            status.baseline_established
            and status.excess_lag_ns is not None
            and status.excess_lag_ns > self.max_excess_lag_ns
        ):
            return GateDecision(
                False, f"{GateState.FEED_LAGGING.value}:excess_ns={status.excess_lag_ns}"
            )
        return GateDecision(True, None, status.best_bid, status.best_ask)
