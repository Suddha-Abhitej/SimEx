"""Completed writer admissions, independent of the lossy notification journal (ADR 0026)."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Protocol

from prometheus_client.core import CounterMetricFamily

log = logging.getLogger(__name__)

# Only these server-owned prefixes may become labels; suffixes and future reasons cannot.
REASONS = frozenset(
    {
        "bad_account",
        "bad_client_order_id",
        "bad_field",
        "bad_decimal",
        "bad_reasoning",
        "unknown_product",
        "bad_quantity",
        "below_min_size",
        "bad_price",
        "idempotency_key_reused",
        "product_not_allowed",
        "quantity_limit",
        "notional_limit",
        "position_limit",
        "book_not_synced",
        "catching_up",
        "stale_level2",
        "feed_lagging",
        "market_closed",
        "no_liquidity",
        "insufficient_funds",
        "writer_halted",
        "account_halted",
        "persistence_failure",
        "internal_error",
        "other",
    }
)
FAMILIES = {
    "submitted": "Completed new admissions: accepted plus arrival-rejected",
    "accepted": "New orders accepted at arrival, not guaranteed fills",
    "arrival_rejected": "New orders recorded REJECTED at arrival",
    "effect_rejected": "Previously accepted orders rejected at effect",
    "replayed": "Same key and request returning an existing order, in any status",
    "submit_errors": "Calls raising without a completed admission",
}
COVERAGE = (
    "; AccountWriter.submit boundary only. Request-validation refusals returned earlier by "
    "ExchangeTools are visible only to the client that made them"
)


class OrderObserver(Protocol):
    def observe(self, outcome: str, product: str | None, reason: str = "") -> None: ...

    def disable(self) -> None: ...


class OrderMetrics:
    """One single-writer snapshot, shared across runtime rebuilds by RunnerMetrics.

    No scrape-held lock or mutable count is shared with the trading task. Publication replaces
    the entire immutable mapping; a scrape captures it once for all counter families. If any
    update fails, omit these families rather than export permanently incomplete accounting.
    """

    def __init__(self, products: Iterable[str]) -> None:
        self.products = frozenset(products) | {"unknown"}
        self._snapshot: Mapping[tuple[str, str, str], int] = MappingProxyType({})
        self._disabled = False

    def disable(self) -> None:
        if self._disabled:
            return
        self._disabled = True
        try:
            log.warning("Writer order metrics disabled; admission counter families unavailable")
        except Exception:
            pass  # Logging is telemetry too, and cannot replace a trading exception.

    def _labels(self, product: str | None, reason: str) -> tuple[str, str]:
        product = product if product in self.products else "unknown"
        prefix = reason.split(":", 1)[0]
        return product, prefix if prefix in REASONS else "other"

    def _publish(self, counts: dict[tuple[str, str, str], int]) -> None:
        self._snapshot = MappingProxyType(counts)

    def observe(self, outcome: str, product: str | None, reason: str = "") -> None:
        if self._disabled:
            return
        try:
            if outcome not in FAMILIES or outcome == "submitted":
                raise ValueError("invalid metric outcome")
            product, reason = self._labels(product, reason)
            if outcome in {"accepted", "replayed"}:
                reason = ""
            key = (outcome, product, reason)
            counts = dict(self._snapshot)
            counts[key] = counts.get(key, 0) + 1
            self._publish(counts)
        except Exception:
            self.disable()

    @staticmethod
    def _family(outcome: str) -> CounterMetricFamily:
        name = (
            "simex_exchange_order_submit_errors"
            if outcome == "submit_errors"
            else f"simex_exchange_orders_{outcome}"
        )
        labels = ["product_id"]
        if outcome in {"arrival_rejected", "effect_rejected", "submit_errors"}:
            labels.append("reason")
        return CounterMetricFamily(name, FAMILIES[outcome] + COVERAGE, labels=labels)

    def describe(self):
        return [self._family(outcome) for outcome in FAMILIES]

    def collect(self):
        if self._disabled:
            return ()
        try:
            snapshot = self._snapshot
            families = {outcome: self._family(outcome) for outcome in FAMILIES}
            for product in sorted(self.products):
                accepted = snapshot.get(("accepted", product, ""), 0)
                rejected = sum(
                    count
                    for (outcome, p, _), count in snapshot.items()
                    if outcome == "arrival_rejected" and p == product
                )
                families["submitted"].add_metric([product], accepted + rejected)
                families["accepted"].add_metric([product], accepted)
                families["replayed"].add_metric(
                    [product], snapshot.get(("replayed", product, ""), 0)
                )
            for (outcome, product, reason), count in snapshot.items():
                if outcome not in {"accepted", "replayed"}:
                    families[outcome].add_metric([product, reason], count)
            # Build everything before returning: an export failure cannot leak a partial family.
            return tuple(families.values())
        except Exception:
            self.disable()
            return ()
