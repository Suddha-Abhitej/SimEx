"""Prometheus metrics for the live runner, served on EXCHANGE_METRICS_PORT (9104) at /metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from simex.book.orderbook import BookState

from .gate import GateDecision, MarketStatus

DELIVERY_BUCKETS = (0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1.0, 5.0)
LATE_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0)


class RunnerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.events = Counter(
            "simex_exchange_events",
            "Records released to the pair's engine, by channel (level2, trades)",
            ["product_id", "channel"],
            registry=r,
        )
        self.late = Counter(
            "simex_exchange_late_events",
            "Records that arrived after the watermark had passed their received time, each "
            "logged with its type. Their delivery took longer than the hold-back: the ordering "
            "margin was lost for them (an order effective in the gap would have missed them), "
            "though most still land in replay's position. A few in bursts are normal; a "
            "sustained rate means the hold-back is too short for this host",
            ["product_id", "channel"],
            registry=r,
        )
        self.late_seconds = Histogram(
            "simex_exchange_late_seconds",
            "How far behind the watermark a late record's received time was",
            buckets=LATE_BUCKETS,
            registry=r,
        )
        self.delivery_seconds = Histogram(
            "simex_exchange_delivery_seconds",
            "Wall time when consumed minus received_at_ns, in steady state only: records from a "
            "partition caught up at this fetch and the one before. The catch-up after a start "
            "is left out (its delay is the backlog's age), so the count is below events. This "
            "is the delay the hold-back has to cover",
            ["channel"],
            buckets=DELIVERY_BUCKETS,
            registry=r,
        )
        self.watermark_lag = Gauge(
            "simex_exchange_watermark_lag_seconds",
            "Wall time minus the pair's watermark, set after each fetch: the hold-back plus the "
            "fetch's wait when caught up",
            ["product_id"],
            registry=r,
        )
        self.held = Gauge(
            "simex_exchange_held_records",
            "Records consumed but not yet released (waiting for the watermark)",
            ["product_id"],
            registry=r,
        )
        self.caught_up = Gauge(
            "simex_exchange_partition_caught_up",
            "1 when the partition was read to its end at the last fetch",
            ["topic", "partition"],
            registry=r,
        )
        self.out_of_order = Gauge(
            "simex_exchange_out_of_order_records",
            "Records whose received time was below an earlier one's in the same partition, each "
            "logged with its type and how far below. The merge assumes this is 0",
            registry=r,
        )
        self.state = Gauge(
            "simex_exchange_book_state",
            "1 for the engine book's current state (EMPTY, SYNCED, RESYNCING), 0 for the others",
            ["product_id", "state"],
            registry=r,
        )
        self.undecodable = Counter(
            "simex_exchange_undecodable_records",
            "Records the engine couldn't decode (released and digested, not applied)",
            ["product_id"],
            registry=r,
        )
        self.gate_state = Gauge(
            "simex_exchange_gate_state",
            "1 for the current gate state of each pair",
            ["product_id", "state"],
            registry=r,
        )
        self.gate_closed_seconds = Counter(
            "simex_exchange_gate_closed_seconds",
            "Seconds each pair spent closed, by gate reason",
            ["product_id", "reason"],
            registry=r,
        )
        self.rejections = Counter(
            "simex_exchange_rejections",
            "Orders rejected by reason",
            ["product_id", "reason"],
            registry=r,
        )
        self.lag_baseline = Gauge(
            "simex_exchange_lag_baseline_established",
            "1 after ten minutes of level2 lag samples establish the rolling baseline",
            ["product_id"],
            registry=r,
        )
        self._gate_last_ns: dict[str, tuple[int, str]] = {}

    def set_state(self, product: str, state: BookState) -> None:
        for s in BookState:
            self.state.labels(product, s.value).set(1 if s is state else 0)

    def observe_gate(
        self, product: str, status: MarketStatus, decision: GateDecision, now_ns: int
    ) -> None:
        reason = (decision.reason or "open").split(":", 1)[0]
        for state in ("open", "book_not_synced", "catching_up", "stale_level2", "feed_lagging"):
            self.gate_state.labels(product, state).set(1 if state == reason else 0)
        self.lag_baseline.labels(product).set(1 if status.baseline_established else 0)
        previous = self._gate_last_ns.get(product)
        if previous is not None and previous[1] != "open":
            self.gate_closed_seconds.labels(product, previous[1]).inc(
                max(0, now_ns - previous[0]) / 1e9
            )
        self._gate_last_ns[product] = (now_ns, reason)
