"""Prometheus metrics for the synchronous ledger boundary."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Histogram

LEDGER_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


class LedgerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.latency = Histogram(
            "simex_ledger_latency_seconds",
            "Ledger and account-writer latency by boundary phase",
            ["phase"],
            buckets=LEDGER_BUCKETS,
            registry=self.registry,
        )
        self.timeouts = Counter(
            "simex_ledger_timeouts_total",
            "Ledger calls that exceeded the hard timeout",
            registry=self.registry,
        )
        self.conflicts = Counter(
            "simex_ledger_conflicts_total",
            "Fill payload conflicts rejected as integrity errors",
            registry=self.registry,
        )
        self.halts = Counter(
            "simex_ledger_halts_total",
            "Ledger failures that halted account-writer processing",
            registry=self.registry,
        )

    def observe(self, phase: str, elapsed_ns: int) -> None:
        self.latency.labels(phase).observe(elapsed_ns / 1e9)
