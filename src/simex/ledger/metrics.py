"""Prometheus metrics for the synchronous ledger boundary."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

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
        self.reconciliation_runs = Counter(
            "simex_ledger_reconciliation_runs_total",
            "Ledger reconciliation runs completed",
            registry=self.registry,
        )
        self.reconciliation_seconds = Histogram(
            "simex_ledger_reconciliation_seconds",
            "Duration of a full ledger reconciliation",
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
            registry=self.registry,
        )
        self.reconciliation_mismatches = Counter(
            "simex_ledger_reconciliation_mismatches_total",
            "Balance mismatches found by reconciliation",
            ["account_id", "asset"],
            registry=self.registry,
        )
        self.reconciliation_mismatch_active = Gauge(
            "simex_ledger_reconciliation_mismatch_active",
            "1 while a balance mismatch remains unresolved",
            ["account_id", "asset"],
            registry=self.registry,
        )
        self.reconciliation_last_success_timestamp_seconds = Gauge(
            "simex_ledger_reconciliation_last_success_timestamp_seconds",
            "Unix timestamp of the last completed ledger reconciliation",
            registry=self.registry,
        )
        self.reconciliation_overrides = Gauge(
            "simex_ledger_reconciliation_override_active",
            "1 while an audited reconciliation override is active",
            ["account_id", "asset"],
            registry=self.registry,
        )
        self.double_entry_violations = Counter(
            "simex_ledger_double_entry_violations_total",
            "Double-entry invariant violations found by reconciliation",
            registry=self.registry,
        )
        self.double_entry_violation_active = Gauge(
            "simex_ledger_double_entry_violation_active",
            "1 while a double-entry invariant violation remains present",
            ["transaction_id", "asset"],
            registry=self.registry,
        )

    def observe(self, phase: str, elapsed_ns: int) -> None:
        self.latency.labels(phase).observe(elapsed_ns / 1e9)
