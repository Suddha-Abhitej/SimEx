"""Prometheus metrics for the book service, served on BOOK_METRICS_PORT (9102) at /metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from .orderbook import BookState

APPLY_BUCKETS = (0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.1)
LAG_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0)


class BookMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        labels = ["venue", "product_id"]
        self.state = Gauge(
            "simex_book_state",
            "1 for the book's current state (EMPTY, SYNCED, RESYNCING), 0 for the others",
            [*labels, "state"],
            registry=r,
        )
        self.levels = Gauge(
            "simex_book_levels", "Price levels in the book", [*labels, "side"], registry=r
        )
        self.updates = Counter(
            "simex_book_updates",
            "Book events by outcome (snapshot, applied, skipped_awaiting, skipped_not_synced, "
            "resync, crossed)",
            [*labels, "outcome"],
            registry=r,
        )
        self.audits = Counter(
            "simex_book_audits",
            "Book audits against the venue's own top levels (ADR 0012): outcome match, mismatch, "
            "skipped (aligned but the book isn't SYNCED at that update) or unaligned; reason is "
            "how the ingestor placed it (aligned, inside_diff, stream_past, timeout, resync, "
            "superseded)",
            [*labels, "outcome", "reason"],
            registry=r,
        )
        self.resync_requests = Counter(
            "simex_book_resync_requests",
            "Resync requests sent to the ingestor on ctl.resync_requests, by reason",
            [*labels, "reason"],
            registry=r,
        )
        self.crossed = Counter(
            "simex_book_crossed",
            "Messages that left the book crossed (best bid >= best ask); should stay 0",
            labels,
            registry=r,
        )
        self.resyncs = Counter(
            "simex_book_resyncs",
            "Times the book was dropped to wait for a snapshot (reason=marker|crossed|"
            "audit_mismatch)",
            [*labels, "reason"],
            registry=r,
        )
        self.apply_seconds = Histogram(
            "simex_book_apply_seconds",
            "Time to decode and apply one md.level2 message",
            labels,
            buckets=APPLY_BUCKETS,
            registry=r,
        )
        self.ingest_to_book = Histogram(
            "simex_book_ingest_to_book_seconds",
            "From the ingestor receiving a message to the book applying it (same-host clocks; "
            "large while catching up)",
            labels,
            buckets=LAG_BUCKETS,
            registry=r,
        )
        self.seconds_since_snapshot = Gauge(
            "simex_book_seconds_since_snapshot",
            "Since the ingestor received the book's last snapshot (+Inf before the first)",
            labels,
            registry=r,
        )
        self.seconds_since_update = Gauge(
            "simex_book_seconds_since_update",
            "Since the ingestor received the last message applied to the book (+Inf before)",
            labels,
            registry=r,
        )
        self.seconds_since_synced = Gauge(
            "simex_book_seconds_since_synced",
            "0 while SYNCED, else seconds since the book was last SYNCED (+Inf if never)",
            labels,
            registry=r,
        )
        self.catching_up = Gauge(
            "simex_book_catching_up",
            "1 while the book is replaying md.level2 history after startup",
            labels,
            registry=r,
        )
        self.valkey_errors = Counter(
            "simex_book_valkey_errors", "Failed writes to Valkey", registry=r
        )
        self.replayed_from_start = Counter(
            "simex_book_replayed_from_log_start",
            "Partitions re-read from the log start because a book had no snapshot in the window",
            registry=r,
        )

    def set_state(self, venue: str, product_id: str, state: BookState) -> None:
        for each in BookState:
            self.state.labels(venue, product_id, str(each)).set(int(each is state))
