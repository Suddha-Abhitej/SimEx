"""Prometheus metrics for the ingestor, served on INGEST_METRICS_PORT at /metrics.

Every metric carries a venue label ("gemini", "coinbase"). Call sites don't pass it: each metric
is wrapped so .labels(...) and the unlabelled methods fill in the ingestor's venue.
"""

from __future__ import annotations

from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Lag is local receive time minus exchange time, so it depends on an NTP-synced clock.
LAG_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class _Venued:
    """A metric whose first label is the venue, filled in from the owning IngestMetrics."""

    def __init__(self, owner: IngestMetrics, metric: Any) -> None:
        self._owner = owner
        self._metric = metric

    def labels(self, *values: str) -> Any:
        return self._metric.labels(self._owner.venue, *values)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._metric.labels(self._owner.venue), name)


class IngestMetrics:
    def __init__(self, registry: CollectorRegistry | None = None, venue: str = "gemini") -> None:
        self.registry = registry or CollectorRegistry()
        self.venue = venue
        self._unlabelled: list[Any] = []  # metrics with no labels but the venue
        r = self.registry

        def venued(kind, name: str, documentation: str, labels=(), **kwargs) -> Any:
            metric = kind(name, documentation, ["venue", *labels], registry=r, **kwargs)
            if not labels:
                self._unlabelled.append(metric)
            return _Venued(self, metric)

        self.messages = venued(
            Counter,
            "simex_ingest_messages",
            "Market-data messages received from the venue; rate() gives messages per second",
            ["channel"],
        )
        self.published = venued(
            Counter, "simex_ingest_published", "Envelopes handed to Kafka", ["topic"]
        )
        self.publish_errors = venued(
            Counter, "simex_ingest_publish_errors", "Envelopes Kafka failed to accept", ["topic"]
        )
        self.feed_lag = venued(
            Histogram,
            "simex_ingest_feed_lag_seconds",
            "Local receive time minus the venue's message timestamp",
            ["channel"],
            buckets=LAG_BUCKETS,
        )
        self.last_feed_lag = venued(
            Gauge,
            "simex_ingest_last_feed_lag_seconds",
            "Feed lag of the latest message",
            ["channel"],
        )
        self.gaps = venued(
            Counter,
            "simex_ingest_gaps",
            "Gaps and Coinbase out-of-order messages (kind=gap|out_of_order; cause=feed|injected)",
            ["kind", "cause"],
        )
        self.feed_gaps_last_minute = venued(
            Gauge,
            "simex_ingest_feed_gaps_last_minute",
            "Real (not injected) gaps in the last 60s; alert if above INGEST_GAP_ALERT_PER_MINUTE",
        )
        self.resyncs = venued(
            Counter,
            "simex_ingest_resyncs",
            "Level2 resyncs started (method=resubscribe|reconnect, trigger=gap|disconnect|refresh)",
            ["method", "trigger"],
        )
        self.resyncs_suppressed = venued(
            Counter,
            "simex_ingest_resyncs_suppressed",
            "Resubscribes deferred by the resync rate limit",
        )
        self.resync_duration = venued(
            Histogram,
            "simex_ingest_resync_duration_seconds",
            "From gap detection until every product has a fresh snapshot, by final method",
            ["method"],
            buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0),
        )
        self.snapshot_refreshes = venued(
            Counter,
            "simex_ingest_snapshot_refreshes",
            "Scheduled per-product snapshot refreshes that ended (outcome=completed|timeout, or "
            "superseded by a gap or disconnect resync)",
            ["product_id", "outcome"],
        )
        self.snapshot_refresh_duration = venued(
            Histogram,
            "simex_ingest_snapshot_refresh_duration_seconds",
            "From a scheduled refresh's resync marker until the product's fresh snapshot",
            ["product_id"],
            buckets=(0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.5, 5.0, 10.0),
        )
        self.last_snapshot_refresh_duration = venued(
            Gauge,
            "simex_ingest_last_snapshot_refresh_duration_seconds",
            "Recovery time of the product's latest completed snapshot refresh",
            ["product_id"],
        )
        # Not a staleness signal: venues only send snapshots on subscribe (and refresh, ADR 0009),
        # so this is the age of the book's baseline, not of the data.
        self.time_since_last_snapshot = venued(
            Gauge,
            "simex_ingest_time_since_last_snapshot_seconds",
            "Seconds since the last level2 snapshot for a product (+Inf before the first). "
            "Grows during healthy operation; not a staleness signal, see last_message_age",
            ["product_id"],
        )
        # The staleness signal for guards, with awaiting_snapshot. Heartbeats carry no product
        # and use product_id="".
        self.last_message_age = venued(
            Gauge,
            "simex_ingest_last_message_age_seconds",
            "Seconds since the last message for a product on a channel (+Inf before the first); "
            'heartbeats (Coinbase) and ping replies (Gemini) use product_id=""',
            ["product_id", "channel"],
        )
        self.awaiting_snapshot = venued(
            Gauge,
            "simex_ingest_awaiting_snapshot",
            "1 while a product's level2 book is invalid and waiting for a snapshot",
            ["product_id"],
        )
        self.fault_injected_drops = venued(
            Counter,
            "simex_ingest_fault_injected_drops",
            "Messages dropped on purpose by SIMEX_INGEST_DROP_EVERY (dev only)",
        )
        self.missed_messages = venued(
            Counter,
            "simex_ingest_missed_messages",
            "Messages known lost to gaps (Coinbase; Gemini gaps can't be counted)",
        )
        self.reconnects = venued(
            Counter, "simex_ingest_reconnects", "Reconnects, by reason", ["reason"]
        )
        self.connected = venued(Gauge, "simex_ingest_connected", "1 while subscribed to the venue")
        self.exchange_errors = venued(
            Counter, "simex_ingest_exchange_errors", "Error messages sent by the venue"
        )
        self.protocol_drift = venued(
            Counter,
            "simex_ingest_protocol_drift",
            "Messages that break the venue's observed protocol (Gemini: overlap, stale, "
            "stale_after_snapshot, unexpected_snapshot, malformed_update_ids); any but "
            "stale_after_snapshot should stay at 0",
            ["product_id", "kind"],
        )
        self.ping_rtt = venued(
            Gauge,
            "simex_ingest_ping_rtt_seconds",
            "Round trip of the latest application-level ping (Gemini)",
        )

    def bind(self, venue: str) -> None:
        """Set the venue, and export every metric without other labels at 0 from the start."""
        self.venue = venue
        for metric in self._unlabelled:
            metric.labels(venue)
