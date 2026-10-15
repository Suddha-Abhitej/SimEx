"""Prometheus metrics for the ingestor, served on INGEST_METRICS_PORT at /metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Lag is local receive time minus exchange time, so it depends on an NTP-synced clock.
LAG_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class IngestMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.messages = Counter(
            "simex_ingest_messages",
            "Market-data messages received from Coinbase; rate() gives messages per second",
            ["channel"],
            registry=r,
        )
        self.published = Counter(
            "simex_ingest_published", "Envelopes handed to Kafka", ["topic"], registry=r
        )
        self.publish_errors = Counter(
            "simex_ingest_publish_errors", "Envelopes Kafka failed to accept", ["topic"], registry=r
        )
        self.feed_lag = Histogram(
            "simex_ingest_feed_lag_seconds",
            "Local receive time minus Coinbase message timestamp",
            ["channel"],
            buckets=LAG_BUCKETS,
            registry=r,
        )
        self.last_feed_lag = Gauge(
            "simex_ingest_last_feed_lag_seconds",
            "Feed lag of the latest message",
            ["channel"],
            registry=r,
        )
        self.gaps = Counter(
            "simex_ingest_gaps",
            "Sequence anomalies (kind=gap|out_of_order; cause=feed|injected by the fault injector)",
            ["kind", "cause"],
            registry=r,
        )
        self.feed_gaps_last_minute = Gauge(
            "simex_ingest_feed_gaps_last_minute",
            "Real (not injected) gaps in the last 60s; alert if above INGEST_GAP_ALERT_PER_MINUTE",
            registry=r,
        )
        self.resyncs = Counter(
            "simex_ingest_resyncs",
            "Level2 resyncs started (method=resubscribe|reconnect, trigger=gap|disconnect)",
            ["method", "trigger"],
            registry=r,
        )
        self.resyncs_suppressed = Counter(
            "simex_ingest_resyncs_suppressed",
            "Resubscribes deferred by the resync rate limit",
            registry=r,
        )
        self.resync_duration = Histogram(
            "simex_ingest_resync_duration_seconds",
            "From gap detection until every product has a fresh snapshot, by final method",
            ["method"],
            buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0),
            registry=r,
        )
        self.time_since_last_snapshot = Gauge(
            "simex_ingest_time_since_last_snapshot_seconds",
            "Seconds since the last level2 snapshot for a product (+Inf before the first)",
            ["product_id"],
            registry=r,
        )
        self.awaiting_snapshot = Gauge(
            "simex_ingest_awaiting_snapshot",
            "1 while a product's level2 book is invalid and waiting for a snapshot",
            ["product_id"],
            registry=r,
        )
        self.fault_injected_drops = Counter(
            "simex_ingest_fault_injected_drops",
            "Messages dropped on purpose by SIMEX_INGEST_DROP_EVERY (dev only)",
            registry=r,
        )
        self.missed_messages = Counter(
            "simex_ingest_missed_messages", "Messages skipped over by sequence gaps", registry=r
        )
        self.reconnects = Counter(
            "simex_ingest_reconnects", "Reconnects, by reason", ["reason"], registry=r
        )
        self.connected = Gauge(
            "simex_ingest_connected", "1 while subscribed to Coinbase", registry=r
        )
        self.coinbase_errors = Counter(
            "simex_ingest_coinbase_errors", "Error messages sent by Coinbase", registry=r
        )
