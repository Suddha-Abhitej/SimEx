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
        self.sequence_gaps = Counter(
            "simex_ingest_sequence_gaps",
            "Sequence anomalies seen (kind=gap or out_of_order)",
            ["kind"],
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
