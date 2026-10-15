"""Prometheus metrics for the recorder, served on RECORDER_METRICS_PORT (9103) at /metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


class RecorderMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.records = Counter(
            "simex_recorder_records",
            "Records written to recording files, by channel, product and envelope type",
            ["channel", "product_id", "type"],
            registry=r,
        )
        self.lag_records = Gauge(
            "simex_recorder_lag_records",
            "Records in Kafka not yet read by the recorder (end offset minus position)",
            ["topic", "partition"],
            registry=r,
        )
        self.lag_seconds = Gauge(
            "simex_recorder_lag_seconds",
            "Now minus received_at_ns of the last record read, by channel. Alert on level2 only, "
            "and only while lag_records > 0: trades can be silent for minutes on Gemini",
            ["channel"],
            registry=r,
        )
        self.seconds_since_last_write = Gauge(
            "simex_recorder_seconds_since_last_write",
            "Seconds since rows were last written to disk (a row group or a file close)",
            registry=r,
        )
        self.seconds_since_last_file = Gauge(
            "simex_recorder_seconds_since_last_file",
            "Seconds since a recording file was last published (files close hourly)",
            registry=r,
        )
        self.files = Counter(
            "simex_recorder_files", "Recording files published", ["channel"], registry=r
        )
        self.bytes = Counter(
            "simex_recorder_bytes",
            "Bytes of published recording files (compressed, on disk)",
            ["channel"],
            registry=r,
        )
        self.open_files = Gauge(
            "simex_recorder_open_files", "Recording files open for writing", registry=r
        )
        self.duplicates_skipped = Counter(
            "simex_recorder_duplicates_skipped",
            "Records skipped because a published file already holds them (re-read after a restart)",
            ["topic"],
            registry=r,
        )
        self.skipped = Counter(
            "simex_recorder_skipped",
            "Records not recorded, by reason (foreign_venue, unrecordable, past_retention)",
            ["topic", "reason"],
            registry=r,
        )
        self.lost_offsets = Counter(
            "simex_recorder_lost_offsets",
            "Upper bound on offsets that expired from Kafka before they were recorded",
            ["topic"],
            registry=r,
        )
        self.retention_deleted = Counter(
            "simex_recorder_retention_deleted_days",
            "Date directories deleted by retention",
            registry=r,
        )
        self.retention_errors = Counter(
            "simex_recorder_retention_errors",
            "Date directories retention couldn't delete (for example a file held open on "
            "Windows); retried on the next hourly pass",
            registry=r,
        )
