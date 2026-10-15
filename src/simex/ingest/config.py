"""Ingestor settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
VENUES = ("gemini", "coinbase")


@dataclass(frozen=True)
class Settings:
    # gemini is the live venue; coinbase is for feed research only (ADR 0010).
    venue: str = "gemini"
    # Coinbase's terms forbid using its data to benchmark AI agents: it only runs when this is
    # set, and publishes to research.coinbase.md.* (ADR 0011).
    coinbase_research: bool = False
    # Empty means the adapter's default URL.
    ws_url: str = ""
    pairs: tuple[str, ...] = DEFAULT_PAIRS
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    topic_partitions: int = 3
    topic_replication_factor: int = 1
    metrics_port: int = 9101
    # No message at all for this long means the socket is stale (longest silence seen: 1 s).
    stale_after_seconds: float = 5.0
    # Application-level ping on venues without a heartbeats channel (Gemini), so a healthy but
    # quiet connection always has traffic and its round trip is measured. 0 means off.
    ping_interval_seconds: float = 2.0
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    # At most one level2 resubscribe per this many seconds after gaps (ADR 0008).
    resync_min_interval_seconds: float = 5.0
    # If a resubscribe hasn't produced every product's snapshot by then, reconnect instead.
    resync_timeout_seconds: float = 10.0
    # More real gaps than this per minute suggests Coinbase changed its sequence semantics.
    gap_alert_per_minute: int = 10
    # Refresh every product's level2 snapshot this often, one product at a time and staggered
    # evenly, so md.level2 always holds a recent snapshot for cold starts (ADR 0009). 0 means off.
    snapshot_refresh_seconds: float = 1800.0
    # md.level2 retention, applied to the topic at startup. Must cover 2x the refresh interval.
    level2_retention_hours: float = 24.0
    # Per partition; -1 means no size limit.
    level2_retention_bytes: int = 2 * 1024**3
    # Dev-only fault injector: drop every Nth received message. 0 means off.
    drop_every: int = 0

    def check(self) -> None:
        """Refuse settings that break ADR 0010 or can't work."""
        if self.venue not in VENUES:
            raise ValueError(f"SIMEX_VENUE must be one of {', '.join(VENUES)}, not {self.venue!r}")
        if self.venue == "coinbase" and not self.coinbase_research:
            raise ValueError(
                "SIMEX_VENUE=coinbase is for feed research only (ADR 0010): Coinbase data must "
                "never feed agents, matching or scoring. Set SIMEX_COINBASE_RESEARCH=1 to run it; "
                "it publishes to research.coinbase.md.*"
            )
        if not self.pairs:
            raise ValueError("SIMEX_PAIRS is empty")

    @property
    def refresh_stagger_seconds(self) -> float:
        """Time between two products' refreshes: the interval spread evenly over the pairs."""
        return self.snapshot_refresh_seconds / max(1, len(self.pairs))

    @property
    def level2_retention_ms(self) -> int:
        return round(self.level2_retention_hours * 3_600_000)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        pairs = tuple(p.strip().upper() for p in get("SIMEX_PAIRS", ",".join(d.pairs)).split(","))
        venue = get("SIMEX_VENUE", d.venue).lower()
        url_variable = "COINBASE_WS_URL" if venue == "coinbase" else "GEMINI_WS_URL"
        return cls(
            venue=venue,
            coinbase_research=get("SIMEX_COINBASE_RESEARCH", "0").lower() in ("1", "true", "yes"),
            ws_url=get(url_variable, d.ws_url),
            pairs=tuple(p for p in pairs if p),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            topic_partitions=int(get("INGEST_TOPIC_PARTITIONS", d.topic_partitions)),
            topic_replication_factor=int(
                get("INGEST_TOPIC_REPLICATION_FACTOR", d.topic_replication_factor)
            ),
            metrics_port=int(get("INGEST_METRICS_PORT", d.metrics_port)),
            stale_after_seconds=float(get("INGEST_STALE_AFTER_SECONDS", d.stale_after_seconds)),
            ping_interval_seconds=float(
                get("INGEST_PING_INTERVAL_SECONDS", d.ping_interval_seconds)
            ),
            backoff_initial_seconds=float(
                get("INGEST_BACKOFF_INITIAL_SECONDS", d.backoff_initial_seconds)
            ),
            backoff_max_seconds=float(get("INGEST_BACKOFF_MAX_SECONDS", d.backoff_max_seconds)),
            resync_min_interval_seconds=float(
                get("INGEST_RESYNC_MIN_INTERVAL_SECONDS", d.resync_min_interval_seconds)
            ),
            resync_timeout_seconds=float(
                get("INGEST_RESYNC_TIMEOUT_SECONDS", d.resync_timeout_seconds)
            ),
            gap_alert_per_minute=int(get("INGEST_GAP_ALERT_PER_MINUTE", d.gap_alert_per_minute)),
            snapshot_refresh_seconds=float(
                get("INGEST_SNAPSHOT_REFRESH_SECONDS", d.snapshot_refresh_seconds)
            ),
            level2_retention_hours=float(
                get("INGEST_LEVEL2_RETENTION_HOURS", d.level2_retention_hours)
            ),
            level2_retention_bytes=int(
                get("INGEST_LEVEL2_RETENTION_BYTES", d.level2_retention_bytes)
            ),
            drop_every=int(get("SIMEX_INGEST_DROP_EVERY", d.drop_every)),
        )
