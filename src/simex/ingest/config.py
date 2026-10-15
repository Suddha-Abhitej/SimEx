"""Ingestor settings, read from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_WS_URL = "wss://advanced-trade-ws.coinbase.com"
DEFAULT_PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")


@dataclass(frozen=True)
class Settings:
    ws_url: str = DEFAULT_WS_URL
    pairs: tuple[str, ...] = DEFAULT_PAIRS
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    topic_partitions: int = 3
    topic_replication_factor: int = 1
    metrics_port: int = 9101
    # No message at all (heartbeats arrive every second) for this long means the socket is stale.
    stale_after_seconds: float = 10.0
    backoff_initial_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    # At most one level2 resubscribe per this many seconds after gaps (ADR 0008).
    resync_min_interval_seconds: float = 5.0
    # If a resubscribe hasn't produced every product's snapshot by then, reconnect instead.
    resync_timeout_seconds: float = 10.0
    # More real gaps than this per minute suggests Coinbase changed its sequence semantics.
    gap_alert_per_minute: int = 10
    # Dev-only fault injector: drop every Nth received message. 0 means off.
    drop_every: int = 0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        pairs = tuple(p.strip().upper() for p in get("SIMEX_PAIRS", ",".join(d.pairs)).split(","))
        return cls(
            ws_url=get("COINBASE_WS_URL", d.ws_url),
            pairs=tuple(p for p in pairs if p),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            topic_partitions=int(get("INGEST_TOPIC_PARTITIONS", d.topic_partitions)),
            topic_replication_factor=int(
                get("INGEST_TOPIC_REPLICATION_FACTOR", d.topic_replication_factor)
            ),
            metrics_port=int(get("INGEST_METRICS_PORT", d.metrics_port)),
            stale_after_seconds=float(get("INGEST_STALE_AFTER_SECONDS", d.stale_after_seconds)),
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
            drop_every=int(get("SIMEX_INGEST_DROP_EVERY", d.drop_every)),
        )
