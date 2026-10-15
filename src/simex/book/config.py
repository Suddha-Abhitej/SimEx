"""Book service settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")


@dataclass(frozen=True)
class BookSettings:
    pairs: tuple[str, ...] = DEFAULT_PAIRS
    venue: str = "gemini"  # md.* carries the live venue only (ADR 0011)
    level2_topic: str = "md.level2"
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    valkey_url: str = "redis://127.0.0.1:6379/0"
    metrics_port: int = 9102
    # The ingestor's refresh interval: cold start reads md.level2 from 2x this long ago (ADR 0009).
    snapshot_refresh_seconds: float = 1800.0
    # Valkey: publish each book this often, with this many levels a side, keys expiring after
    # the TTL so a dead service's books disappear instead of going stale.
    publish_interval_seconds: float = 0.25
    publish_depth: int = 10
    publish_ttl_seconds: float = 2.0
    # Readers treat a book with no update received for this long as stale (ADR 0011).
    stale_after_seconds: float = 5.0

    @property
    def lookback_seconds(self) -> float:
        return 2 * self.snapshot_refresh_seconds

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BookSettings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        pairs = tuple(p.strip().upper() for p in get("SIMEX_PAIRS", ",".join(d.pairs)).split(","))
        valkey_url = env.get("VALKEY_URL", "").strip() or (
            f"redis://127.0.0.1:{get('VALKEY_PORT', 6379)}/0"
        )
        return cls(
            pairs=tuple(p for p in pairs if p),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            valkey_url=valkey_url,
            metrics_port=int(get("BOOK_METRICS_PORT", d.metrics_port)),
            snapshot_refresh_seconds=float(
                get("INGEST_SNAPSHOT_REFRESH_SECONDS", d.snapshot_refresh_seconds)
            ),
            publish_interval_seconds=float(
                get("BOOK_PUBLISH_INTERVAL_SECONDS", d.publish_interval_seconds)
            ),
            publish_depth=int(get("BOOK_PUBLISH_DEPTH", d.publish_depth)),
            publish_ttl_seconds=float(get("BOOK_PUBLISH_TTL_SECONDS", d.publish_ttl_seconds)),
            stale_after_seconds=float(get("INGEST_STALE_AFTER_SECONDS", d.stale_after_seconds)),
        )
