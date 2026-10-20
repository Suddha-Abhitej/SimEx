"""Live runner settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")


@dataclass(frozen=True)
class RunnerSettings:
    pairs: tuple[str, ...] = DEFAULT_PAIRS
    venue: str = "gemini"  # md.* carries the live venue only (ADR 0011)
    level2_topic: str = "md.level2"
    trades_topic: str = "md.trades"
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    metrics_port: int = 9104
    # Cold start reads both topics from 2x the ingestor's refresh interval ago (ADR 0009).
    snapshot_refresh_seconds: float = 1800.0
    # The merge's hold-back: a caught-up partition is taken to hold everything received up to
    # this long before the wall time it was seen caught up (ADR 0015). It covers the ingestor's
    # producer linger, the broker and the fetch wait below.
    # 250 ms: set from the measured delivery distribution (ADR 0015, Measured), about the
    # 99.99th percentile on the laptop; re-measure on Oracle.
    holdback_ms: float = 250.0
    fetch_max_wait_ms: int = 20
    # The anti-cheat delay (#10 makes it configurable).
    order_delay_ms: float = 200.0
    # Per pair and UTC hour, the order the runner released records in (for the replay check).
    digest_path: str = "data/exchange/merge-digests.jsonl"

    @property
    def lookback_seconds(self) -> float:
        return 2 * self.snapshot_refresh_seconds

    @property
    def topics(self) -> tuple[str, str]:
        return (self.level2_topic, self.trades_topic)

    def __post_init__(self) -> None:
        if self.holdback_ms < 0:
            raise ValueError(f"EXCHANGE_MERGE_HOLDBACK_MS can't be negative ({self.holdback_ms})")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RunnerSettings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        pairs = tuple(p.strip().upper() for p in get("SIMEX_PAIRS", ",".join(d.pairs)).split(","))
        return cls(
            pairs=tuple(p for p in pairs if p),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            metrics_port=int(get("EXCHANGE_METRICS_PORT", d.metrics_port)),
            snapshot_refresh_seconds=float(
                get("INGEST_SNAPSHOT_REFRESH_SECONDS", d.snapshot_refresh_seconds)
            ),
            holdback_ms=float(get("EXCHANGE_MERGE_HOLDBACK_MS", d.holdback_ms)),
            fetch_max_wait_ms=int(get("EXCHANGE_FETCH_MAX_WAIT_MS", d.fetch_max_wait_ms)),
            digest_path=get("EXCHANGE_DIGEST_PATH", d.digest_path),
        )
