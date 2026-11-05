"""Live runner settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from simex.agents import BaselineSpec

from .events import DEFAULT_EVENT_BUFFER_SIZE

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
    stale_after_ms: float = 5000.0
    max_excess_lag_ms: float = 2000.0
    max_absolute_lag_ms: float = 30000.0
    lag_baseline_minutes: float = 10.0
    # Per pair and UTC hour, the order the runner released records in (for the replay check).
    digest_path: str = "data/exchange/merge-digests.jsonl"
    ledger_dsn: str | None = None
    account_id: str = "owner"
    initial_usd: Decimal = Decimal("100000")
    mcp_port: int | None = None
    api_port: int | None = None
    event_buffer_size: int = DEFAULT_EVENT_BUFFER_SIZE
    api_heartbeat_seconds: float = 15.0
    api_send_timeout_seconds: float = 10.0
    reconciliation_interval_seconds: float = 60.0
    baseline_agents: tuple[BaselineSpec, ...] = ()
    agent_cadence_seconds: float = 60.0
    agent_dca_quote_amount: Decimal = Decimal("100")

    @property
    def lookback_seconds(self) -> float:
        return 2 * self.snapshot_refresh_seconds

    @property
    def topics(self) -> tuple[str, str]:
        return (self.level2_topic, self.trades_topic)

    @property
    def account_ids(self) -> tuple[str, ...]:
        return (self.account_id,) + tuple(spec.account_id for spec in self.baseline_agents)

    def __post_init__(self) -> None:
        if self.api_port is not None and (
            isinstance(self.api_port, bool)
            or not isinstance(self.api_port, int)
            or not 1 <= self.api_port <= 65535
        ):
            raise ValueError("EXCHANGE_API_PORT must be between 1 and 65535")
        if (
            isinstance(self.event_buffer_size, bool)
            or not isinstance(self.event_buffer_size, int)
            or self.event_buffer_size <= 0
        ):
            raise ValueError("EXCHANGE_EVENT_BUFFER_SIZE must be a positive integer")
        for name, value in (
            ("EXCHANGE_API_HEARTBEAT_SECONDS", self.api_heartbeat_seconds),
            ("EXCHANGE_API_SEND_TIMEOUT_SECONDS", self.api_send_timeout_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.holdback_ms < 0:
            raise ValueError(f"EXCHANGE_MERGE_HOLDBACK_MS can't be negative ({self.holdback_ms})")
        if self.order_delay_ms < 0:
            raise ValueError(f"EXCHANGE_ORDER_DELAY_MS can't be negative ({self.order_delay_ms})")
        account_ids = self.account_ids
        if len(set(account_ids)) != len(account_ids):
            raise ValueError("exchange account IDs must be unique")
        if self.agent_cadence_seconds <= 0:
            raise ValueError("EXCHANGE_AGENT_CADENCE_SECONDS must be positive")
        if self.agent_dca_quote_amount <= 0:
            raise ValueError("EXCHANGE_AGENT_DCA_QUOTE_AMOUNT must be positive")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RunnerSettings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        pairs = tuple(p.strip().upper() for p in get("SIMEX_PAIRS", ",".join(d.pairs)).split(","))
        specs = []
        for item in get("EXCHANGE_BASELINE_AGENTS", "").split(","):
            fields = [field.strip() for field in item.split(":")]
            if not item.strip():
                continue
            if len(fields) not in (2, 3) or not fields[0] or not fields[1]:
                raise ValueError("EXCHANGE_BASELINE_AGENTS entries are kind:account[:seed]")
            specs.append(
                BaselineSpec(fields[0], fields[1], int(fields[2]) if len(fields) == 3 else 0)
            )
        return cls(
            pairs=tuple(p for p in pairs if p),
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            metrics_port=int(get("EXCHANGE_METRICS_PORT", d.metrics_port)),
            snapshot_refresh_seconds=float(
                get("INGEST_SNAPSHOT_REFRESH_SECONDS", d.snapshot_refresh_seconds)
            ),
            holdback_ms=float(get("EXCHANGE_MERGE_HOLDBACK_MS", d.holdback_ms)),
            fetch_max_wait_ms=int(get("EXCHANGE_FETCH_MAX_WAIT_MS", d.fetch_max_wait_ms)),
            order_delay_ms=float(get("EXCHANGE_ORDER_DELAY_MS", d.order_delay_ms)),
            digest_path=get("EXCHANGE_DIGEST_PATH", d.digest_path),
            stale_after_ms=float(get("EXCHANGE_STALE_AFTER_MS", d.stale_after_ms)),
            max_excess_lag_ms=float(get("EXCHANGE_MAX_EXCESS_LAG_MS", d.max_excess_lag_ms)),
            max_absolute_lag_ms=float(get("EXCHANGE_MAX_ABS_LAG_MS", d.max_absolute_lag_ms)),
            lag_baseline_minutes=float(
                get("EXCHANGE_LAG_BASELINE_MINUTES", d.lag_baseline_minutes)
            ),
            ledger_dsn=env.get("EXCHANGE_LEDGER_DSN") or None,
            account_id=get("EXCHANGE_ACCOUNT_ID", d.account_id),
            initial_usd=Decimal(get("EXCHANGE_INITIAL_USD", d.initial_usd)),
            mcp_port=(
                int(env["EXCHANGE_MCP_PORT"]) if env.get("EXCHANGE_MCP_PORT", "").strip() else None
            ),
            api_port=(
                int(env["EXCHANGE_API_PORT"]) if env.get("EXCHANGE_API_PORT", "").strip() else None
            ),
            event_buffer_size=int(get("EXCHANGE_EVENT_BUFFER_SIZE", d.event_buffer_size)),
            api_heartbeat_seconds=float(
                get("EXCHANGE_API_HEARTBEAT_SECONDS", d.api_heartbeat_seconds)
            ),
            api_send_timeout_seconds=float(
                get("EXCHANGE_API_SEND_TIMEOUT_SECONDS", d.api_send_timeout_seconds)
            ),
            reconciliation_interval_seconds=float(
                get("EXCHANGE_RECONCILIATION_INTERVAL_SECONDS", d.reconciliation_interval_seconds)
            ),
            baseline_agents=tuple(specs),
            agent_cadence_seconds=float(
                get("EXCHANGE_AGENT_CADENCE_SECONDS", d.agent_cadence_seconds)
            ),
            agent_dca_quote_amount=Decimal(
                get("EXCHANGE_AGENT_DCA_QUOTE_AMOUNT", d.agent_dca_quote_amount)
            ),
        )
