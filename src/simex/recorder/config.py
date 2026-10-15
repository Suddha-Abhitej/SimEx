"""Recorder settings, read from environment variables (see .env.example)."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

# The live venue's topics only (ADR 0011): research.<venue>.md.* is never recorded.
TOPICS = ("md.level2", "md.trades", "md.gaps")


@dataclass(frozen=True)
class RecorderSettings:
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    group_id: str = "simex-recorder"
    venue: str = "gemini"
    root: Path = Path("data/recordings")
    metrics_port: int = 9103
    # Days of recordings kept locally (ADR 0006: about 30 on Oracle, then the home archive).
    retention_days: int = 30
    # Keep expired days until the nightly copy to the home server has marked them _ARCHIVED.
    require_archived: bool = False
    # A row group is written this often (or at this many buffered bytes) while a file is open.
    row_group_seconds: float = 300.0
    row_group_bytes: int = 8 * 1024 * 1024
    # A file whose hour has ended is closed this long after the hour, even if no later record
    # for its product came (trades can be silent for minutes).
    hour_grace_seconds: float = 60.0

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RecorderSettings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        return cls(
            kafka_bootstrap_servers=get("KAFKA_BOOTSTRAP_SERVERS", d.kafka_bootstrap_servers),
            group_id=get("RECORDER_GROUP_ID", d.group_id),
            root=Path(get("RECORDER_ROOT", d.root)),
            metrics_port=int(get("RECORDER_METRICS_PORT", d.metrics_port)),
            retention_days=int(get("RECORDER_RETENTION_DAYS", d.retention_days)),
            require_archived=get("RECORDER_REQUIRE_ARCHIVED", "0") in ("1", "true", "yes"),
            row_group_seconds=float(get("RECORDER_ROW_GROUP_SECONDS", d.row_group_seconds)),
            row_group_bytes=int(get("RECORDER_ROW_GROUP_BYTES", d.row_group_bytes)),
            hour_grace_seconds=float(get("RECORDER_HOUR_GRACE_SECONDS", d.hour_grace_seconds)),
        )
