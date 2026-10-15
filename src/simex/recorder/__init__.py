"""The recorder: md.* to hourly zstd Parquet files, and a replay reader for them (ADR 0013)."""

from .replay import RecordedEnvelope, read

__all__ = ["RecordedEnvelope", "read"]
