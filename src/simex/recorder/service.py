"""The recorder: md.level2, md.trades and md.gaps to hourly zstd Parquet files (ADR 0013).

Exactly once, across crashes and restarts:

1. Records are appended to the open file for their (topic, partition, product); its rows go to a
   .tmp file as row groups every few minutes.
2. When the hour ends, the file is closed and published (fsync, atomic replace, directory fsync;
   see files.publish).
3. Only then is a partition's offset committed: the lowest offset still in an open file on that
   partition, or the position if none is open.
4. At startup, leftover .tmp files are deleted (never committed, so Kafka still has their
   records), the published file names give each (topic, partition, product)'s highest recorded
   offset, and any record at or below it is skipped. So a crash between publishing a file and
   committing its offsets re-reads records without writing them twice, and so does a group whose
   committed offsets have expired (the consumer then starts from the log start).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .config import TOPICS, RecorderSettings
from .files import (
    HourFile,
    apply_retention,
    high_water,
    hour_of,
    iter_parts,
    remove_tmp,
    write_file,
)
from .metrics import RecorderMetrics
from .schema import GAPS_PRODUCT, RECORDER_TOPIC, TOPIC_CHANNELS, Unrecordable, place
from .source import TP, RecordSource, SourceRecord

log = logging.getLogger(__name__)

HOUR = timedelta(hours=1)
SCAN_MARGIN = timedelta(days=1)  # files are dated by received time, Kafka retention by append
LAG_REFRESH_SECONDS = 15.0
RETENTION_EVERY_SECONDS = 3600.0

Key = tuple[str, int, str]  # (topic, partition, product)


class Recorder:
    def __init__(
        self,
        settings: RecorderSettings,
        source: RecordSource,
        metrics: RecorderMetrics,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.source = source
        self.metrics = metrics
        self.root = Path(settings.root)
        self._clock_ns = clock_ns
        self._mono = monotonic
        self.writers: dict[Key, HourFile] = {}
        self.flushed_at: dict[Key, float] = {}
        self.marks: dict[Key, int] = {}  # highest offset on disk per key
        self.position: dict[TP, int] = {}
        self.committed: dict[TP, int | None] = {}
        self.last_received_ns: dict[str, int] = {}  # per channel
        self.partition_time_ns: dict[TP, int] = {}  # latest record time per partition
        self.ends: dict[TP, int] = {}  # end offsets, refreshed with the lag gauges
        self.last_write_mono: float | None = None
        self.last_file_mono: float | None = None
        self._lag_at: float | None = None
        self._retention_at: float | None = None

    # Startup -----------------------------------------------------------------------------------

    async def start(self) -> None:
        tps = await self.source.start()
        if removed := remove_tmp(self.root):
            log.warning("removed %d unfinished .tmp file(s) left by a crash", removed)
        await self._scan_files({topic for topic, _ in tps})
        beginnings = await self.source.beginning_offsets()
        for tp in tps:
            committed = await self.source.committed(tp)
            start = beginnings[tp]
            if committed is None:
                log.warning(
                    "%s p%d: no committed offset (new or expired group); reading from the log "
                    "start %d and skipping what the files already hold",
                    *tp,
                    start,
                )
            elif committed < start:
                self._record_loss(tp, committed, start)
            else:
                start = committed
            self.source.seek(tp, start)
            self.position[tp] = start
            self.committed[tp] = committed
        await self._update_lag()
        self._apply_retention()

    async def _scan_files(self, topics: set[str]) -> None:
        """Find what is already on disk, as far back as Kafka could still hand it to us."""
        now = datetime.fromtimestamp(self._clock_ns() / 1e9, UTC)
        since: dict[str, datetime | None] = {}
        for topic in topics:
            retention = await self.source.retention_ms(topic)
            if retention is None or retention < 0:
                since[topic] = None  # unknown or forever: scan everything
            else:
                since[topic] = now - timedelta(milliseconds=retention) - SCAN_MARGIN
        oldest = None if any(s is None for s in since.values()) else min(since.values())
        parts = [
            part
            for part in iter_parts(self.root, oldest)
            if part.topic in since
            and (since[part.topic] is None or part.hour >= since[part.topic] - HOUR)
        ]
        self.marks = high_water(parts)
        log.info(
            "scanned %d recording file(s) back to %s: %d (topic, partition, product) high-water "
            "marks",
            len(parts),
            "the start" if oldest is None else f"{oldest:%Y-%m-%d %H:%M}",
            len(self.marks),
        )

    def _record_loss(self, tp: TP, committed: int, log_start: int) -> None:
        """Kafka deleted records the recorder never got to. Some may be on disk already (other
        products' files), so the count is an upper bound; the gap row marks the window."""
        topic, partition = tp
        lost = log_start - committed
        self.metrics.lost_offsets.labels(topic).inc(lost)
        log.error(
            "%s p%d: offsets %d..%d expired from Kafka before they were recorded (at most %d "
            "records lost); writing a recorder_gap row",
            topic,
            partition,
            committed,
            log_start - 1,
            lost,
        )
        now_ns = self._clock_ns()
        value = json.dumps(
            {
                "v": 1,
                "type": "recorder_gap",
                "venue": self.settings.venue,
                "topic": topic,
                "partition": partition,
                "from_offset": committed,
                "to_offset": log_start - 1,
                "detected_at_ns": now_ns,
            },
            separators=(",", ":"),
        ).encode()
        row = {
            "kafka_topic": RECORDER_TOPIC,
            "partition": partition,
            "offset": committed,
            "kafka_ts_ms": None,
            "key": None,
            "received_at_ns": now_ns,
            "exchange_ts_ns": None,
            "venue": self.settings.venue,
            "channel": "gaps",
            "product_id": None,
            "type": "recorder_gap",
            "snapshot": None,
            "update_first": None,
            "update_last": None,
            "awaiting_snapshot": None,
            "update_id": None,
            "aligned": None,
            "reason": f"{topic} offsets {committed}..{log_start - 1} expired",
            "value": value,
        }
        hour = hour_of(now_ns)
        write_file(self.root, self.settings.venue, "gaps", GAPS_PRODUCT, hour, [row])

    # Records -----------------------------------------------------------------------------------

    def handle(self, record: SourceRecord) -> None:
        tp = (record.topic, record.partition)
        self.position[tp] = record.offset + 1
        if record.value is None:
            self.metrics.skipped.labels(record.topic, "unrecordable").inc()
            return
        try:
            placed = place(
                record.topic,
                record.partition,
                record.offset,
                record.timestamp_ms,
                record.key,
                record.value,
            )
        except Unrecordable as exc:
            self.metrics.skipped.labels(record.topic, "unrecordable").inc()
            log.warning("%s p%d@%d not recorded: %s", *tp, record.offset, exc)
            return
        if placed.venue != self.settings.venue:
            self.metrics.skipped.labels(record.topic, "foreign_venue").inc()
            return
        key = (record.topic, record.partition, placed.product)
        if record.offset <= self.marks.get(key, -1):
            self.metrics.duplicates_skipped.labels(record.topic).inc()
            return
        horizon_ns = self._clock_ns() - self.settings.retention_days * 86_400 * 1_000_000_000
        if placed.time_ns < horizon_ns:
            self.metrics.skipped.labels(record.topic, "past_retention").inc()
            return
        hour = hour_of(placed.time_ns)
        writer = self.writers.get(key)
        # A file only rolls forward: if the clock steps back, records stay in the open file, so
        # every file is one contiguous run of offsets.
        if writer is not None and hour > writer.hour:
            self._close(key)
            writer = None
        if writer is None:
            writer = HourFile(
                self.root,
                placed.venue,
                placed.channel,
                placed.product,
                record.topic,
                record.partition,
                hour,
                record.offset,
            )
            self.writers[key] = writer
            self.flushed_at[key] = self._mono()
        writer.add(placed.row)
        self.metrics.records.labels(placed.channel, placed.product, placed.row["type"]).inc()
        self.last_received_ns[placed.channel] = placed.time_ns
        self.partition_time_ns[tp] = max(self.partition_time_ns.get(tp, 0), placed.time_ns)
        if writer.bytes_buffered >= self.settings.row_group_bytes:
            self._flush(key)

    def _flush(self, key: Key) -> None:
        if self.writers[key].flush_row_group():
            self.last_write_mono = self._mono()
        self.flushed_at[key] = self._mono()

    def _close(self, key: Key) -> None:
        writer = self.writers.pop(key)
        self.flushed_at.pop(key, None)
        path = writer.close()
        self.marks[key] = writer.last_offset
        self.metrics.files.labels(writer.channel).inc()
        self.metrics.bytes.labels(writer.channel).inc(path.stat().st_size)
        self.last_write_mono = self.last_file_mono = self._mono()
        log.info("published %s (%d rows)", path, writer.rows)

    # Housekeeping ------------------------------------------------------------------------------

    def commit_offsets(self) -> dict[TP, int]:
        """Per partition, the offset that is safe to commit: nothing before it is only in an
        open file."""
        safe = dict(self.position)
        for (topic, partition, _), writer in self.writers.items():
            tp = (topic, partition)
            safe[tp] = min(safe.get(tp, writer.first_offset), writer.first_offset)
        return safe

    async def housekeeping(self) -> None:
        mono = self._mono()
        now_ns = self._clock_ns()
        now = datetime.fromtimestamp(now_ns / 1e9, UTC)
        grace = timedelta(seconds=self.settings.hour_grace_seconds)
        for key, writer in list(self.writers.items()):
            if self._hour_over(key, writer.hour + HOUR + grace, now):
                self._close(key)
            elif mono - self.flushed_at[key] >= self.settings.row_group_seconds:
                self._flush(key)
        await self._commit()
        self._update_gauges(mono, now_ns)
        if self._lag_at is None or mono - self._lag_at >= LAG_REFRESH_SECONDS:
            self._lag_at = mono
            await self._update_lag()
        if self._retention_at is None or mono - self._retention_at >= RETENTION_EVERY_SECONDS:
            self._apply_retention()

    def _hour_over(self, key: Key, closes_at: datetime, now: datetime) -> bool:
        """Close a file before the next hour's record for its product arrives only if the
        partition's stream has moved past the hour (another product's records), or the
        partition is caught up and the wall clock says so (a silent product). Never by the wall
        clock alone: catching up on a backlog, every old hour is over, and closing after each
        fetch would split an hour into many files."""
        tp = (key[0], key[1])
        closes_ns = int(closes_at.timestamp() * 1e9)
        if self.partition_time_ns.get(tp, 0) >= closes_ns:
            return True
        caught_up = self.position.get(tp, 0) >= self.ends.get(tp, 2**63)
        return caught_up and now >= closes_at

    async def _commit(self) -> None:
        changed = {
            tp: offset
            for tp, offset in self.commit_offsets().items()
            if self.committed.get(tp) is None or offset > self.committed[tp]
        }
        if changed:
            await self.source.commit(changed)
            self.committed.update(changed)

    def _update_gauges(self, mono: float, now_ns: int) -> None:
        for channel, received in self.last_received_ns.items():
            self.metrics.lag_seconds.labels(channel).set(max(0.0, (now_ns - received) / 1e9))
        if self.last_write_mono is not None:
            self.metrics.seconds_since_last_write.set(mono - self.last_write_mono)
        if self.last_file_mono is not None:
            self.metrics.seconds_since_last_file.set(mono - self.last_file_mono)
        self.metrics.open_files.set(len(self.writers))

    async def _update_lag(self) -> None:
        try:
            ends = await self.source.end_offsets()
        except Exception as exc:  # metrics only; never stop recording for them
            log.warning("cannot read end offsets: %r", exc)
            return
        self.ends = dict(ends)
        for (topic, partition), end in ends.items():
            position = self.position.get((topic, partition), end)
            self.metrics.lag_records.labels(topic, str(partition)).set(max(0, end - position))

    def _apply_retention(self) -> None:
        self._retention_at = self._mono()
        now = datetime.fromtimestamp(self._clock_ns() / 1e9, UTC)
        try:
            result = apply_retention(
                self.root, now, self.settings.retention_days, self.settings.require_archived
            )
        except OSError as exc:  # listing the directories failed; recording must go on
            log.warning("retention pass failed (%r); retrying on the next pass", exc)
            self.metrics.retention_errors.inc()
            return
        if result.deleted:
            self.metrics.retention_deleted.inc(result.deleted)
        if result.failed:
            self.metrics.retention_errors.inc(result.failed)

    # Run ---------------------------------------------------------------------------------------

    async def step(self, timeout_ms: int = 1000) -> int:
        records = await self.source.fetch(timeout_ms)
        for record in records:
            self.handle(record)
        await self.housekeeping()
        return len(records)

    async def close(self) -> None:
        """A clean stop: publish every open file (the hour may get a second part after the
        restart) and commit."""
        for key in list(self.writers):
            self._close(key)
        try:
            await self._commit()
        except Exception as exc:  # the files are published; a restart dedupes against them
            log.warning(
                "cannot commit offsets on shutdown (%r); the next start skips what the files hold",
                exc,
            )

    async def run(self) -> None:
        await self.start()
        log.info(
            "recording %s to %s (group %s)",
            ", ".join(t for t in TOPICS if t in TOPIC_CHANNELS),
            self.root,
            self.settings.group_id,
        )
        try:
            while True:
                await self.step()
        finally:
            await self.close()
