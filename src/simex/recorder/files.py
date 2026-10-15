"""Recording files: layout, the durable write sequence, the startup scan and retention (ADR 0013).

Layout, one file per product, channel and UTC hour (an hour can have more than one part after a
restart; the offsets in the name tell them apart):

    <root>/venue=gemini/channel=level2/date=2026-10-15/hour=07/product=BTC-USD/
        part-md.level2-p0-1200-35811.parquet

Every file holds a contiguous run of one partition's records for one product, in offset order.
A file is written as <name>.tmp and becomes visible only through publish(): fsync the file,
atomic os.replace to the final name, fsync the directory. Readers ignore *.tmp, and the recorder
deletes leftovers at startup, so a crash mid-file leaves nothing behind but records Kafka still
has. The file names are the record of what is on disk: scan() reads them to find, per topic,
partition and product, the highest offset already recorded.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .schema import SCHEMA, SCHEMA_VERSION

log = logging.getLogger(__name__)

ARCHIVED_MARKER = "_ARCHIVED"  # written by the (future) nightly copy to the home server
_PART = re.compile(r"part-(?P<topic>.+)-p(?P<partition>\d+)-(?P<first>\d+)-(?P<last>\d+)\.parquet")
_DATE = re.compile(r"date=(\d{4}-\d{2}-\d{2})")

# Directory fsync makes a rename durable on POSIX. Windows can't open a directory for fsync
# (NTFS journals the rename itself), so it is skipped there.
IS_WINDOWS = os.name == "nt"
_warned_windows = False


def hour_of(time_ns: int) -> datetime:
    return datetime.fromtimestamp(time_ns // 1_000_000_000, UTC).replace(
        minute=0, second=0, microsecond=0
    )


def directory(root: Path, venue: str, channel: str, hour: datetime, product: str) -> Path:
    return (
        root
        / f"venue={venue}"
        / f"channel={channel}"
        / f"date={hour:%Y-%m-%d}"
        / f"hour={hour:%H}"
        / f"product={product}"
    )


def part_name(topic: str, partition: int, first: int, last: int) -> str:
    return f"part-{topic}-p{partition}-{first}-{last}.parquet"


@dataclass(frozen=True)
class PartInfo:
    path: Path
    topic: str
    partition: int
    first: int
    last: int
    product: str
    hour: datetime


def parse_part(path: Path) -> PartInfo | None:
    match = _PART.fullmatch(path.name)
    if match is None:
        return None
    try:
        product = path.parent.name.removeprefix("product=")
        hour = int(path.parent.parent.name.removeprefix("hour="))
        day = date.fromisoformat(path.parent.parent.parent.name.removeprefix("date="))
    except ValueError:
        return None
    return PartInfo(
        path=path,
        topic=match["topic"],
        partition=int(match["partition"]),
        first=int(match["first"]),
        last=int(match["last"]),
        product=product,
        hour=datetime(day.year, day.month, day.day, hour, tzinfo=UTC),
    )


# The durable write sequence ---------------------------------------------------------------------


def fsync_file(path: Path) -> None:
    with open(path, "rb+") as handle:
        os.fsync(handle.fileno())


def fsync_dir(path: Path) -> None:
    global _warned_windows
    if IS_WINDOWS:
        if not _warned_windows:
            log.debug("directory fsync is not supported on Windows; relying on NTFS for renames")
            _warned_windows = True
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def publish(tmp: Path, final: Path) -> None:
    """Make a finished .tmp file visible, durably: fsync, atomic replace, fsync the directory.
    os.replace (not rename) so it is atomic and also works on Windows."""
    fsync_file(tmp)
    os.replace(tmp, final)
    fsync_dir(final.parent)


# Writing one file ------------------------------------------------------------------------------


@dataclass
class HourFile:
    """One open recording file: a product's records from one partition, within one hour. Rows
    are buffered and written as a row group every few minutes; the file is published on close."""

    root: Path
    venue: str
    channel: str
    product: str
    topic: str
    partition: int
    hour: datetime
    first_offset: int
    opened_mono: float = field(default_factory=time.monotonic)
    last_offset: int = -1
    rows: int = 0
    bytes_buffered: int = 0
    _buffer: list[dict] = field(default_factory=list)
    _writer: pq.ParquetWriter | None = None

    @property
    def dir(self) -> Path:
        return directory(self.root, self.venue, self.channel, self.hour, self.product)

    @property
    def tmp(self) -> Path:
        return self.dir / f"{part_name(self.topic, self.partition, self.first_offset, 0)}.tmp"

    def add(self, row: dict) -> None:
        self._buffer.append(row)
        self.last_offset = row["offset"]
        self.rows += 1
        self.bytes_buffered += len(row["value"])

    def flush_row_group(self) -> int:
        """Write the buffered rows as one row group; returns how many."""
        if not self._buffer:
            return 0
        if self._writer is None:
            self.dir.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                self.tmp, SCHEMA, compression="zstd", compression_level=3
            )
        table = pa.Table.from_pylist(self._buffer, schema=SCHEMA)
        self._writer.write_table(table, row_group_size=len(self._buffer))
        written = len(self._buffer)
        self._buffer = []
        self.bytes_buffered = 0
        return written

    def close(self) -> Path:
        """Finish the file and publish it under its final name. Returns that path."""
        self.flush_row_group()
        assert self._writer is not None, "a file is only opened with a record in it"
        self._writer.add_key_value_metadata(
            {
                "simex.schema_version": str(SCHEMA_VERSION),
                "simex.topic": self.topic,
                "simex.partition": str(self.partition),
                "simex.first_offset": str(self.first_offset),
                "simex.last_offset": str(self.last_offset),
                "simex.rows": str(self.rows),
                "simex.venue": self.venue,
                "simex.channel": self.channel,
                "simex.product": self.product,
                "simex.hour": self.hour.isoformat(),
            }
        )
        self._writer.close()
        final = self.dir / part_name(
            self.topic, self.partition, self.first_offset, self.last_offset
        )
        publish(self.tmp, final)
        return final

    def abandon(self) -> None:
        """Drop the file without publishing it (its records were never committed)."""
        if self._writer is not None:
            self._writer.close()
        self.tmp.unlink(missing_ok=True)


def write_file(
    root: Path, venue: str, channel: str, product: str, hour: datetime, rows: list[dict]
) -> Path:
    """Write and publish a whole file at once (the recorder's own gap rows)."""
    first = rows[0]
    hour_file = HourFile(
        root,
        venue,
        channel,
        product,
        first["kafka_topic"],
        first["partition"],
        hour,
        first["offset"],
    )
    for row in rows:
        hour_file.add(row)
    return hour_file.close()


# Startup scan and retention --------------------------------------------------------------------


def iter_parts(root: Path, since: datetime | None = None) -> Iterator[PartInfo]:
    """Every published file under root, optionally only from date directories on or after
    `since`'s date."""
    if not root.exists():
        return
    for path in root.glob("venue=*/channel=*/date=*/hour=*/product=*/part-*.parquet"):
        if since is not None:
            match = _DATE.search(str(path))
            if match and date.fromisoformat(match[1]) < since.date():
                continue
        info = parse_part(path)
        if info is not None:
            yield info


def remove_tmp(root: Path) -> int:
    """Delete files a crash left unfinished; their records were never committed."""
    removed = 0
    if root.exists():
        for path in root.rglob("*.tmp"):
            path.unlink(missing_ok=True)
            removed += 1
    return removed


def high_water(parts: Iterable[PartInfo]) -> dict[tuple[str, int, str], int]:
    """Per (topic, partition, product): the highest offset already on disk."""
    marks: dict[tuple[str, int, str], int] = {}
    for part in parts:
        key = (part.topic, part.partition, part.product)
        marks[key] = max(marks.get(key, -1), part.last)
    return marks


def expired_days(
    root: Path, now: datetime, keep_days: int, require_archived: bool
) -> list[tuple[Path, str]]:
    """Date directories past retention, with why each is kept or deleted. Today and yesterday
    are always kept, whatever keep_days says."""
    cutoff = (now - timedelta(days=max(keep_days, 2))).date()
    found = []
    for path in sorted(root.glob("venue=*/channel=*/date=*")):
        match = _DATE.fullmatch(path.name)
        if not match or date.fromisoformat(match[1]) >= cutoff:
            continue
        if require_archived and not (path / ARCHIVED_MARKER).exists():
            found.append((path, "kept: not archived yet"))
        else:
            found.append((path, "deleted"))
    return found


def apply_retention(root: Path, now: datetime, keep_days: int, require_archived: bool) -> int:
    deleted = 0
    for path, verdict in expired_days(root, now, keep_days, require_archived):
        if verdict == "deleted":
            shutil.rmtree(path)
            deleted += 1
            log.info("retention: deleted %s", path)
        else:
            log.warning("retention: %s %s (RECORDER_REQUIRE_ARCHIVED=1)", path, verdict)
    return deleted
