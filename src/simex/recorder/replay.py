"""Read recordings back in their original order (ADR 0013).

    from simex.recorder.replay import read
    for rec in read("data/recordings", start, end, products=["BTC-USD"], channels=["level2"]):
        for event in decode(rec.value): ...

`rec.value` is the exact Kafka record value, so the same simex.marketdata.decode and OrderBook
code runs on recordings as on the live topic. Within one product and channel, records come in
Kafka offset order: exactly the order the live consumers saw. Streams of different products and
channels are merged by received time (ties by topic, partition and offset), which is how they
interleaved when they arrived. Files are read one row group at a time, so memory stays small.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from .files import PartInfo, iter_parts

_COLUMNS = [
    "kafka_topic",
    "partition",
    "offset",
    "received_at_ns",
    "channel",
    "product_id",
    "type",
    "aligned",
    "reason",
    "value",
]


@dataclass(frozen=True)
class RecordedEnvelope:
    topic: str
    partition: int
    offset: int
    received_at_ns: int
    channel: str
    product_id: str | None
    type: str
    aligned: bool | None  # audits only
    reason: str | None
    value: bytes  # the envelope exactly as Kafka held it

    @property
    def sort_key(self) -> tuple[int, str, int, int]:
        return (self.received_at_ns, self.topic, self.partition, self.offset)


def _as_ns(when: datetime | int | None) -> int | None:
    if when is None or isinstance(when, int):
        return when
    if when.tzinfo is None:
        raise ValueError("use timezone-aware datetimes (UTC)")
    return int(when.timestamp() * 1e9)


def files(
    root: str | Path,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    *,
    venue: str = "gemini",
    products: Iterable[str] | None = None,
    channels: Iterable[str] | None = None,
) -> list[PartInfo]:
    """The published files that can hold records in [start, end), in reading order."""
    start_ns, end_ns = _as_ns(start), _as_ns(end)
    wanted_products = set(products) if products is not None else None
    wanted_channels = set(channels) if channels is not None else None
    since = None if start_ns is None else datetime.fromtimestamp(start_ns / 1e9, UTC)
    found = []
    for part in iter_parts(Path(root), since):
        venue_dir = part.path.parents[4].name.removeprefix("venue=")
        channel = part.path.parents[3].name.removeprefix("channel=")
        if venue_dir != venue:
            continue
        if wanted_channels is not None and channel not in wanted_channels:
            continue
        if wanted_products is not None and part.product not in wanted_products | {"all"}:
            continue
        # A file is dated by its first record and runs at most into the next hour or so (late
        # close, clock steps), so keep files whose hour starts before end and ends after start.
        hour_ns = int(part.hour.timestamp() * 1e9)
        if end_ns is not None and hour_ns >= end_ns:
            continue
        if start_ns is not None and hour_ns + int(2 * 3600e9) <= start_ns:
            continue
        found.append(part)
    return sorted(
        found, key=lambda p: (p.path.parents[3].name, p.product, p.topic, p.partition, p.first)
    )


def _stream(parts: list[PartInfo], start_ns: int | None, end_ns: int | None):
    """One (channel, product) stream: its files in offset order, deduplicated defensively."""
    last: dict[tuple[str, int], int] = {}
    for part in parts:
        handle = pq.ParquetFile(part.path)
        for batch in handle.iter_batches(columns=_COLUMNS, batch_size=4096):
            columns = batch.to_pydict()
            for i in range(batch.num_rows):
                received = columns["received_at_ns"][i]
                if start_ns is not None and received < start_ns:
                    continue
                if end_ns is not None and received >= end_ns:
                    continue
                tp = (columns["kafka_topic"][i], columns["partition"][i])
                offset = columns["offset"][i]
                if offset <= last.get(tp, -1):
                    continue
                last[tp] = offset
                yield RecordedEnvelope(
                    topic=tp[0],
                    partition=tp[1],
                    offset=offset,
                    received_at_ns=received,
                    channel=columns["channel"][i],
                    product_id=columns["product_id"][i],
                    type=columns["type"][i],
                    aligned=columns["aligned"][i],
                    reason=columns["reason"][i],
                    value=columns["value"][i],
                )


def read(
    root: str | Path,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    *,
    venue: str = "gemini",
    products: Iterable[str] | None = None,
    channels: Iterable[str] | None = None,
) -> Iterator[RecordedEnvelope]:
    """Recorded envelopes with received time in [start, end): offset order within each product
    and channel, merged across them by received time."""
    start_ns, end_ns = _as_ns(start), _as_ns(end)
    groups: dict[tuple[str, str], list[PartInfo]] = defaultdict(list)
    for part in files(root, start_ns, end_ns, venue=venue, products=products, channels=channels):
        groups[(part.path.parents[3].name, part.product)].append(part)
    streams = [_stream(parts, start_ns, end_ns) for _, parts in sorted(groups.items())]
    yield from heapq.merge(*streams, key=lambda rec: rec.sort_key)
