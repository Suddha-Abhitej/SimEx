"""Reading md.level2 and md.trades for the live runner (ADR 0015).

Like the book service's source (simex.book.source), there is no consumer group and nothing is
committed: every start seeks by time. Each fetch also reports which partitions had nothing more
to read, and the wall time that was true at, which is what lets the merge release records from a
quiet topic. The fetch wait is kept short, so a caught-up observation is never older than it.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from simex.ingest.kafka import MAX_MESSAGE_BYTES


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


@dataclass
class Fetched:
    records: list[Record]
    # Partitions read to their end as of observed_wall_ns: nothing more was in Kafka then.
    caught_up: list[tuple[str, int]] = field(default_factory=list)
    observed_wall_ns: int = 0


class MarketSource(Protocol):
    async def start(self) -> dict[str, list[int]]:
        """Connect; the partitions of each topic."""

    async def offsets_for_time(self, timestamp_ms: int) -> dict[tuple[str, int], int | None]: ...

    async def beginning_offsets(self) -> dict[tuple[str, int], int]: ...

    async def end_offsets(self) -> dict[tuple[str, int], int]: ...

    def seek(self, topic: str, partition: int, offset: int) -> None: ...

    async def fetch(self, timeout_ms: int) -> Fetched: ...

    async def stop(self) -> None: ...


class KafkaMarketSource:
    def __init__(
        self,
        bootstrap_servers: str,
        topics: tuple[str, ...],
        *,
        fetch_max_wait_ms: int = 20,
        wall_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self._wall_ns = wall_ns
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            group_id=None,
            # If a position falls out of range (retention deleted records the runner hadn't
            # read), go to the log start: losing only what Kafka already deleted. aiokafka's
            # default, latest, would silently skip everything up to the end.
            auto_offset_reset="earliest",
            fetch_max_wait_ms=fetch_max_wait_ms,
            max_partition_fetch_bytes=MAX_MESSAGE_BYTES,
            fetch_max_bytes=4 * MAX_MESSAGE_BYTES,
        )
        self._tps: dict[tuple[str, int], TopicPartition] = {}
        self._next: dict[tuple[str, int], int] = {}

    async def start(self) -> dict[str, list[int]]:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            described = await admin.describe_topics(list(self.topics))
        finally:
            await admin.close()
        found: dict[str, list[int]] = {}
        for topic in described:
            if topic.get("error_code") == 0:
                found[topic["topic"]] = sorted(p["partition"] for p in topic.get("partitions", []))
        missing = [t for t in self.topics if not found.get(t)]
        if missing:
            raise RuntimeError(f"topics {missing} don't exist yet; start the ingestor first")
        await self.consumer.start()
        self._tps = {(t, p): TopicPartition(t, p) for t, parts in found.items() for p in parts}
        self.consumer.assign(list(self._tps.values()))
        return found

    async def offsets_for_time(self, timestamp_ms: int) -> dict[tuple[str, int], int | None]:
        hits = await self.consumer.offsets_for_times(
            {tp: timestamp_ms for tp in self._tps.values()}
        )
        return {
            (tp.topic, tp.partition): (None if h is None else h.offset) for tp, h in hits.items()
        }

    async def beginning_offsets(self) -> dict[tuple[str, int], int]:
        found = await self.consumer.beginning_offsets(list(self._tps.values()))
        return {(tp.topic, tp.partition): offset for tp, offset in found.items()}

    async def end_offsets(self) -> dict[tuple[str, int], int]:
        found = await self.consumer.end_offsets(list(self._tps.values()))
        return {(tp.topic, tp.partition): offset for tp, offset in found.items()}

    def seek(self, topic: str, partition: int, offset: int) -> None:
        self.consumer.seek(self._tps[(topic, partition)], offset)
        self._next[(topic, partition)] = offset

    async def fetch(self, timeout_ms: int) -> Fetched:
        # The high-water marks come from the fetch responses buffered before this call, so the
        # wall time before it bounds how old a caught-up observation can be (with the fetch
        # wait, which the hold-back covers).
        observed = self._wall_ns()
        batches = await self.consumer.getmany(timeout_ms=timeout_ms, max_records=2000)
        records = []
        for tp, items in batches.items():
            for r in items:
                records.append(Record(tp.topic, tp.partition, r.offset, r.key, r.value))
            if items:
                self._next[(tp.topic, tp.partition)] = items[-1].offset + 1
        caught_up = []
        for key, tp in self._tps.items():
            highwater = self.consumer.highwater(tp)
            if highwater is not None and self._next.get(key, 0) >= highwater:
                caught_up.append(key)
        return Fetched(records, caught_up, observed)

    async def stop(self) -> None:
        await self.consumer.stop()
