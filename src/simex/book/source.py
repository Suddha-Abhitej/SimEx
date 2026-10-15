"""Reading md.level2 for the book service: the Kafka source and the cold-start plan (ADR 0009).

The service only needs a few operations from Kafka, behind Level2Source, so its logic can be
tested with an in-memory source. There is no consumer group and nothing is committed: every
start seeks by time, so a restart always rebuilds the books from a recent snapshot.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType

from simex.ingest.kafka import MAX_MESSAGE_BYTES

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Record:
    partition: int
    offset: int
    key: bytes | None
    value: bytes


class Level2Source(Protocol):
    async def start(self) -> list[int]:
        """Connect and return the topic's partitions."""

    async def offsets_for_time(self, timestamp_ms: int) -> dict[int, int | None]:
        """Per partition, the first offset at or after the time; None if every record is older."""

    async def beginning_offsets(self) -> dict[int, int]: ...

    async def end_offsets(self) -> dict[int, int]: ...

    def seek(self, partition: int, offset: int) -> None: ...

    async def fetch(self, timeout_ms: int) -> list[Record]:
        """The next records, in offset order within each partition."""

    async def retention_ms(self) -> int | None:
        """The topic's retention.ms, or None if it can't be read."""

    async def stop(self) -> None: ...


def plan_start_offsets(
    partitions: list[int],
    by_time: dict[int, int | None],
    beginnings: dict[int, int],
    ends: dict[int, int],
) -> dict[int, int]:
    """Where to start each partition: the first record inside the lookback window, clamped to the
    log start; at the end if every record in the partition is older than the window."""
    plan = {}
    for partition in partitions:
        offset = by_time.get(partition)
        if offset is None:
            offset = ends[partition]
        plan[partition] = max(offset, beginnings[partition])
    return plan


class KafkaLevel2Source:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            group_id=None,
            max_partition_fetch_bytes=MAX_MESSAGE_BYTES,
            fetch_max_bytes=4 * MAX_MESSAGE_BYTES,
        )
        self._tps: dict[int, TopicPartition] = {}

    async def _partitions(self) -> list[int]:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            described = await admin.describe_topics([self.topic])
        finally:
            await admin.close()
        return sorted(
            partition["partition"]
            for topic in described
            if topic.get("error_code") == 0
            for partition in topic.get("partitions", [])
        )

    async def start(self) -> list[int]:
        partitions = await self._partitions()
        if not partitions:
            raise RuntimeError(f"topic {self.topic} doesn't exist yet; start the ingestor first")
        await self.consumer.start()
        self._tps = {p: TopicPartition(self.topic, p) for p in partitions}
        self.consumer.assign(list(self._tps.values()))
        return sorted(self._tps)

    async def offsets_for_time(self, timestamp_ms: int) -> dict[int, int | None]:
        found = await self.consumer.offsets_for_times(
            {tp: timestamp_ms for tp in self._tps.values()}
        )
        return {tp.partition: (None if hit is None else hit.offset) for tp, hit in found.items()}

    async def beginning_offsets(self) -> dict[int, int]:
        found = await self.consumer.beginning_offsets(list(self._tps.values()))
        return {tp.partition: offset for tp, offset in found.items()}

    async def end_offsets(self) -> dict[int, int]:
        found = await self.consumer.end_offsets(list(self._tps.values()))
        return {tp.partition: offset for tp, offset in found.items()}

    def seek(self, partition: int, offset: int) -> None:
        self.consumer.seek(self._tps[partition], offset)

    async def fetch(self, timeout_ms: int) -> list[Record]:
        batches = await self.consumer.getmany(timeout_ms=timeout_ms, max_records=2000)
        return [
            Record(tp.partition, record.offset, record.key, record.value)
            for tp, records in batches.items()
            for record in records
        ]

    async def retention_ms(self) -> int | None:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        try:
            responses = await admin.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, self.topic)]
            )
        except Exception as exc:  # the check is advisory; never stop the service for it
            log.warning("cannot read %s retention: %r", self.topic, exc)
            return None
        finally:
            await admin.close()
        for response in responses:
            for resource in response.resources:
                for entry in resource[4]:
                    if entry[0] == "retention.ms":
                        return int(entry[1])
        return None

    async def stop(self) -> None:
        await self.consumer.stop()
