"""Reading md.* for the recorder, behind a small interface so tests can use an in-memory Kafka.

The recorder uses its own consumer group with auto-commit off: it commits a partition's offset
only once every record before it is in a published file (ADR 0013).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.structs import OffsetAndMetadata

from simex.ingest.kafka import MAX_MESSAGE_BYTES

log = logging.getLogger(__name__)

TP = tuple[str, int]  # (topic, partition)


@dataclass(frozen=True)
class SourceRecord:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    value: bytes


class RecordSource(Protocol):
    async def start(self) -> list[TP]:
        """Connect; return every partition of the subscribed topics that exist."""

    async def committed(self, tp: TP) -> int | None:
        """The group's committed offset, or None if it has none (new or expired)."""

    async def beginning_offsets(self) -> dict[TP, int]: ...

    async def end_offsets(self) -> dict[TP, int]: ...

    def seek(self, tp: TP, offset: int) -> None: ...

    async def fetch(self, timeout_ms: int) -> list[SourceRecord]:
        """The next records, in offset order within each partition."""

    async def commit(self, offsets: dict[TP, int]) -> None: ...

    async def retention_ms(self, topic: str) -> int | None:
        """retention.ms of the topic (-1: forever), or None if it can't be read."""

    async def stop(self) -> None: ...


class KafkaRecordSource:
    def __init__(self, bootstrap_servers: str, topics: tuple[str, ...], group_id: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            # Committed offsets expire (offsets.retention.minutes, 7 days by default); then
            # read from the log start and let the file scan drop what is already on disk.
            auto_offset_reset="earliest",
            max_partition_fetch_bytes=MAX_MESSAGE_BYTES,
            fetch_max_bytes=4 * MAX_MESSAGE_BYTES,
        )
        self._tps: dict[TP, TopicPartition] = {}

    async def _admin(self) -> AIOKafkaAdminClient:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        await admin.start()
        return admin

    async def start(self) -> list[TP]:
        admin = await self._admin()
        try:
            described = await admin.describe_topics(list(self.topics))
        finally:
            await admin.close()
        tps = sorted(
            (topic["topic"], partition["partition"])
            for topic in described
            if topic.get("error_code") == 0
            for partition in topic.get("partitions", [])
        )
        if not tps:
            raise RuntimeError(f"none of {', '.join(self.topics)} exist yet; start the ingestor")
        await self.consumer.start()
        # Assigned, not subscribed: one recorder process owns every partition, so there is no
        # rebalance to lose buffered records in. The group only holds the committed offsets.
        self._tps = {tp: TopicPartition(*tp) for tp in tps}
        self.consumer.assign(list(self._tps.values()))
        return tps

    async def committed(self, tp: TP) -> int | None:
        offset = await self.consumer.committed(self._tps[tp])
        return None if offset is None or offset < 0 else offset

    async def beginning_offsets(self) -> dict[TP, int]:
        found = await self.consumer.beginning_offsets(list(self._tps.values()))
        return {(tp.topic, tp.partition): offset for tp, offset in found.items()}

    async def end_offsets(self) -> dict[TP, int]:
        found = await self.consumer.end_offsets(list(self._tps.values()))
        return {(tp.topic, tp.partition): offset for tp, offset in found.items()}

    def seek(self, tp: TP, offset: int) -> None:
        self.consumer.seek(self._tps[tp], offset)

    async def fetch(self, timeout_ms: int) -> list[SourceRecord]:
        batches = await self.consumer.getmany(timeout_ms=timeout_ms, max_records=5000)
        return [
            SourceRecord(tp.topic, tp.partition, r.offset, r.timestamp, r.key, r.value)
            for tp, records in batches.items()
            for r in records
        ]

    async def commit(self, offsets: dict[TP, int]) -> None:
        await self.consumer.commit(
            {self._tps[tp]: OffsetAndMetadata(offset, "") for tp, offset in offsets.items()}
        )

    async def retention_ms(self, topic: str) -> int | None:
        try:
            admin = await self._admin()
        except Exception as exc:  # fall back to scanning everything on disk
            log.warning("cannot read %s retention: %r", topic, exc)
            return None
        try:
            responses = await admin.describe_configs(
                [ConfigResource(ConfigResourceType.TOPIC, topic)]
            )
        except Exception as exc:
            log.warning("cannot read %s retention: %r", topic, exc)
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
