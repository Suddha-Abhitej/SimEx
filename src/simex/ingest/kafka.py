"""Kafka setup for the ingestor: topic creation and the producer."""

from __future__ import annotations

import logging
from collections.abc import Iterable

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

log = logging.getLogger(__name__)

# A level2 snapshot for a deep book is several MB of JSON, over Kafka's 1 MB default.
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


async def ensure_topics(
    bootstrap_servers: str, names: Iterable[str], partitions: int, replication_factor: int
) -> list[str]:
    """Create any missing topics; return the names that were created."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [name for name in names if name not in existing]
        if not missing:
            return []
        topics = [
            NewTopic(
                name,
                num_partitions=partitions,
                replication_factor=replication_factor,
                topic_configs={"max.message.bytes": str(MAX_MESSAGE_BYTES)},
            )
            for name in missing
        ]
        try:
            await admin.create_topics(topics)
        except TopicAlreadyExistsError:
            log.info("topics were created concurrently: %s", ", ".join(missing))
        else:
            log.info("created topics %s (%d partitions)", ", ".join(missing), partitions)
        return missing
    finally:
        await admin.close()


def create_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        linger_ms=5,
        max_request_size=MAX_MESSAGE_BYTES,
    )
