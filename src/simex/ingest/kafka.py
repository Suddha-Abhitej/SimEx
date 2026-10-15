"""Kafka setup for the ingestor: topic creation and configs, and the producer."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import TopicAlreadyExistsError

log = logging.getLogger(__name__)

# A level2 snapshot for a deep book is several MB of JSON, over Kafka's 1 MB default.
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


def level2_topic_configs(retention_ms: int, retention_bytes: int) -> dict[str, str]:
    """Configs owned by the ingestor for md.level2 (ADR 0009)."""
    return {
        "max.message.bytes": str(MAX_MESSAGE_BYTES),
        "retention.ms": str(retention_ms),
        "retention.bytes": str(retention_bytes),
    }


def level2_retention_problem(retention_ms: int, refresh_seconds: float) -> str | None:
    """Why md.level2 might not hold a snapshot for every product, or None if it will.

    Book builders cold-start by reading md.level2 from 2x the refresh interval ago (ADR 0009),
    so the topic must keep at least that much. retention.ms of -1 means keep forever.
    """
    if refresh_seconds <= 0:
        return (
            "snapshot refresh is off (INGEST_SNAPSHOT_REFRESH_SECONDS=0): md.level2 may hold no "
            "snapshot and a book builder that starts later may never sync"
        )
    needed_ms = round(2 * refresh_seconds * 1000)
    if 0 <= retention_ms < needed_ms:
        return (
            f"md.level2 retention.ms={retention_ms} is less than 2x the snapshot refresh "
            f"interval ({needed_ms} ms): a book builder may find no snapshot on cold start"
        )
    return None


async def ensure_topics(
    bootstrap_servers: str,
    names: Iterable[str],
    partitions: int,
    replication_factor: int,
    configs: Mapping[str, Mapping[str, str]] | None = None,
) -> list[str]:
    """Create any missing topics; return the names that were created.

    Topics named in `configs` get those settings whether they are new or already exist, so a
    retention change takes effect on restart. The update is not incremental: Kafka resets the
    topic's other dynamic settings, so these topics' configs belong to the ingestor.
    """
    configs = configs or {}
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        missing = [name for name in names if name not in existing]
        if missing:
            topics = [
                NewTopic(
                    name,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    topic_configs={
                        "max.message.bytes": str(MAX_MESSAGE_BYTES),
                        **configs.get(name, {}),
                    },
                )
                for name in missing
            ]
            try:
                await admin.create_topics(topics)
            except TopicAlreadyExistsError:
                log.info("topics were created concurrently: %s", ", ".join(missing))
            else:
                log.info("created topics %s (%d partitions)", ", ".join(missing), partitions)
        to_alter = [name for name in configs if name in existing]
        if to_alter:
            responses = await admin.alter_configs(
                [
                    ConfigResource(ConfigResourceType.TOPIC, name, dict(configs[name]))
                    for name in to_alter
                ]
            )
            for response in responses:
                for error_code, error_message, _, name in response.resources:
                    if error_code:
                        log.error(
                            "cannot set configs on %s: %s (%d)", name, error_message, error_code
                        )
                    else:
                        log.info("set configs on %s: %s", name, dict(configs[name]))
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
