"""The resync back-channel: books ask the ingestor for a fresh snapshot (ADR 0012).

A book that finds itself wrong (crossed, or an audit mismatch) can't fix itself, and only the
ingestor talks to the exchange. The book service publishes a request to ctl.resync_requests; the
ingestor consumes it and runs its normal single-product resync: a resync marker, then a
resubscribe through the shared rate limiter, deduplicated per product. The topic keeps requests
for an hour only, and the ingestor reads it from the end when it starts, so an old request is
never replayed.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from .kafka import ensure_topics

log = logging.getLogger(__name__)

RESYNC_REQUESTS_TOPIC = "ctl.resync_requests"
RESYNC_REQUESTS_RETENTION_MS = 3_600_000
REQUEST_VERSION = 1


@dataclass(frozen=True)
class ResyncRequest:
    venue: str
    product_id: str
    reason: str  # e.g. "audit_mismatch", "crossed"
    requested_at_ns: int


def build_resync_request(venue: str, product_id: str, reason: str, requested_at_ns: int) -> bytes:
    return json.dumps(
        {
            "v": REQUEST_VERSION,
            "type": "resync_request",
            "venue": venue,
            "product_id": product_id,
            "reason": reason,
            "requested_at_ns": requested_at_ns,
        },
        separators=(",", ":"),
    ).encode()


def parse_resync_request(value: bytes) -> ResyncRequest | None:
    try:
        message = json.loads(value)
    except ValueError:
        return None
    if not isinstance(message, dict) or message.get("type") != "resync_request":
        return None
    try:
        return ResyncRequest(
            venue=str(message["venue"]),
            product_id=str(message["product_id"]),
            reason=str(message.get("reason", "")),
            requested_at_ns=int(message.get("requested_at_ns", 0)),
        )
    except (KeyError, TypeError, ValueError):
        return None


async def ensure_control_topic(bootstrap_servers: str, replication_factor: int = 1) -> None:
    await ensure_topics(
        bootstrap_servers,
        [RESYNC_REQUESTS_TOPIC],
        1,
        replication_factor,
        configs={RESYNC_REQUESTS_TOPIC: {"retention.ms": str(RESYNC_REQUESTS_RETENTION_MS)}},
    )


async def consume_resync_requests(
    bootstrap_servers: str, handle: Callable[[ResyncRequest], Awaitable[object]]
) -> None:
    """Read requests published from now on and hand each to `handle`, forever."""
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap_servers, enable_auto_commit=False)
    await consumer.start()
    try:
        partitions = [TopicPartition(RESYNC_REQUESTS_TOPIC, p) for p in range(1)]
        consumer.assign(partitions)
        await consumer.seek_to_end(*partitions)
        while True:
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    request = parse_resync_request(record.value)
                    if request is None:
                        log.warning("ignoring a malformed resync request: %.200r", record.value)
                        continue
                    await handle(request)
    finally:
        await consumer.stop()


class KafkaResyncRequester:
    """The book service's side: publish requests to ctl.resync_requests."""

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, acks="all")

    async def start(self) -> None:
        await ensure_control_topic(self.bootstrap_servers)
        await self.producer.start()

    async def send(self, venue: str, product_id: str, reason: str) -> None:
        value = build_resync_request(venue, product_id, reason, time.time_ns())
        await self.producer.send_and_wait(RESYNC_REQUESTS_TOPIC, value, key=product_id.encode())

    async def stop(self) -> None:
        await self.producer.stop()
