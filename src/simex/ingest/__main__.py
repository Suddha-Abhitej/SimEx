"""Run the market-data ingestor: python -m simex.ingest

SIMEX_VENUE picks the venue: gemini (default, the live venue) or coinbase (feed research only,
needs SIMEX_COINBASE_RESEARCH=1, publishes to research.coinbase.md.*). See ADR 0010 and 0011.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

from prometheus_client import start_http_server

from .backoff import Backoff
from .client import Ingestor
from .config import Settings
from .control import consume_resync_requests, ensure_control_topic
from .envelope import Topics
from .kafka import (
    create_producer,
    ensure_topics,
    level2_retention_problem,
    level2_topic_configs,
)
from .metrics import IngestMetrics

log = logging.getLogger("simex.ingest")


async def connect_kafka(settings: Settings, topics: Topics, channels: tuple[str, ...]):
    """Create topics and start the producer, retrying until Kafka is reachable."""
    backoff = Backoff(settings.backoff_initial_seconds, settings.backoff_max_seconds)
    while True:
        producer = create_producer(settings.kafka_bootstrap_servers)
        try:
            await ensure_topics(
                settings.kafka_bootstrap_servers,
                topics.for_channels(channels),
                settings.topic_partitions,
                settings.topic_replication_factor,
                configs={
                    topics.level2: level2_topic_configs(
                        settings.level2_retention_ms, settings.level2_retention_bytes
                    )
                },
            )
            await producer.start()
            return producer
        except Exception as exc:  # aiokafka raises several unrelated types when Kafka is down
            await producer.stop()
            delay = backoff.next_delay()
            log.warning(
                "Kafka at %s not ready (%r); retrying in %.1fs",
                settings.kafka_bootstrap_servers,
                exc,
                delay,
            )
            await asyncio.sleep(delay)


async def main() -> None:
    settings = Settings.from_env()
    try:
        settings.check()
    except ValueError as exc:
        raise SystemExit(f"simex.ingest: {exc}") from None
    if problem := level2_retention_problem(
        settings.level2_retention_ms, settings.snapshot_refresh_seconds
    ):
        log.error("%s (ADR 0009)", problem)
    metrics = IngestMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)
    log.info("metrics on http://0.0.0.0:%d/metrics", settings.metrics_port)

    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on Windows; Ctrl+C still raises KeyboardInterrupt there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)

    producer = None
    ingestor = Ingestor(settings, producer, metrics)
    if ingestor.venue != "gemini":
        log.warning(
            "RESEARCH ONLY: %s data goes to %s and must never feed agents, matching or scoring "
            "(ADR 0010)",
            ingestor.venue,
            ingestor.topics.level2.removesuffix("level2") + "*",
        )
    producer = await connect_kafka(settings, ingestor.topics, ingestor.adapter.channels)
    ingestor.producer = producer
    requests = asyncio.create_task(resync_requests_forever(settings, ingestor))
    try:
        await ingestor.run()
    finally:
        requests.cancel()
        await asyncio.gather(requests, return_exceptions=True)
        log.info("shutting down; flushing Kafka producer")
        await producer.stop()


async def resync_requests_forever(settings: Settings, ingestor: Ingestor) -> None:
    """Serve the books' resync requests (ctl.resync_requests, ADR 0012); retry if Kafka drops."""
    backoff = Backoff(settings.backoff_initial_seconds, settings.backoff_max_seconds)
    while True:
        try:
            await ensure_control_topic(
                settings.kafka_bootstrap_servers, settings.topic_replication_factor
            )
            log.info("serving resync requests from the book service")
            await consume_resync_requests(settings.kafka_bootstrap_servers, ingestor.request_resync)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # aiokafka raises several unrelated types when Kafka is down
            delay = backoff.next_delay()
            log.warning("resync request consumer stopped (%r); retrying in %.1fs", exc, delay)
            await asyncio.sleep(delay)


def run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())


if __name__ == "__main__":
    run()
