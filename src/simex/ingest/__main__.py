"""Run the Coinbase ingestor: python -m simex.ingest"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

from prometheus_client import start_http_server

from .backoff import Backoff
from .client import CoinbaseIngestor
from .config import Settings
from .envelope import TOPICS
from .kafka import create_producer, ensure_topics
from .metrics import IngestMetrics

log = logging.getLogger("simex.ingest")


async def connect_kafka(settings: Settings):
    """Create topics and start the producer, retrying until Kafka is reachable."""
    backoff = Backoff(settings.backoff_initial_seconds, settings.backoff_max_seconds)
    while True:
        producer = create_producer(settings.kafka_bootstrap_servers)
        try:
            await ensure_topics(
                settings.kafka_bootstrap_servers,
                TOPICS.values(),
                settings.topic_partitions,
                settings.topic_replication_factor,
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
    metrics = IngestMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)
    log.info("metrics on http://0.0.0.0:%d/metrics", settings.metrics_port)

    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on Windows; Ctrl+C still raises KeyboardInterrupt there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)

    producer = await connect_kafka(settings)
    try:
        await CoinbaseIngestor(settings, producer, metrics).run()
    finally:
        log.info("shutting down; flushing Kafka producer")
        await producer.stop()


def run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())


if __name__ == "__main__":
    run()
