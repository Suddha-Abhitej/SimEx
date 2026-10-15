"""Run the book service: python -m simex.book

Keeps one order book per product from md.level2 and publishes each to Valkey every 250 ms as
simex:book:gemini:<product_id> (ADR 0012). Metrics are on BOOK_METRICS_PORT (9102).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

import valkey.asyncio as valkey
from prometheus_client import start_http_server

from simex.ingest.backoff import Backoff
from simex.ingest.control import KafkaResyncRequester

from .config import BookSettings
from .metrics import BookMetrics
from .service import BookService
from .source import KafkaLevel2Source

log = logging.getLogger("simex.book")


async def main() -> None:
    settings = BookSettings.from_env()
    metrics = BookMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)
    log.info("metrics on http://0.0.0.0:%d/metrics", settings.metrics_port)

    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on Windows; Ctrl+C still raises KeyboardInterrupt there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)

    store = valkey.from_url(settings.valkey_url)
    backoff = Backoff(1.0, 30.0)
    try:
        while True:
            source = KafkaLevel2Source(settings.kafka_bootstrap_servers, settings.level2_topic)
            requester = KafkaResyncRequester(settings.kafka_bootstrap_servers)
            service = BookService(settings, source, store, metrics, requester=requester)
            try:
                await requester.start()
                await service.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Kafka down or the topic missing: retry, books start over
                delay = backoff.next_delay()
                log.warning("book service stopped (%r); starting over in %.1fs", exc, delay)
                await asyncio.sleep(delay)
            finally:
                await requester.stop()
    finally:
        await store.aclose()


def run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())


if __name__ == "__main__":
    run()
