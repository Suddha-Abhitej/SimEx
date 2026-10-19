"""Run the live exchange runner: python -m simex.exchange

Merges md.level2 and md.trades per pair by received time and drives one fill engine per pair
up to the watermark (ADR 0015). Metrics are on EXCHANGE_METRICS_PORT (9104).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

from prometheus_client import start_http_server

from simex.ingest.backoff import Backoff

from .runner import ExchangeRunner
from .runner_config import RunnerSettings
from .runner_metrics import RunnerMetrics
from .source import KafkaMarketSource

log = logging.getLogger("simex.exchange")


async def main() -> None:
    settings = RunnerSettings.from_env()
    metrics = RunnerMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)
    log.info(
        "metrics on http://0.0.0.0:%d/metrics; hold-back %.0f ms",
        settings.metrics_port,
        settings.holdback_ms,
    )
    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on Windows; Ctrl+C still raises KeyboardInterrupt there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)
    backoff = Backoff(1.0, 30.0)
    while True:
        source = KafkaMarketSource(
            settings.kafka_bootstrap_servers,
            settings.topics,
            fetch_max_wait_ms=settings.fetch_max_wait_ms,
        )
        runner = ExchangeRunner(settings, source, metrics)
        try:
            await runner.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # Kafka down or a topic missing: start over from a snapshot
            delay = backoff.next_delay()
            log.warning("exchange runner stopped (%r); starting over in %.1fs", exc, delay)
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
