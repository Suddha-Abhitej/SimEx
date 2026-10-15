"""Run the recorder: python -m simex.recorder

Records md.level2, md.trades and md.gaps to hourly zstd Parquet files under RECORDER_ROOT
(data/recordings), exactly once across crashes and restarts (ADR 0013). Metrics are on
RECORDER_METRICS_PORT (9103).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

from prometheus_client import start_http_server

from simex.ingest.backoff import Backoff

from .config import TOPICS, RecorderSettings
from .metrics import RecorderMetrics
from .service import Recorder
from .source import KafkaRecordSource

log = logging.getLogger("simex.recorder")


async def main() -> None:
    settings = RecorderSettings.from_env()
    metrics = RecorderMetrics()
    start_http_server(settings.metrics_port, registry=metrics.registry)
    log.info("metrics on http://0.0.0.0:%d/metrics", settings.metrics_port)

    task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Not available on Windows; Ctrl+C still raises KeyboardInterrupt there.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, task.cancel)

    backoff = Backoff(1.0, 30.0)
    while True:
        source = KafkaRecordSource(settings.kafka_bootstrap_servers, TOPICS, settings.group_id)
        recorder = Recorder(settings, source, metrics)
        try:
            await recorder.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # Kafka down or topics missing: start over; files dedupe
            delay = backoff.next_delay()
            log.warning("recorder stopped (%r); starting over in %.1fs", exc, delay)
            await asyncio.sleep(delay)
        finally:
            with contextlib.suppress(Exception):
                await source.stop()


def run() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())


if __name__ == "__main__":
    run()
