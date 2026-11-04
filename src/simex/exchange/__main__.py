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
from decimal import Decimal

from prometheus_client import start_http_server

from simex.ingest.backoff import Backoff
from simex.ledger import (
    LedgerMetrics,
    PostgresReconciliationStore,
    ReconciliationJob,
    SyncLedger,
)
from simex.mcp import ExchangeTools, RiskLimits, run_server_async

from .runner import ExchangeRunner
from .runner_config import RunnerSettings
from .runner_metrics import RunnerMetrics
from .source import KafkaMarketSource

log = logging.getLogger("simex.exchange")


async def main() -> None:
    settings = RunnerSettings.from_env()
    metrics = RunnerMetrics()
    ledger_metrics = (
        LedgerMetrics(registry=metrics.registry) if settings.ledger_dsn is not None else None
    )
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

    def risk_limits() -> RiskLimits:
        max_quantity = {}
        max_notional = {}
        for product in settings.pairs:
            suffix = product.replace("-", "_")
            max_quantity[product] = Decimal(
                os.environ.get(f"EXCHANGE_MCP_MAX_QUANTITY_{suffix}", "0")
            )
            max_notional[product] = Decimal(
                os.environ.get(f"EXCHANGE_MCP_MAX_NOTIONAL_{suffix}", "0")
            )
        return RiskLimits(max_quantity, max_notional)

    async def run_group() -> None:
        ledger = None
        source = KafkaMarketSource(
            settings.kafka_bootstrap_servers,
            settings.topics,
            fetch_max_wait_ms=settings.fetch_max_wait_ms,
        )
        reconciliation_store = None
        tasks = []
        try:
            if settings.ledger_dsn is not None:
                ledger = SyncLedger(settings.ledger_dsn, metrics=ledger_metrics)
                ledger.migrate()
            runner = ExchangeRunner(settings, source, metrics, ledger=ledger)
            if ledger is not None:
                reconciliation_store = PostgresReconciliationStore(settings.ledger_dsn)
                reconciliation = ReconciliationJob(
                    reconciliation_store,
                    ledger_metrics,
                    interval_seconds=settings.reconciliation_interval_seconds,
                )
                reconciliation.register_writer(settings.account_id, runner.writer)
            tasks.append(asyncio.create_task(runner.run(), name="exchange-runner"))
            if ledger is not None:
                tasks.append(
                    asyncio.create_task(reconciliation.run_forever(), name="reconciliation")
                )
            if ledger is not None and settings.mcp_port is not None:
                tools = ExchangeTools(
                    settings.account_id, runner.writer, runner.engines, risk_limits()
                )
                tasks.append(
                    asyncio.create_task(
                        run_server_async(tools, port=settings.mcp_port),
                        name="mcp-server",
                    )
                )
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()
            await asyncio.gather(*pending)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if reconciliation_store is not None:
                reconciliation_store.close()
            if ledger is not None:
                ledger.close()

    while True:
        try:
            await run_group()
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
