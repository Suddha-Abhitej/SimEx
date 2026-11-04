"""Supervised live exchange runtime group."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

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


class LiveRuntimeGroup:
    """Keep the runner, MCP tools, reconciliation, and ledger on one live projection."""

    def __init__(
        self,
        settings: RunnerSettings,
        metrics: RunnerMetrics,
        source_factory: Callable[[], Any],
        *,
        ledger_metrics: LedgerMetrics | None = None,
        risk_limits: RiskLimits | None = None,
        server_runner: Callable[..., Any] = run_server_async,
        runner_factory: Callable[..., ExchangeRunner] = ExchangeRunner,
        store_factory: Callable[..., PostgresReconciliationStore] = PostgresReconciliationStore,
    ) -> None:
        self.settings = settings
        self.metrics = metrics
        self.source_factory = source_factory
        self.ledger_metrics = ledger_metrics
        self.risk_limits = risk_limits
        self.server_runner = server_runner
        self.runner_factory = runner_factory
        self.store_factory = store_factory
        self.ledger: SyncLedger | None = None
        self.runner: ExchangeRunner | None = None
        self.reconciliation: ReconciliationJob | None = None
        self.tools: ExchangeTools | None = None
        self.store: PostgresReconciliationStore | None = None
        self.tasks: list[asyncio.Task[Any]] = []

    async def run(self) -> None:
        source = self.source_factory()
        try:
            if self.settings.mcp_port is not None and self.risk_limits is None:
                suffixes = self.settings.pairs
                variables = ", ".join(
                    f"EXCHANGE_MCP_MAX_{kind}_{product.replace('-', '_')}"
                    for product in suffixes
                    for kind in ("QUANTITY", "NOTIONAL")
                )
                raise ValueError(f"EXCHANGE_MCP_PORT requires risk limits; set {variables}")
            if self.settings.ledger_dsn is not None:
                self.ledger = SyncLedger(self.settings.ledger_dsn, metrics=self.ledger_metrics)
                self.ledger.migrate()
            self.runner = self.runner_factory(
                self.settings, source, self.metrics, ledger=self.ledger
            )
            if self.ledger is not None:
                self.store = self.store_factory(self.settings.ledger_dsn)
                self.reconciliation = ReconciliationJob(
                    self.store,
                    self.ledger_metrics,
                    interval_seconds=self.settings.reconciliation_interval_seconds,
                )
                self.reconciliation.register_writer(self.settings.account_id, self.runner.writer)
            self.tasks.append(asyncio.create_task(self.runner.run(), name="exchange-runner"))
            if self.reconciliation is not None:
                self.tasks.append(
                    asyncio.create_task(self.reconciliation.run_forever(), name="reconciliation")
                )
            if self.ledger is not None and self.settings.mcp_port is not None:
                self.tools = ExchangeTools(
                    self.settings.account_id,
                    self.runner.writer,
                    self.runner.engines,
                    self.risk_limits,
                )
                self.tasks.append(
                    asyncio.create_task(
                        self.server_runner(self.tools, port=self.settings.mcp_port),
                        name="mcp-server",
                    )
                )
            done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()
            await asyncio.gather(*pending)
        finally:
            await self.close()

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        if self.store is not None:
            self.store.close()
            self.store = None
        if self.ledger is not None:
            self.ledger.close()
            self.ledger = None
