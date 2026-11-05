"""Supervised live exchange runtime group."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from simex.agents import BaselineAgent
from simex.api import run_server_async as run_api_server_async
from simex.api import validate_api_configuration
from simex.ledger import (
    LedgerMetrics,
    PostgresReconciliationStore,
    ReconciliationJob,
    SyncLedger,
)
from simex.mcp import ExchangeTools, run_server_async

from .risk import RiskLimits
from .runner import ExchangeRunner
from .runner_config import RunnerSettings
from .runner_metrics import RunnerMetrics

log = logging.getLogger(__name__)


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
        api_runner: Callable[..., Any] = run_api_server_async,
        api_token: str | None = None,
        runner_factory: Callable[..., ExchangeRunner] = ExchangeRunner,
        store_factory: Callable[..., PostgresReconciliationStore] = PostgresReconciliationStore,
    ) -> None:
        self.settings = settings
        self.metrics = metrics
        self.source_factory = source_factory
        self.ledger_metrics = ledger_metrics
        self.risk_limits = risk_limits
        self.server_runner = server_runner
        self.api_runner = api_runner
        self._api_token = api_token
        self.runner_factory = runner_factory
        self.store_factory = store_factory
        self.ledger: SyncLedger | None = None
        self.source: Any | None = None
        self.runner: ExchangeRunner | None = None
        self.reconciliation: ReconciliationJob | None = None
        self.tools: ExchangeTools | None = None
        self.tools_by_account: dict[str, ExchangeTools] = {}
        self.store: PostgresReconciliationStore | None = None
        self.tasks: list[asyncio.Task[Any]] = []
        self.agents: list[BaselineAgent] = []

    async def _run_agent(self, agent: BaselineAgent) -> None:
        try:
            await agent.run_forever()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.metrics.agent_errors.labels(agent.spec.kind, agent.spec.account_id).inc()
            logging.getLogger(__name__).exception(
                "baseline agent %s for %s stopped after an error",
                agent.spec.kind,
                agent.spec.account_id,
            )

    async def run(self) -> None:
        self.source = self.source_factory()
        try:
            validate_api_configuration(self.settings, self._api_token)
            if self.risk_limits is None:
                suffixes = self.settings.pairs
                variables = ", ".join(
                    f"EXCHANGE_MAX_{kind}_{product.replace('-', '_')}"
                    for product in suffixes
                    for kind in ("QUANTITY", "NOTIONAL", "POSITION")
                )
                raise ValueError(f"live account risk limits are required; set {variables}")
            if set(self.risk_limits.max_quantity) != set(self.settings.pairs):
                raise ValueError("live account risk limits must cover all configured pairs")
            if self.settings.ledger_dsn is not None:
                self.ledger = SyncLedger(self.settings.ledger_dsn, metrics=self.ledger_metrics)
                self.ledger.migrate()
            self.runner = self.runner_factory(
                self.settings,
                self.source,
                self.metrics,
                ledger=self.ledger,
                risk_limits=self.risk_limits,
            )
            if self.ledger is not None:
                self.store = self.store_factory(self.settings.ledger_dsn)
                self.reconciliation = ReconciliationJob(
                    self.store,
                    self.ledger_metrics,
                    interval_seconds=self.settings.reconciliation_interval_seconds,
                )
                for account_id in self.settings.account_ids:
                    self.reconciliation.register_writer(account_id, self.runner.writer)
            self.tasks.append(asyncio.create_task(self.runner.run(), name="exchange-runner"))
            if self.reconciliation is not None:
                self.tasks.append(
                    asyncio.create_task(self.reconciliation.run_forever(), name="reconciliation")
                )
            if self.ledger is not None and (
                self.settings.mcp_port is not None or self.settings.api_port is not None
            ):
                self.tools = ExchangeTools(
                    self.settings.account_id,
                    self.runner.writer,
                    self.runner.engines,
                    self.risk_limits,
                )
                self.tools_by_account[self.settings.account_id] = self.tools
            if self.tools is not None and self.settings.mcp_port is not None:
                self.tasks.append(
                    asyncio.create_task(
                        self.server_runner(self.tools, port=self.settings.mcp_port),
                        name="mcp-server",
                    )
                )
            if self.tools is not None and self.settings.api_port is not None:
                self.tasks.append(
                    asyncio.create_task(
                        self.api_runner(
                            self.tools,
                            port=self.settings.api_port,
                            token=self._api_token,
                            heartbeat_seconds=self.settings.api_heartbeat_seconds,
                            send_timeout_seconds=self.settings.api_send_timeout_seconds,
                        ),
                        name="api-server",
                    )
                )
            self.agents = [
                BaselineAgent(
                    spec,
                    ExchangeTools(
                        spec.account_id,
                        self.runner.writer,
                        self.runner.engines,
                        self.risk_limits,
                    ),
                    cadence_seconds=self.settings.agent_cadence_seconds,
                    dca_quote_amount=self.settings.agent_dca_quote_amount,
                    metrics=self.metrics,
                )
                for spec in self.settings.baseline_agents
            ]
            for agent in self.agents:
                log.info(
                    "starting baseline agent %s for account %s with seed %d",
                    agent.spec.kind,
                    agent.spec.account_id,
                    agent.spec.seed,
                )
                self.tasks.append(
                    asyncio.create_task(
                        self._run_agent(agent), name=f"agent-{agent.spec.account_id}"
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
        try:
            if self.source is not None:
                await self.source.stop()
        finally:
            self.source = None
            try:
                if self.store is not None:
                    self.store.close()
                    self.store = None
            finally:
                if self.ledger is not None:
                    self.ledger.close()
                    self.ledger = None
