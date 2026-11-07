"""Private measurement harness for load_test.py; never used by the trading runtime."""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import sys
import threading
import time
from collections import defaultdict
from decimal import Decimal
from urllib.parse import unquote, urlsplit

import asyncpg
from prometheus_client import start_http_server

from simex.exchange.risk import RiskLimits
from simex.exchange.runner import ExchangeRunner
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.exchange.runtime import LiveRuntimeGroup
from simex.exchange.source import KafkaMarketSource
from simex.ledger import LedgerMetrics


def validate_destination(dsn, account):
    """Reject before connecting, including URL options that could override the database."""
    try:
        url = urlsplit(dsn)
        name = unquote(url.path.removeprefix("/"))
    except ValueError:
        raise ValueError("supply a PostgreSQL URL for a dedicated simex_load_* database") from None
    if (
        url.scheme not in {"postgres", "postgresql"}
        or not re.fullmatch(r"simex_load_[A-Za-z0-9_]+", name)
        or url.query
        or url.fragment
    ):
        raise ValueError("--dsn must explicitly name simex_load_* without URL options")
    if not re.fullmatch(r"load_[A-Za-z0-9_]{1,48}", account):
        raise ValueError("--account must be a fresh load_* account")
    return name


async def connect(dsn):
    try:
        return await asyncpg.connect(dsn, timeout=5)
    except Exception:
        raise ValueError("cannot connect to the explicit load database; check --dsn") from None


async def check_empty_database(dsn, account):
    expected = validate_destination(dsn, account)
    connection = await connect(dsn)
    try:
        if await connection.fetchval("SELECT current_database()") != expected:
            raise ValueError("connected database differs from the explicit load destination")
        # Inspect every user-schema account reference, not just orders/public. Never migrate,
        # seed, reset or delete anything before this check succeeds.
        tables = await connection.fetch(
            "SELECT n.nspname, c.relname, a.attname IS NOT NULL FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "LEFT JOIN pg_attribute a ON a.attrelid=c.oid "
            "AND a.attname='account_id' AND NOT a.attisdropped "
            "WHERE c.relkind IN ('r','p') "
            "AND (a.attname IS NOT NULL OR c.relname IN ('ledger_events','outbox')) "
            "AND left(n.nspname, 3) <> 'pg_' AND n.nspname <> 'information_schema'"
        )
        for schema, table, has_account in tables:
            quoted = ".".join('"' + part.replace('"', '""') + '"' for part in (schema, table))
            if await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM {quoted})"):
                kind = "account" if has_account else "ledger"
                raise ValueError(f"load database contains {kind} history; use a fresh database")
    finally:
        await connection.close()


async def committed_orders(dsn, account, client_ids):
    validate_destination(dsn, account)
    connection = await connect(dsn)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            orders = await connection.fetch(
                "SELECT order_id, client_order_id, status, reject_reason, filled_quantity "
                "FROM orders WHERE account_id=$1 AND client_order_id=ANY($2::text[])",
                account,
                list(client_ids),
            )
            fills = await connection.fetch(
                "SELECT fill_id, order_id FROM fills WHERE account_id=$1 "
                "AND order_id=ANY($2::text[])",
                account,
                [row["order_id"] for row in orders],
            )
        return [dict(row) for row in orders], [dict(row) for row in fills]
    finally:
        await connection.close()


class Observations:
    def __init__(self):
        self.lock = threading.Lock()
        self.fills = []
        self.ledger = defaultdict(list)

    def snapshot(self):
        with self.lock:
            return {"fills": list(self.fills), "ledger": dict(self.ledger)}


class MeasuredLedgerMetrics(LedgerMetrics):
    def __init__(self, observations, **kwargs):
        super().__init__(**kwargs)
        self.observations = observations

    def observe(self, phase, elapsed_ns):
        super().observe(phase, elapsed_ns)
        with self.observations.lock:
            self.observations.ledger[phase].append((time.perf_counter_ns(), elapsed_ns))


class MeasuredRunner(ExchangeRunner):
    def __init__(self, *args, observations, **kwargs):
        super().__init__(*args, **kwargs)
        self.observations = observations
        self.causal_tick = None
        emit = self.writer._emit

        def observe(kind, **fields):
            emit(kind, **fields)  # Fill emission already follows the durable commit.
            if kind != "fill":
                return
            fill = fields["fill"]
            order = self.writer.orders[fill["order_id"]]
            maker = fill["liquidity"] == "maker"
            causal = maker and self.causal_tick == fill["ts_ns"]
            wall = time.time_ns()
            with observations.lock:
                observations.fills.append(
                    {
                        "liquidity": fill["liquidity"],
                        "cause": fill["cause"],
                        "fill_id": fill["fill_id"],
                        "order_id": order.order_id,
                        "client_order_id": order.request.client_order_id,
                        "observed_ns": time.perf_counter_ns(),
                        "submit_to_fill_ns": wall - order.created_at_ns,
                        "tick_to_fill_ns": wall - fill["ts_ns"] if causal else None,
                        "maker_without_causal_tick": maker and not causal,
                    }
                )

        self.writer._emit = observe

    def _apply(self, arrival, late):
        previous = self.causal_tick
        self.causal_tick = arrival.received_at_ns
        try:
            return super()._apply(arrival, late)
        finally:
            self.causal_tick = previous


@contextlib.asynccontextmanager
async def exclusive_destination(dsn, account):
    validate_destination(dsn, account)
    connection = await connect(dsn)
    try:
        if not await connection.fetchval(
            "SELECT pg_try_advisory_lock(hashtext('simex_load_runtime'))"
        ):
            raise ValueError("another load runtime already owns this database")
        await check_empty_database(dsn, account)
        yield
    finally:
        await connection.close()


@contextlib.asynccontextmanager
async def measured_runtime(config, source_factory=None):
    async with exclusive_destination(config["dsn"], config["account"]):
        async with _runtime(config, source_factory) as state:
            yield state


@contextlib.asynccontextmanager
async def _runtime(config, source_factory=None):
    await check_empty_database(config["dsn"], config["account"])
    product = config["product"]
    settings = RunnerSettings(
        pairs=(product,),
        ledger_dsn=config["dsn"],
        account_id=config["account"],
        api_port=config["api_port"],
        digest_path="",
        baseline_agents=(),
        kafka_bootstrap_servers=config["kafka"],
    )
    limits = RiskLimits(
        {product: Decimal(config["max_quantity"])},
        {product: Decimal(config["max_notional"])},
        {product: Decimal(config["max_position"])},
    )
    metrics, observations = RunnerMetrics(), Observations()
    ledger_metrics = MeasuredLedgerMetrics(observations, registry=metrics.registry)
    server, thread = start_http_server(0, addr="127.0.0.1", registry=metrics.registry)

    def runner(*args, **kwargs):
        return MeasuredRunner(*args, observations=observations, **kwargs)

    group = LiveRuntimeGroup(
        settings,
        metrics,
        source_factory or (lambda: KafkaMarketSource(config["kafka"], settings.topics)),
        ledger_metrics=ledger_metrics,
        risk_limits=limits,
        api_token=config["token"],
        runner_factory=runner,
    )
    task = asyncio.create_task(group.run())
    try:
        # Once these objects exist, startup has connected, checked and migrated the dedicated
        # database. API binding and book catch-up are checked by the parent through the SDK.
        while group.tools is None:
            if task.done():
                task.result()
            await asyncio.sleep(0.01)
        yield group, observations, server.server_port, task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


async def serve(config):
    async with measured_runtime(config) as (group, observations, port, task):
        engine = group.runner.engines[config["product"]]
        policy = {
            "maker_bps": str(group.runner.writer.fees.maker_bps),
            "taker_bps": str(group.runner.writer.fees.taker_bps),
            "order_delay_ms": group.settings.order_delay_ms,
            "holdback_ms": group.settings.holdback_ms,
            "match_window_ns": engine.settings.window_ns(config["product"]),
            "book_cross": engine.settings.book_cross.value,
            "cancel_attribution": engine.settings.cancel_attribution.value,
            "market_band_bps": str(
                group.runner.writer.instruments[config["product"]].market_band_bps
            ),
            "lag_baseline_minutes": engine.settings.lag_baseline_minutes,
            "initial_usd": str(group.settings.initial_usd),
            **{name: config[name] for name in ("max_quantity", "max_notional", "max_position")},
        }
        print(json.dumps({"ready": True, "metrics_port": port, "policy": policy}), flush=True)
        while True:
            line = asyncio.create_task(asyncio.to_thread(sys.stdin.readline))
            done, _ = await asyncio.wait((line, task), return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                task.result()
                raise RuntimeError("exchange runtime stopped")
            if not line.result() or json.loads(line.result())["op"] == "stop":
                break
            print(json.dumps(observations.snapshot()), flush=True)
    # Quiesce the writer and API before the parent resolves identities in SQL.
    print(json.dumps(observations.snapshot()), flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(serve(json.loads(sys.stdin.readline())))
    except Exception as exc:
        # Child errors must not echo a driver exception's DSN or market payload.
        print(json.dumps({"error": "load runtime failed", "type": type(exc).__name__}), flush=True)
        raise SystemExit(1) from None
