import asyncio
import os
import uuid
from decimal import Decimal

import asyncpg
import pytest
from test_mcp import make_tools

from simex.agents import BaselineAgent, BaselineSpec
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    Liquidity,
)
from simex.exchange.orders import FillCause
from simex.ledger import (
    LedgerMetrics,
    PostgresReconciliationStore,
    ReconciliationJob,
    SyncLedger,
)
from simex.mcp import ExchangeTools, RiskLimits


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, Decimal("100"), Decimal("101"))


def _dsn_or_skip() -> str:
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
    if not dsn:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            raise RuntimeError("SIMEX_TEST_POSTGRES_DSN is required in this test job")
        pytest.skip("set SIMEX_TEST_POSTGRES_DSN for ledger integration tests")
    return dsn


async def _create_schema(dsn: str, schema: str) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
    finally:
        await connection.close()


async def _drop_schema(dsn: str, schema: str) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await connection.close()


def _schema_dsn(dsn: str, schema: str) -> str:
    separator = "&" if "?" in dsn else "?"
    return f"{dsn}{separator}options=-csearch_path%3D{schema}"


def test_reasoning_and_history_survive_ledger_restart():
    dsn = _dsn_or_skip()
    schema = f"mcp_{uuid.uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    schema_dsn = _schema_dsn(dsn, schema)
    ledger = None
    restarted = None
    reasoning = "SQL '; DROP TABLE orders; %s {opaque}"
    try:
        ledger = SyncLedger(schema_dsn)
        ledger.migrate()
        writer = AccountWriter(
            DEFAULT_INSTRUMENTS,
            FeeSchedule(),
            OpenGate(),
            200_000_000,
            ledger=ledger,
        )
        writer.deposit("owner", "USD", Decimal("1000"), "seed:owner:USD:v1")
        tools = ExchangeTools(
            "owner",
            writer,
            {},
            RiskLimits({}, {}, {}),
        )
        order = writer.submit(
            "owner",
            {
                "client_order_id": "history-1",
                "product_id": "BTC-USD",
                "side": "buy",
                "type": "limit",
                "quantity": "0.1",
                "limit_price": "100",
                "reasoning": reasoning,
            },
            1_000,
        )
        writer.activate(order.order_id, 200_000_000)
        writer.record_fill(
            order.order_id,
            Decimal("100"),
            Decimal("0.1"),
            Liquidity.TAKER,
            FillCause.TAKER,
            200_000_001,
        )
        assert tools.history()["fills"][0]["reasoning"] == reasoning
        ledger.close()
        ledger = None

        restarted = SyncLedger(schema_dsn)
        restarted.migrate()
        restored = AccountWriter(
            DEFAULT_INSTRUMENTS,
            FeeSchedule(),
            OpenGate(),
            200_000_000,
            ledger=restarted,
        )
        restored.restore("owner")
        history = ExchangeTools(
            "owner",
            restored,
            {},
            RiskLimits({}, {}, {}),
        ).history()
        assert not restored.orders
        assert (
            restored.lookup_order("owner", order_id=order.order_id).request.reasoning == reasoning
        )
        assert history["ok"]
        assert history["fills"][0]["reasoning"] == reasoning
        assert Decimal(history["fills"][0]["price"]) == Decimal("100")
    finally:
        if ledger is not None:
            ledger.close()
        if restarted is not None:
            restarted.close()
        asyncio.run(_drop_schema(dsn, schema))


def test_multiple_accounts_reconcile_exactly():
    dsn = _dsn_or_skip()
    schema = f"mcp_multi_{uuid.uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    schema_dsn = _schema_dsn(dsn, schema)
    ledger = None
    store = None
    try:
        ledger = SyncLedger(schema_dsn)
        ledger.migrate()
        writer = AccountWriter(
            DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), 200_000_000, ledger=ledger
        )
        for account in ("buyhold", "dca", "random", "momentum"):
            writer.deposit(account, "USD", Decimal("1000"), f"seed:{account}:USD:v1")
        store = PostgresReconciliationStore(schema_dsn)
        job = ReconciliationJob(store, LedgerMetrics())
        for account in ("buyhold", "dca", "random", "momentum"):
            job.register_writer(account, writer)
        assert job.run_once() == ()
    finally:
        if store is not None:
            store.close()
        if ledger is not None:
            ledger.close()
        asyncio.run(_drop_schema(dsn, schema))


def test_buy_and_hold_does_not_rebuy_after_ledger_restart():
    dsn = _dsn_or_skip()
    schema = f"buyhold_{uuid.uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    schema_dsn = _schema_dsn(dsn, schema)
    ledger = None
    try:
        ledger = SyncLedger(schema_dsn)
        ledger.migrate()
        tools = make_tools()
        tools.writer.ledger = ledger
        tools.writer.deposit("owner", "USD", Decimal("1000"), "seed:owner:USD:v1")
        baseline = BaselineAgent(BaselineSpec("buy_and_hold", "owner", 7), tools)
        assert baseline.tick()["ok"]
        order = next(iter(tools.writer.orders.values()))
        tools.writer.activate(order.order_id, order.effective_at_ns)
        tools.writer.record_fill(
            order.order_id,
            Decimal("101"),
            order.request.quantity,
            Liquidity.TAKER,
            FillCause.TAKER,
            order.effective_at_ns,
        )
        ledger.close()
        ledger = None

        for _ in range(2):
            ledger = SyncLedger(schema_dsn)
            ledger.migrate()
            restored = make_tools()
            restored.writer.ledger = ledger
            restored.writer.restore("owner")
            assert Decimal(restored.portfolio()["positions"]["BTC"]) == order.request.quantity
            rebuilt = BaselineAgent(BaselineSpec("buy_and_hold", "owner", 7), restored)

            assert rebuilt.tick() == {"ok": True, "action": "none"}
            assert rebuilt.tick() == {"ok": True, "action": "none"}
            assert not restored.writer.orders
            original = restored.writer.lookup_order("owner", order_id=order.order_id)
            assert original.fill_count == 1
            assert original == order
            assert len(restored.history()["fills"]) == 1
            ledger.close()
            ledger = None
    finally:
        if ledger is not None:
            ledger.close()
        asyncio.run(_drop_schema(dsn, schema))
