import asyncio
import os
import uuid
from decimal import Decimal

import asyncpg
import pytest

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    Liquidity,
)
from simex.exchange.orders import FillCause
from simex.ledger import SyncLedger
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
            RiskLimits({}, {}),
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
            RiskLimits({}, {}),
        ).history()
        assert restored.orders[order.order_id].request.reasoning == reasoning
        assert history["ok"]
        assert history["fills"][0]["reasoning"] == reasoning
        assert Decimal(history["fills"][0]["price"]) == Decimal("100")
    finally:
        if ledger is not None:
            ledger.close()
        if restarted is not None:
            restarted.close()
        asyncio.run(_drop_schema(dsn, schema))
