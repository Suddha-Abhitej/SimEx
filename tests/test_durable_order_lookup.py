"""Synthetic recovery, durable identity and planner guards for ADR 0027."""

import asyncio
import json
import re
import uuid
from decimal import Decimal as D

import pytest
from test_account_writer_ledger import DELAY, T0, MemoryLedger, make_writer, order_payload
from test_ledger import connection_or_skip
from test_mcp import ClosedGate, make_tools
from test_mcp_ledger import _create_schema, _drop_schema, _dsn_or_skip, _schema_dsn

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, Liquidity
from simex.exchange.orders import TERMINAL, FillCause, OrderError, OrderStatus
from simex.exchange.risk import RiskLimits
from simex.ledger import LedgerHalted, SyncLedger
from simex.ledger.adapter import LIVE_ORDER_PREDICATE, RECOVERY_ORDERS_SQL
from simex.ledger.migration_runner import MIGRATIONS
from simex.ledger.sync import STARTUP_TIMEOUT_SECONDS


@pytest.fixture
def ledger():
    base = _dsn_or_skip()
    schema = f"durable_lookup_{uuid.uuid4().hex}"
    asyncio.run(_create_schema(base, schema))
    connection = SyncLedger(_schema_dsn(base, schema))
    try:
        connection.migrate()
        yield connection
    finally:
        connection.close()
        asyncio.run(_drop_schema(base, schema))


@pytest.mark.parametrize("terminal", [*TERMINAL, "effect_rejected", "partial_cancelled"])
def test_terminal_identity_survives_retirement_restart_and_changed_policy(ledger, terminal):
    writer = make_writer(ledger)
    payload = {**order_payload(str(terminal)), "reasoning": " rationale\nverbatim "}
    if terminal == OrderStatus.EXPIRED:
        payload.update(type="market")
        payload.pop("limit_price")
    if terminal == OrderStatus.REJECTED:
        writer.gate = ClosedGate()
    order = writer.submit("alice", payload, T0)
    if terminal == OrderStatus.FILLED or terminal == "partial_cancelled":
        writer.activate(order.order_id, T0 + DELAY)
        writer.record_fill(
            order.order_id,
            D("100"),
            D("0.04") if terminal == "partial_cancelled" else D("0.1"),
            Liquidity.TAKER,
            FillCause.TAKER,
            T0 + DELAY,
        )
    if terminal == OrderStatus.CANCELLED or terminal == "partial_cancelled":
        writer.cancel_now(order.order_id, T0 + DELAY)
    if terminal == OrderStatus.EXPIRED:
        writer.expire(order.order_id, T0 + DELAY)
    if terminal == "effect_rejected":
        writer.reject_on_effect(order.order_id, "feed_lagging:excess_ns=3000000000", T0 + DELAY)
    assert order.status.terminal
    assert not writer.orders and not writer.by_key
    balances = writer.balance_totals("alice")
    assert writer.submit("alice", payload, T0 + 999) == order
    # New connection and writer, as with a process restart or runtime rebuild.
    restarted_ledger = SyncLedger(ledger._dsn)
    try:
        restored = AccountWriter(
            DEFAULT_INSTRUMENTS,
            FeeSchedule(),
            ClosedGate(),
            DELAY,
            ledger=restarted_ledger,
            risk_limits=RiskLimits(
                {"BTC-USD": D("0.00001")}, {"BTC-USD": D("1")}, {"BTC-USD": D("0.00001")}
            ),
        )
        restored.restore("alice")
        assert not restored.orders and not restored.by_key and not restored.band_prices
        assert restored.balance_totals("alice") == balances
        assert not restored.accounts.reservations
        assert restored.submit("alice", {**payload, "quantity": "0.1000"}, T0) == order
        for field, different in (("reasoning", "different rationale"), ("quantity", "0.2")):
            with pytest.raises(OrderError, match="idempotency_key_reused"):
                restored.submit("alice", {**payload, field: different}, T0)
        assert restored.request_cancel("alice", T0, order_id=order.order_id) == order
        assert restored.lookup_order("another", order_id=order.order_id) is None
        assert not restored.orders and not restored.by_key
        assert restored.balance_totals("alice") == balances
    finally:
        restarted_ledger.close()


@pytest.mark.parametrize("kind", ["market", "limit"])
def test_tools_share_one_lookup_and_replay_market_without_a_band(ledger, monkeypatch, kind):
    tools = make_tools()
    tools.writer.ledger = ledger
    tools.writer.deposit("owner", "USD", D("1000"), "seed:owner")
    calls = []
    load = ledger.load_order

    def counted(*args, **kwargs):
        calls.append(1)
        return load(*args, **kwargs)

    monkeypatch.setattr(ledger, "load_order", counted)
    payload = {**order_payload("one"), "type": kind, "reasoning": "synthetic"}
    if kind == "market":
        payload.pop("limit_price")
    result = tools.place_order(payload)
    assert result["ok"] and len(calls) == 1
    order = tools.writer.orders[result["order"]["order_id"]]
    tools.writer.cancel_now(order.order_id, order.effective_at_ns)
    tools.writer.gate = ClosedGate()
    assert tools.place_order(payload)["order"]["status"] == "cancelled"
    assert len(calls) == 2
    assert tools.cancel(client_order_id="one")["reason"] == "order_terminal"
    assert len(calls) == 3
    assert not tools.writer.orders and not tools.writer.by_key


def test_resolution_cannot_be_reused_as_a_negative_cache(ledger):
    writer = make_writer(ledger)
    payload = order_payload()
    with writer.submission_lookup("alice", "one"):
        original = writer.submit("alice", payload, T0)
        writer.cancel_now(original.order_id, T0 + DELAY)
        with pytest.raises(OrderError, match="idempotency_key_reused"):
            writer.submit("alice", {**payload, "quantity": "0.2"}, T0)
    assert writer._submission_resolution is None


def test_lookup_failure_is_not_absence_and_does_not_mutate_admission(ledger, monkeypatch):
    writer = make_writer(ledger)
    before = writer.balance_snapshot("alice")

    def timeout(*args, **kwargs):
        raise TimeoutError("synthetic timeout")

    monkeypatch.setattr(ledger, "_call", timeout)
    with pytest.raises(LedgerHalted) as error:
        writer.submit("alice", order_payload(), T0)
    assert isinstance(error.value.__cause__, TimeoutError)
    assert not writer.orders and not writer.by_key and not writer.accounts.reservations
    assert writer.balance_snapshot("alice") == before


def test_terminal_maps_stay_empty_over_long_history_and_failed_persistence_does_not_evict():
    ledger = MemoryLedger()
    writer = make_writer(ledger)
    writer.gate = ClosedGate()
    for index in range(512):
        writer.submit("alice", {**order_payload(str(index)), "reasoning": "x" * 8192}, T0)
        assert not writer.orders and not writer.by_key
    assert len(ledger.orders) == 512
    assert writer.submit(
        "alice", {**order_payload("0"), "reasoning": "x" * 8192}, T0
    ).status.terminal

    def fail(*args):
        raise RuntimeError("synthetic persistence failure")

    ledger.save_order = fail
    with pytest.raises(RuntimeError, match="persistence failure"):
        writer.submit("alice", order_payload("failed"), T0)
    assert writer.halted and len(writer.orders) == len(writer.by_key) == 1


def test_nondurable_history_is_not_evicted():
    writer = make_writer()
    writer.gate = ClosedGate()
    original = writer.submit("alice", order_payload(), T0)
    assert writer.orders[original.order_id] is original
    assert writer.submit("alice", order_payload(), T0) is original
    with pytest.raises(OrderError, match="idempotency_key_reused"):
        writer.submit("alice", {**order_payload(), "quantity": "0.2"}, T0)


def test_restore_has_a_separate_budget_without_changing_lookup_or_fill_budgets(monkeypatch):
    ledger = SyncLedger.__new__(SyncLedger)
    ledger.restore_timeout_seconds = STARTUP_TIMEOUT_SECONDS
    timeouts = []

    def call(operation, *, timeout=None):
        timeouts.append(timeout)

    monkeypatch.setattr(ledger, "_call", call)
    ledger.load_account("alice")
    ledger.load_order("alice", client_order_id="one")
    assert timeouts == [5, None]


def test_recovery_sequence_is_maximum_across_assets(ledger):
    writer = make_writer(ledger)
    restored = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), ClosedGate(), DELAY, ledger=ledger)
    restored.restore("alice")
    assert restored.balance_snapshot("alice") == writer.balance_snapshot("alice")
    assert restored.applied_sequences["alice"] == 2


@pytest.mark.parametrize("prebuilt", [False, True])
def test_previous_schema_upgrade_and_owner_prebuild_are_compatible(ledger, prebuilt):
    writer = make_writer(ledger)
    order = writer.submit("alice", order_payload(), T0)

    async def prepare(adapter):
        await adapter.connection.execute("DROP INDEX orders_live_account_idx")
        await adapter.connection.execute("DELETE FROM schema_migrations WHERE version=9")
        if prebuilt:
            await adapter.connection.execute((MIGRATIONS / "009_live_orders_index.sql").read_text())

    ledger._call(prepare)
    ledger.migrate()
    restored = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), ClosedGate(), DELAY, ledger=ledger)
    restored.restore("alice")
    assert restored.orders[order.order_id] == order
    assert restored.balance_snapshot("alice") == writer.balance_snapshot("alice")
    assert (
        ledger._call(
            lambda adapter: adapter.connection.fetchval(
                "SELECT count(*) FROM schema_migrations WHERE version=9"
            )
        )
        == 1
    )


def test_partial_index_predicate_matches_python_terminal_states():
    sql = (MIGRATIONS / "009_live_orders_index.sql").read_text()
    predicate = sql.split("WHERE ", 1)[1].strip().rstrip(";")
    assert predicate == LIVE_ORDER_PREDICATE
    assert set(re.findall(r"'([^']+)'", predicate)) == {s.value for s in TERMINAL}
    assert {s for s in OrderStatus if s.terminal} == TERMINAL
    assert LIVE_ORDER_PREDICATE in RECOVERY_ORDERS_SQL


def test_recovery_planner_uses_partial_index_with_large_terminal_history():
    async def scenario():
        connection = await connection_or_skip()
        try:
            await connection.execute(
                "INSERT INTO orders (order_id,account_id,client_order_id,request,status,"
                "created_at_ns,effective_at_ns,reasoning) "
                "SELECT 'o'||i,'alice','k'||i,'{}'::jsonb,"
                "CASE WHEN i <= 16 THEN 'pending' ELSE 'filled' END,0,0,repeat('x',128) "
                "FROM generate_series(1,25000) i"
            )
            await connection.execute("ANALYZE orders")
            raw = await connection.fetchval("EXPLAIN (FORMAT JSON) " + RECOVERY_ORDERS_SQL, "alice")
            plan = json.loads(raw) if isinstance(raw, str) else raw

            def nodes(node):
                yield node
                for child in node.get("Plans", []):
                    yield from nodes(child)

            all_nodes = list(nodes(plan[0]["Plan"]))
            assert any(n.get("Index Name") == "orders_live_account_idx" for n in all_nodes), plan
            assert not any(n["Node Type"] == "Seq Scan" for n in all_nodes), plan
        finally:
            await connection.close()

    asyncio.run(scenario())
