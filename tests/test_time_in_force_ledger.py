"""Immediate execution is terminal and atomic in a disposable synthetic ledger schema."""

import asyncio
import json
from contextlib import closing
from decimal import Decimal as D

import asyncpg
import pytest
from test_account_writer_ledger import DELAY, OpenGate, make_writer
from test_exchange_engine import SNAP_ASKS, SNAP_BIDS, T0, P, book
from test_portfolio_pnl import pnl_dsn as pnl_dsn
from test_time_in_force import payload

from simex.client.codec import order as decoded_order
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    OrderError,
    OrderStatus,
    PairEngine,
)
from simex.ledger import LedgerHalted, SyncLedger
from simex.ledger.migration_runner import MIGRATIONS, apply_migrations


@pytest.fixture
def exchange(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        writer = make_writer(ledger)
        engine = PairEngine(P, writer)
        engine.on_event(book(T0, SNAP_BIDS, SNAP_ASKS, snapshot=True))
        yield ledger, writer, engine


def query(ledger, sql, *args):
    return ledger._call(lambda adapter: adapter.connection.fetch(sql, *args))


def restored(ledger):
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    writer.restore("alice")
    return writer


@pytest.mark.parametrize(
    "tif,quantity,status,count",
    [
        ("IOC", "4", "cancelled", 2),
        ("IOC", "0.5", "filled", 1),
        ("IOC", "4", "cancelled", 0),
        ("FOK", "3", "filled", 2),
        ("FOK", "4", "rejected", 0),
    ],
)
def test_terminal_snapshot_exact_reservation_release_and_historical_identity(
    exchange, tif, quantity, status, count
):
    ledger, writer, engine = exchange
    request = payload(
        tif, quantity=quantity, price="99" if tif == "IOC" and count == 0 else "100.02"
    )
    order = writer.submit("alice", request, T0)
    engine.submit(order)
    engine.advance(T0 + DELAY)
    row = query(ledger, "SELECT status,time_in_force,request,filled_quantity,fees FROM orders")[0]
    assert row["time_in_force"] == tif and json.loads(row["request"])["time_in_force"] == tif
    assert row["status"] == status and row["filled_quantity"] == order.filled_quantity
    assert row["fees"] == order.fees
    assert query(ledger, "SELECT count(*) AS n FROM fills")[0]["n"] == count
    assert (
        query(ledger, "SELECT count(*) AS n FROM ledger_postings WHERE fill_id IS NOT NULL")[0]["n"]
        == count * 5
    )
    assert not query(ledger, "SELECT * FROM account_reservations")
    assert not writer.orders and not writer.by_key and not engine.resting
    restart = restored(ledger)
    assert not restart.orders and not restart.by_key
    assert restart.balance_totals("alice") == writer.balance_totals("alice")
    assert restart.accounts.balance("alice", "USD").reserved == 0
    replay = restart.submit("alice", request, T0 + DELAY + 1)
    assert decoded_order(replay.to_dict()) == decoded_order(order.to_dict())
    with pytest.raises(OrderError, match="idempotency_key_reused"):
        restart.submit("alice", request | {"time_in_force": "GTC"}, T0 + DELAY + 2)
    assert not restart.orders and not restart.accounts.reservations


@pytest.mark.parametrize("tif", ["IOC", "FOK"])
@pytest.mark.parametrize("kind", ["limit", "market"])
def test_pending_recovers_original_effective_time_full_reservation_and_band(exchange, tif, kind):
    ledger, writer, _ = exchange
    request = payload(tif, kind=kind, quantity="3")
    order = writer.submit("alice", request, T0)
    reserved = writer.accounts.reservations[order.order_id].remaining
    restart = restored(ledger)
    recovered = restart.orders[order.order_id]
    assert recovered.status is OrderStatus.PENDING
    assert recovered.effective_at_ns == T0 + DELAY
    assert recovered.request == order.request
    assert restart.accounts.reservations[order.order_id].remaining == reserved
    assert restart.band_prices == writer.band_prices
    engine = PairEngine(P, restart)
    engine.restore_orders(list(restart.orders.values()))
    engine.on_event(book(T0, SNAP_BIDS, SNAP_ASKS, snapshot=True))
    engine.advance(T0 + DELAY - 1)
    assert recovered.filled_quantity == 0
    engine.advance(T0 + DELAY)
    assert recovered.status is OrderStatus.FILLED
    assert not engine.resting and not restart.orders and not restart.accounts.reservations
    assert query(ledger, "SELECT market_band_price FROM orders")[0][
        "market_band_price"
    ] == writer.band_prices.get(order.order_id)


@pytest.mark.parametrize("tif", ["IOC", "FOK"])
@pytest.mark.parametrize("boundary", ["after_fill", "before_execution_commit"])
def test_group_failure_rolls_back_every_fill_and_terminalization(exchange, tif, boundary):
    ledger, writer, engine = exchange
    order = writer.submit("alice", payload(tif, quantity="3"), T0)
    before = writer.balance_totals("alice")
    reserved = writer.accounts.reservations[order.order_id].remaining
    calls = 0

    def fail(phase):
        nonlocal calls
        if phase == boundary:
            calls += 1
            if boundary != "after_fill" or calls % 2 == 0:
                raise RuntimeError("synthetic interrupted group")

    ledger._adapter.failure_hook = fail
    engine.submit(order)
    with pytest.raises(LedgerHalted):
        engine.advance(T0 + DELAY)
    assert writer.halted and order.status is OrderStatus.PENDING and order.filled_quantity == 0
    assert writer.balance_totals("alice") == before
    assert writer.accounts.reservations[order.order_id].remaining == reserved
    assert not engine.depletion and not [e for e in writer.events if e["type"] == "fill"]
    for table in ("fills", "outbox"):
        assert query(ledger, f"SELECT count(*) AS n FROM {table}")[0]["n"] == 0
    assert (
        query(ledger, "SELECT count(*) AS n FROM ledger_postings WHERE fill_id IS NOT NULL")[0]["n"]
        == 0
    )
    assert query(ledger, "SELECT status FROM orders")[0]["status"] == "pending"
    assert query(ledger, "SELECT remaining FROM account_reservations")[0]["remaining"] == reserved


@pytest.mark.parametrize("tif,quantity,status", [("FOK", "3", "filled"), ("IOC", "4", "cancelled")])
def test_lost_group_commit_ack_retries_same_fills_without_double_application(
    exchange, tif, quantity, status
):
    ledger, writer, engine = exchange
    order = writer.submit("alice", payload(tif, quantity=quantity), T0)
    lost = False

    def lose(phase):
        nonlocal lost
        if phase == "after_commit" and not lost:
            lost = True
            # Commit includes final status and released reservation before any notification.
            assert order.status is OrderStatus.PENDING
            assert not [event for event in writer.events if event["type"] == "fill"]
            raise RuntimeError("synthetic lost acknowledgement")

    ledger._adapter.failure_hook = lose
    engine.submit(order)
    engine.advance(T0 + DELAY)
    assert lost and order.status.value == status
    assert query(ledger, "SELECT count(*) AS n FROM fills")[0]["n"] == 2
    assert (
        query(ledger, "SELECT count(*) AS n FROM ledger_postings WHERE fill_id IS NOT NULL")[0]["n"]
        == 10
    )
    assert query(ledger, "SELECT count(*) AS n FROM outbox")[0]["n"] == 2
    assert (
        query(ledger, "SELECT count(*) AS n FROM ledger_events WHERE event_type='fill'")[0]["n"]
        == 2
    )
    assert not query(ledger, "SELECT * FROM account_reservations")
    assert writer.accounts.balance("alice", "USD").available == D("696.3494")
    assert writer.accounts.balance("alice", "BTC").available == D("13")
    assert len([event for event in writer.events if event["type"] == "fill"]) == 2
    assert restored(ledger).balance_totals("alice") == writer.balance_totals("alice")


def test_migration_defaults_legacy_requests_without_rewriting_audit_json(pnl_dsn):
    async def scenario():
        connection = await asyncpg.connect(pnl_dsn)
        try:
            for path in sorted(MIGRATIONS.glob("*.sql")):
                if int(path.name[:3]) >= 10:
                    continue
                await connection.execute(path.read_text())
                await connection.execute(
                    "INSERT INTO schema_migrations(version) VALUES($1)", int(path.name[:3])
                )
            request = payload()
            request.pop("time_in_force")
            await connection.execute(
                "INSERT INTO orders(order_id,account_id,client_order_id,request,status,"
                "created_at_ns,effective_at_ns,reasoning) "
                "VALUES('legacy','alice','tif',$1::jsonb,'cancelled',$2,$3,$4)",
                json.dumps(request),
                T0,
                T0 + DELAY,
                request["reasoning"],
            )
            await apply_migrations(connection)
            row = await connection.fetchrow("SELECT request,time_in_force FROM orders")
            assert row["time_in_force"] == "GTC" and "time_in_force" not in json.loads(
                row["request"]
            )
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute("UPDATE orders SET time_in_force='DAY'")
        finally:
            await connection.close()

    asyncio.run(scenario())
    with closing(SyncLedger(pnl_dsn)) as ledger:
        restart = restored(ledger)
        legacy = payload()
        legacy.pop("time_in_force")
        order = restart.submit("alice", legacy, T0 + DELAY)
        assert order.order_id == "legacy" and order.request.time_in_force.value == "GTC"


@pytest.mark.parametrize("tif,quantity,status", [("IOC", "4", "cancelled"), ("FOK", "3", "filled")])
def test_sdk_stream_delivers_committed_group_fills_and_terminal_poll(
    exchange, tif, quantity, status
):
    from test_api import TOKEN, no_floats
    from test_client import receive, serving

    from simex.client import Client
    from simex.exchange.risk import RiskLimits
    from simex.exchange.tools import ExchangeTools

    ledger, writer, engine = exchange
    limits = RiskLimits({P: D("10")}, {P: D("5000")}, {P: D("1000")})
    writer.risk_limits = limits
    tools = ExchangeTools("alice", writer, {P: engine}, limits, clock_ns=lambda: T0)
    with serving(tools) as server, Client(server.url, TOKEN) as client, client.stream() as stream:
        assert next(stream)["type"] == "hello"
        assert not stream.hello["writer_halted"] and not stream.hello["account_halted"]
        request = payload(tif, quantity=quantity)
        assert client.place_order(**request)["order"]["status"] == "pending"
        server.call(engine.advance, T0 + DELAY)
        applied = []
        for _ in range(2):
            frame = receive(stream, "fill")
            assert query(ledger, "SELECT count(*) AS n FROM fills")[0]["n"] == 2
            applied.append(frame["fill"])
            stream.ack(frame)
            no_floats(frame)
        assert sum((fill["quantity"] for fill in applied), D(0)) == D("3")
        assert sum((fill["fee"] for fill in applied), D(0)) == D("3.6006")
        assert client.place_order(**request)["order"]["status"] == status
        assert client.open_orders()["orders"] == []
        assert client.portfolio()["balances"]["BTC"] == D("13")


@pytest.mark.parametrize("tif,status", [("IOC", "cancelled"), ("FOK", "rejected")])
def test_zero_fill_outcome_is_pollable_but_has_no_fill_stream_frame(exchange, tif, status):
    from test_api import TOKEN
    from test_client import serving

    from simex.client import APIError, Client
    from simex.exchange.risk import RiskLimits
    from simex.exchange.tools import ExchangeTools

    _, writer, engine = exchange
    limits = RiskLimits({P: D("10")}, {P: D("5000")}, {P: D("1000")})
    writer.risk_limits = limits
    tools = ExchangeTools("alice", writer, {P: engine}, limits, clock_ns=lambda: T0)
    with serving(tools) as server, Client(server.url, TOKEN) as client, client.stream() as stream:
        assert next(stream)["type"] == "hello"
        assert not stream.hello["writer_halted"] and not stream.hello["account_halted"]
        request = payload(tif, quantity="1", price="99")
        client.place_order(**request)
        server.call(engine.advance, T0 + DELAY)
        assert next(stream)["type"] == "heartbeat"  # No invented order/fill notification.
        if tif == "FOK":
            with pytest.raises(APIError) as error:
                client.place_order(**request)
            result = error.value.body
            assert error.value.status == 400 and error.value.reason == "fok_not_fillable"
        else:
            result = client.place_order(**request)
        assert result["order"]["status"] == status


def test_restart_on_a_new_connection_recovers_terminal_identity(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        writer = make_writer(ledger)
        engine = PairEngine(P, writer)
        engine.on_event(book(T0, SNAP_BIDS, SNAP_ASKS, snapshot=True))
        request = payload("FOK", quantity="3")
        order = writer.submit("alice", request, T0)
        engine.submit(order)
        engine.advance(T0 + DELAY)
    with closing(SyncLedger(pnl_dsn)) as ledger:
        restart = restored(ledger)
        assert not restart.orders and not restart.by_key
        same = restart.submit("alice", request, T0 + DELAY + 1)
        assert same.status is OrderStatus.FILLED
        assert same.order_id == order.order_id and same.request == order.request
        assert same.filled_quantity == D("3") and same.fees == D("3.6006")
        with pytest.raises(OrderError, match="idempotency_key_reused"):
            restart.submit("alice", request | {"time_in_force": "IOC"}, T0 + DELAY + 2)
