"""Synthetic execution bounds survive terminal persistence, not runtime recovery."""

import asyncio
from contextlib import closing
from decimal import Decimal as D

import asyncpg
import pytest
from test_account_writer_ledger import (
    DELAY,
    T0,
    MemoryLedger,
    OpenGate,
    P,
    _book_snapshot,
    make_writer,
)
from test_mcp import ClosedGate
from test_portfolio_pnl import pnl_dsn as pnl_dsn

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, OrderStatus, PairEngine
from simex.ledger import SyncLedger


@pytest.fixture
def ledger(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        yield ledger


@pytest.fixture
def exchange(ledger):
    writer = make_writer(ledger)
    engine = PairEngine(P, writer)
    engine.book.apply(_book_snapshot(T0, "2"))
    return writer, engine


def request(key, *, side="buy", quantity="0.1", kind="market"):
    payload = {
        "client_order_id": key,
        "product_id": P,
        "side": side,
        "type": kind,
        "quantity": quantity,
        "reasoning": "Handwritten synthetic audit regression",
    }
    if kind == "limit":
        payload["limit_price"] = "100.01"
    return payload


def row(dsn, order_id):
    async def read():
        connection = await asyncpg.connect(dsn)
        try:
            return dict(
                await connection.fetchrow(
                    "SELECT status, market_band_price, filled_quantity, filled_notional, fees, "
                    "reject_details FROM orders WHERE order_id=$1",
                    order_id,
                )
            )
        finally:
            await connection.close()

    return asyncio.run(read())


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_filled_market_order_keeps_exact_band_in_sql_after_close(exchange, pnl_dsn, ledger, side):
    writer, engine = exchange
    order = writer.submit("alice", request("filled", side=side), T0)
    accepted_band = writer.band_prices[order.order_id]
    assert row(pnl_dsn, order.order_id)["market_band_price"] == accepted_band
    cursor = writer.event_sequence
    engine.submit(order)
    engine.advance(order.effective_at_ns - 1)
    assert order.status is OrderStatus.PENDING and order.fill_count == 0
    engine.advance(order.effective_at_ns)

    committed = row(pnl_dsn, order.order_id)
    assert committed["status"] == "filled"
    assert committed["market_band_price"] == accepted_band  # Read after the final close write.
    assert order.order_id not in writer.band_prices
    assert order.order_id not in writer.accounts.reservations
    assert committed["filled_quantity"] == D("0.1")
    price, fee = (D("100.01"), D("0.120012")) if side == "buy" else (D("100"), D("0.12"))
    assert committed["filled_notional"] == price * D("0.1")
    assert committed["fees"] == fee
    fill = ledger.history("alice")[0]
    assert fill["price"] == price and fill["fee"] == fee
    assert fill["liquidity"] == fill["cause"] == "taker"
    assert fill["ts_ns"] == T0 + DELAY
    assert [event["type"] for event in writer.events_after(cursor)[2]] == ["fill"]


def test_partially_filled_expired_market_retains_band(exchange, pnl_dsn):
    writer, engine = exchange
    order = writer.submit("alice", request("expires", quantity="1.5"), T0)
    accepted_band = writer.band_prices[order.order_id]
    engine.submit(order)
    engine.advance(order.effective_at_ns)

    committed = row(pnl_dsn, order.order_id)
    assert committed["status"] == "expired"
    assert committed["market_band_price"] == accepted_band
    assert committed["filled_quantity"] == D("1") and order.fill_count == 1
    assert committed["filled_notional"] == D("100.01")
    assert committed["fees"] == D("1.20012")
    assert writer.band_prices == {} and writer.accounts.reservations == {}


def test_rejection_at_effect_preserves_accepted_band_without_leaking_it(exchange, pnl_dsn):
    writer, engine = exchange
    order = writer.submit("alice", request("gate-closed-later"), T0)
    accepted_band = writer.band_prices[order.order_id]
    engine.submit(order)
    writer.gate = ClosedGate()
    engine.advance(order.effective_at_ns)

    committed = row(pnl_dsn, order.order_id)
    assert committed["status"] == "rejected"
    assert committed["market_band_price"] == accepted_band
    assert committed["filled_quantity"] == 0 and order.reject_reason == "stale_level2"
    assert writer.band_prices == {} and writer.accounts.reservations == {}
    assert not {"market_band_price", "band_price", "best_bid", "best_ask", "mid"} & (
        order.reject_details.keys() | order.to_dict().keys()
    )


def test_rejection_before_band_established_keeps_null(exchange, pnl_dsn):
    writer, _ = exchange
    writer.gate = ClosedGate()
    order = writer.submit("alice", request("gate-closed-already"), T0)

    committed = row(pnl_dsn, order.order_id)
    assert committed["status"] == "rejected"
    assert committed["market_band_price"] is None
    assert writer.band_prices == {} and writer.accounts.reservations == {}


def test_limit_order_keeps_null_band_after_fill(exchange, pnl_dsn):
    writer, engine = exchange
    order = writer.submit("alice", request("limit", kind="limit"), T0)
    engine.submit(order)
    engine.advance(order.effective_at_ns)

    committed = row(pnl_dsn, order.order_id)
    assert committed["status"] == "filled" and committed["market_band_price"] is None
    assert writer.band_prices == {} and writer.accounts.reservations == {}


def test_restore_loads_only_live_market_bands_and_pending_order_still_fills(
    exchange, pnl_dsn, ledger
):
    writer, engine = exchange
    completed = writer.submit("alice", request("completed"), T0)
    historical_band = writer.band_prices[completed.order_id]
    engine.submit(completed)
    engine.advance(completed.effective_at_ns)
    pending = writer.submit("alice", request("pending", quantity="0.2"), T0 + 1_000_000_000)
    pending_band = writer.band_prices[pending.order_id]
    limit = writer.submit("alice", request("limit-pending", kind="limit"), T0 + 1_000_000_000)
    expected_available = writer.accounts.balance("alice", "USD").available
    expected_reserved = writer.accounts.balance("alice", "USD").reserved

    restored = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    restored.restore("alice")
    assert restored.band_prices == {pending.order_id: pending_band}
    assert completed.order_id not in restored.orders
    assert restored.lookup_order("alice", order_id=completed.order_id).status is OrderStatus.FILLED
    assert row(pnl_dsn, completed.order_id)["market_band_price"] == historical_band
    assert row(pnl_dsn, limit.order_id)["market_band_price"] is None
    assert restored.accounts.balance("alice", "USD").available == expected_available
    assert restored.accounts.balance("alice", "USD").reserved == expected_reserved
    assert restored.accounts.reservations[pending.order_id].remaining == D("20.345248")

    recovered_engine = PairEngine(P, restored)
    recovered_engine.book.apply(_book_snapshot(T0 + 1_000_000_000, "2"))
    recovered_engine.restore_orders(restored.open_orders("alice"))
    recovered_engine.advance(pending.effective_at_ns)
    assert pending.order_id not in restored.orders
    assert restored.lookup_order("alice", order_id=pending.order_id).status is OrderStatus.FILLED
    assert restored.band_prices == {}
    assert pending.order_id not in restored.accounts.reservations
    assert row(pnl_dsn, pending.order_id)["market_band_price"] == pending_band


def test_close_persistence_failure_still_halts_and_raises():
    class FailingClose(MemoryLedger):
        failures = 0

        def save_order(self, order, reservation, band_price=None):
            if order.status.terminal:
                self.failures += 1
                raise RuntimeError("synthetic terminal persistence failure")
            super().save_order(order, reservation, band_price)

    ledger = FailingClose()
    writer = make_writer(ledger)
    order = writer.submit("alice", request("fail-close"), T0)
    cursor = writer.event_sequence
    with pytest.raises(RuntimeError, match="synthetic terminal persistence failure"):
        writer.expire(order.order_id, order.effective_at_ns)
    assert writer.halted and ledger.failures == 1
    assert [event["type"] for event in writer.events_after(cursor)[2]] == ["halt"]
