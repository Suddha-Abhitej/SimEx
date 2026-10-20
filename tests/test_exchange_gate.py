from dataclasses import replace
from decimal import Decimal as D

from simex.book import BookState
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    MarketStatus,
    OrderStatus,
    PairEngine,
    StaleDataGate,
)
from simex.marketdata import BookUpdate, Trade

P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
MS = 1_000_000


def status(**changes):
    values = dict(
        book_state=BookState.SYNCED,
        last_level2_received_ns=T0,
        lag_ns=50 * MS,
        excess_lag_ns=100 * MS,
        best_bid=D("100"),
        best_ask=D("101"),
        baseline_established=True,
    )
    values.update(changes)
    return MarketStatus(**values)


def test_each_guard_closing_rule_has_a_distinct_reason():
    cases = [
        (status(book_state=BookState.RESYNCING), T0, "book_not_synced"),
        (status(catching_up=True), T0, "catching_up"),
        (status(last_level2_received_ns=T0), T0 + 5001 * MS, "stale_level2"),
        (status(lag_ns=31 * 1000 * MS), T0, "feed_lagging"),
        (status(excess_lag_ns=2001 * MS), T0, "feed_lagging"),
    ]
    for snapshot, now, reason in cases:
        decision = StaleDataGate({"BTC-USD": snapshot}).check(P, now)
        assert not decision.open
        assert decision.reason.startswith(reason)


def test_lag_baseline_and_constant_clock_offset():
    gate = StaleDataGate({"BTC-USD": status(baseline_established=False, excess_lag_ns=10 * MS)})
    assert gate.check(P, T0).open
    gate = StaleDataGate({"BTC-USD": status(excess_lag_ns=2001 * MS)})
    assert gate.check(P, T0).reason.startswith("feed_lagging")


def _engine():
    engines = {}
    gate = StaleDataGate(engines)
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate, 200 * MS)
    writer.accounts.deposit("alice", "USD", D("100000"))
    writer.accounts.deposit("alice", "BTC", D("10"))
    engine = PairEngine(P, writer)
    engines[P] = engine
    return engine, writer


def _snapshot(t):
    return BookUpdate(
        "gemini",
        P,
        True,
        ((D("100"), D("2")),),
        ((D("101"), D("2")),),
        (1, 1),
        False,
        t - 50 * MS,
        t,
    )


def test_trades_silence_does_not_refresh_or_close_the_level2_gate():
    engine, writer = _engine()
    engine.on_event(_snapshot(T0))
    engine.on_event(Trade("gemini", P, "1", D("101"), D("1"), "buy", T0, T0 + 4 * MS))
    assert engine.status.last_level2_received_ns == T0
    assert writer.gate.check(P, T0 + 4 * MS).open

    engine.on_event(Trade("gemini", P, "2", D("101"), D("1"), "buy", T0, T0 + 6 * 1000 * MS))
    assert engine.stats.quantities["prints_ignored_stale"] == D("1")
    assert writer.gate.check(P, T0 + 6 * 1000 * MS).reason.startswith("stale_level2")


def test_engine_publishes_rolling_p99_level2_lag():
    engine, _ = _engine()
    for i, lag_ms in enumerate((100, 200, 300), 1):
        event = replace(_snapshot(T0 + i * MS), exchange_ts_ns=T0 + i * MS - lag_ms * MS)
        engine.on_event(event)
    assert engine.status.p99_lag_ns == 300 * MS


def test_cancels_apply_while_gate_is_closed():
    engine, writer = _engine()
    engine.on_event(_snapshot(T0))
    order = writer.submit(
        "alice",
        {
            "client_order_id": "rest",
            "product_id": P,
            "side": "buy",
            "type": "limit",
            "quantity": "0.1",
            "limit_price": "99.00",
        },
        T0,
    )
    engine.submit(order)
    engine.advance(T0 + 200 * MS)
    cancel = writer.request_cancel("alice", T0 + 6 * 1000 * MS, order_id=order.order_id)
    engine.advance(cancel.cancel_effective_at_ns)
    assert order.status is OrderStatus.CANCELLED


def test_cancel_of_pending_order_is_not_rejected_when_gate_closes():
    engine, writer = _engine()
    engine.on_event(_snapshot(T0))
    order = writer.submit(
        "alice",
        {
            "client_order_id": "pending",
            "product_id": P,
            "side": "buy",
            "type": "limit",
            "quantity": "0.1",
            "limit_price": "99.00",
        },
        T0,
    )
    engine.submit(order)
    cancel = writer.request_cancel("alice", T0, order_id=order.order_id)
    engine.advance(T0 + 6 * 1000 * MS)
    assert cancel.cancel_effective_at_ns is not None
    assert order.status is OrderStatus.CANCELLED
