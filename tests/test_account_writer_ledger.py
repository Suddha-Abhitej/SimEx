import copy
import os
import threading
import time
import uuid
from collections import defaultdict
from decimal import Decimal as D

import pytest

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    Liquidity,
    OrderStatus,
)
from simex.exchange.engine import EngineSettings, PairEngine
from simex.exchange.orders import FillCause
from simex.ledger import AccountState, SyncLedger, fill_postings

P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
DELAY = 200_000_000


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, D("100"), D("100.01"))


class MemoryLedger:
    def __init__(self):
        self.orders = {}
        self.reservations = {}
        self.balances = defaultdict(lambda: D(0))

    def save_order(self, order, reservation, band_price=None):
        self.orders[order.order_id] = copy.deepcopy(order)
        if reservation is None:
            self.reservations.pop(order.order_id, None)
        else:
            self.reservations[order.order_id] = (
                copy.deepcopy(reservation),
                order.order_id,
            )

    def record_deposit(self, account_id, asset, amount, event_key):
        self.balances[(account_id, asset)] += amount

    def record_fill(self, fill, instrument, order, reservation, band_price=None):
        for posting in fill_postings(fill, instrument):
            self.balances[(posting.account_id, posting.asset)] += posting.amount
        self.save_order(order, reservation, band_price)

    def load_account(self, account_id):
        orders = []
        for order in self.orders.values():
            if order.account_id != account_id:
                continue
            row = order.to_dict()
            row["quantity"] = row.pop("quantity")
            row["limit_price"] = row["limit_price"]
            row["market_band_price"] = None
            row["filled_notional"] = order.filled_notional
            row["maker_notional"] = order.maker_notional
            row["taker_notional"] = order.taker_notional
            row["fill_count"] = order.fill_count
            orders.append(row)
        reservations = [
            {
                "order_id": order_id,
                "account_id": reservation.account_id,
                "asset": reservation.asset,
                "remaining": reservation.remaining,
            }
            for reservation, order_id in self.reservations.values()
            if reservation.account_id == account_id
        ]
        balances = [
            {"account_id": account_id, "asset": asset, "total": total}
            for (owner, asset), total in self.balances.items()
            if owner == account_id
        ]
        return AccountState(tuple(orders), tuple(reservations), tuple(balances))


class SequencedMemoryLedger(MemoryLedger):
    def __init__(self):
        super().__init__()
        self.sequence = 0
        self.history = {}

    def _save_sequence(self, account_id):
        self.sequence += 1
        self.history[self.sequence] = {
            asset: amount for (owner, asset), amount in self.balances.items() if owner == account_id
        }
        return self.sequence

    def record_deposit(self, account_id, asset, amount, event_key):
        super().record_deposit(account_id, asset, amount, event_key)
        return self._save_sequence(account_id)

    def record_fill(self, fill, instrument, order, reservation, band_price=None):
        super().record_fill(fill, instrument, order, reservation, band_price)
        time.sleep(0.005)
        return self._save_sequence(fill.account_id)

    def fold(self, account_id, marker):
        eligible = [sequence for sequence in self.history if sequence <= marker]
        return self.history[max(eligible)] if eligible else {}


def make_writer(ledger=None, account="alice"):
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    writer.deposit(account, "USD", D("1000"), f"deposit:{account}:usd")
    writer.deposit(account, "BTC", D("10"), f"deposit:{account}:btc")
    return writer


def order_payload(client_order_id="one"):
    return {
        "client_order_id": client_order_id,
        "product_id": P,
        "side": "buy",
        "type": "limit",
        "quantity": "0.1",
        "limit_price": "100.00",
    }


def test_memory_restart_restores_order_reservation_and_idempotency():
    ledger = MemoryLedger()
    writer = make_writer(ledger)
    order = writer.submit("alice", order_payload(), T0)
    writer.activate(order.order_id, T0 + DELAY)
    writer.record_fill(
        order.order_id, D("100"), D("0.04"), Liquidity.MAKER, FillCause.PRINT, T0 + DELAY
    )

    restarted = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    restarted.restore("alice")
    same = restarted.submit("alice", {**order_payload(), "quantity": "0.100"}, T0 + 1)

    assert same.order_id == order.order_id
    assert same.status is OrderStatus.PARTIALLY_FILLED
    assert restarted.accounts.reservations[order.order_id].remaining == D("6.096")
    assert restarted.accounts.balance("alice", "USD").reserved == D("6.096")


def test_persistent_ledger_failure_halts_writer_without_emitting_fill():
    class FailingLedger(MemoryLedger):
        def record_fill(self, *args, **kwargs):
            raise RuntimeError("ledger unavailable")

    writer = make_writer(FailingLedger())
    order = writer.submit("alice", order_payload(), T0)
    writer.activate(order.order_id, T0 + DELAY)
    with pytest.raises(RuntimeError, match="ledger unavailable"):
        writer.record_fill(
            order.order_id, D("100"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0 + DELAY
        )
    assert writer.halted
    assert not [event for event in writer.events if event["type"] == "fill"]
    with pytest.raises(RuntimeError, match="halted"):
        writer.submit("alice", order_payload("after-halt"), T0)


def test_reconciliation_snapshots_stay_coherent_while_fills_commit():
    ledger = SequencedMemoryLedger()
    writer = make_writer(ledger)
    order = writer.submit("alice", {**order_payload(), "quantity": "0.100"}, T0)
    writer.activate(order.order_id, T0 + DELAY)
    mismatches = []
    stop = threading.Event()

    def reconcile():
        while not stop.is_set():
            projection, marker = writer.balance_snapshot("alice")
            if projection != ledger.fold("alice", marker):
                mismatches.append((projection, marker))

    thread = threading.Thread(target=reconcile)
    thread.start()
    try:
        for index in range(5):
            writer.record_fill(
                order.order_id,
                D("100"),
                D("0.001"),
                Liquidity.TAKER,
                FillCause.TAKER,
                T0 + DELAY + index,
            )
    finally:
        stop.set()
        thread.join()

    assert mismatches == []


def test_cancel_and_expiry_release_durable_reservations_once():
    ledger = MemoryLedger()
    writer = make_writer(ledger)
    cancelled = writer.submit("alice", order_payload("cancel"), T0)
    writer.request_cancel("alice", T0 + 1, order_id=cancelled.order_id)

    restarted = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    restarted.restore("alice")
    restarted.cancel_now(cancelled.order_id, T0 + DELAY)
    assert cancelled.order_id not in restarted.accounts.reservations

    expired = restarted.submit(
        "alice",
        {
            "client_order_id": "market",
            "product_id": P,
            "side": "buy",
            "type": "market",
            "quantity": "0.1",
        },
        T0,
    )
    restarted.expire(expired.order_id, T0 + DELAY)
    assert expired.order_id not in restarted.accounts.reservations
    recovered = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    recovered.restore("alice")
    assert recovered.accounts.reservations == {}


def test_restart_resting_order_is_not_ahead_of_uninterrupted_queue():
    ledger = MemoryLedger()
    writer = make_writer(ledger)
    order = writer.submit("alice", order_payload(), T0)
    engine = PairEngine(P, writer, EngineSettings())
    engine.book.apply(_book_snapshot(T0, "2"))
    engine.submit(order)
    engine.advance(T0 + DELAY)
    assert engine.resting[order.order_id].ahead == D("2")

    restarted = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    restarted.restore("alice")
    recovered_engine = PairEngine(P, restarted, EngineSettings())
    recovered_engine.restore_orders(restarted.open_orders("alice"))
    recovered_engine.on_event(_book_snapshot(T0, "2"))

    assert recovered_engine.resting[order.order_id].ahead == D("2")
    uninterrupted_trade = _trade(T0 + 1, "1")
    recovered_trade = _trade(T0 + 1, "1")
    engine.on_event(uninterrupted_trade)
    recovered_engine.on_event(recovered_trade)
    assert not [event for event in writer.events if event["type"] == "fill"]
    assert not [event for event in restarted.events if event["type"] == "fill"]

    engine.on_event(_trade(T0 + 2, "2"))
    recovered_engine.on_event(_trade(T0 + 2, "2"))
    assert [event["fill"]["ts_ns"] for event in writer.events if event["type"] == "fill"] == [
        T0 + 2
    ]
    assert [event["fill"]["ts_ns"] for event in restarted.events if event["type"] == "fill"] == [
        T0 + 2
    ]


def _book_snapshot(ts, bid_size):
    from simex.marketdata import BookUpdate

    return BookUpdate(
        "gemini",
        P,
        True,
        ((D("100"), D(bid_size)),),
        ((D("100.01"), D("1")),),
        (1, 1),
        False,
        ts,
        ts,
    )


def _trade(ts, size):
    from simex.marketdata import Trade

    return Trade("gemini", P, str(ts), D("100"), D(size), "sell", ts, ts)


def test_postgres_restart_restores_state():
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set SIMEX_TEST_POSTGRES_DSN for ledger integration tests")
    ledger = SyncLedger(dsn)
    account = f"restart-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    try:
        writer = make_writer(ledger, account)
        order = writer.submit(account, {**order_payload(), "client_order_id": "pg"}, T0)
        restarted = AccountWriter(
            DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger
        )
        restarted.restore(account)
        assert restarted.orders[order.order_id].status is OrderStatus.PENDING
        assert restarted.accounts.balance(account, "USD").reserved == D("10.12")
    finally:
        ledger.close()
