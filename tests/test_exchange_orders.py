"""The order model and account writer (#7, ADR 0014): unit tests, synthetic values only."""

import json
from decimal import Decimal as D

import pytest

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    EXTERNAL,
    FEES,
    MARKET,
    AccountWriter,
    FeeSchedule,
    FillCause,
    GateDecision,
    InvalidTransition,
    Liquidity,
    OrderError,
    OrderRequest,
    OrderStatus,
    instruments_from_env,
)
from simex.exchange.writer import band_price

T0 = 1_792_036_800_000_000_000
DELAY = 200_000_000


class Gate:
    """A stand-in for the stale-data guard (#9): open or closed, with best prices."""

    def __init__(self, open=True, reason=None, bid="100.00", ask="100.50"):
        self.decision = GateDecision(open, reason, D(bid) if bid else None, D(ask) if ask else None)
        self.calls = []

    def check(self, product_id, now_ns):
        self.calls.append((product_id, now_ns))
        return self.decision


def writer(gate=None, usd="100000", btc="10", fees=None):
    w = AccountWriter(DEFAULT_INSTRUMENTS, fees or FeeSchedule(), gate or Gate(), DELAY)
    w.accounts.deposit("alice", "USD", D(usd))
    w.accounts.deposit("alice", "BTC", D(btc))
    return w


def limit(cid="c1", side="buy", qty="0.5", price="100.00", product="BTC-USD"):
    return {
        "client_order_id": cid,
        "product_id": product,
        "side": side,
        "type": "limit",
        "quantity": qty,
        "limit_price": price,
    }


def market(cid="m1", side="buy", qty="0.5", product="BTC-USD"):
    return {
        "client_order_id": cid,
        "product_id": product,
        "side": side,
        "type": "market",
        "quantity": qty,
    }


# Parsing: decimal strings only ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"quantity": 0.5}, "bad_decimal"),  # a JSON number has been through a float
        ({"limit_price": 100}, "bad_decimal"),
        ({"quantity": "NaN"}, "bad_decimal"),
        ({"quantity": "Infinity"}, "bad_decimal"),
        ({"quantity": "half"}, "bad_decimal"),
        ({"client_order_id": ""}, "bad_client_order_id"),
        ({"client_order_id": "x" * 65}, "bad_client_order_id"),
        ({"client_order_id": "has space"}, "bad_client_order_id"),
        ({"side": "hold"}, "bad_field"),
        ({"type": "stop"}, "bad_field"),
        ({"product_id": 7}, "unknown_product"),
    ],
)
def test_malformed_requests_are_refused_without_a_record(change, reason):
    w = writer()
    with pytest.raises(OrderError) as raised:
        w.submit("alice", {**limit(), **change}, T0)
    assert raised.value.reason == reason
    assert w.orders == {} and w.events == []


def test_a_market_order_cannot_carry_a_price():
    with pytest.raises(OrderError, match="no limit_price"):
        OrderRequest.parse({**market(), "limit_price": "100.00"})


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (limit(qty="0.000000001"), "bad_quantity"),  # finer than BTC's 1e-8 increment
        (limit(qty="-1"), "bad_quantity"),
        (limit(qty="0.000009"), "below_min_size"),
        (limit(price="100.005"), "bad_price"),  # off the 0.01 tick: rejected, not rounded
        (limit(price="0"), "bad_price"),
        (limit(product="DOGE-USD"), "unknown_product"),
    ],
)
def test_off_grid_or_unknown_orders_are_recorded_rejections(payload, reason):
    w = writer()
    order = w.submit("alice", payload, T0)
    assert order.status is OrderStatus.REJECTED and order.reject_reason == reason
    assert w.accounts.reservations == {}
    assert [e["type"] for e in w.events] == ["order_rejected"]


# Idempotency ---------------------------------------------------------------------------------


def test_the_same_key_and_request_return_the_same_order():
    w = writer()
    first = w.submit("alice", limit(), T0)
    again = w.submit("alice", limit(qty="0.50", price="100.0"), T0 + 5)  # equal decimals
    assert again is first and len(w.orders) == 1 and len(w.accounts.reservations) == 1
    assert [e["type"] for e in w.events] == ["order_accepted"]


def test_the_same_key_with_a_different_request_is_refused():
    w = writer()
    w.submit("alice", limit(), T0)
    with pytest.raises(OrderError) as raised:
        w.submit("alice", limit(qty="0.6"), T0)
    assert raised.value.reason == "idempotency_key_reused" and len(w.orders) == 1


def test_a_rejected_order_stays_rejected_under_its_key():
    gate = Gate(open=False, reason="book_not_synced")
    w = writer(gate)
    first = w.submit("alice", limit(), T0)
    gate.decision = GateDecision(True, None, D("100"), D("100.5"))
    again = w.submit("alice", limit(), T0 + 10**9)  # the market reopened; the key is spent
    assert again is first and again.reject_reason == "book_not_synced"


def test_keys_are_per_account_and_ids_are_deterministic():
    w = writer()
    w.accounts.deposit("bob", "USD", D("1000"))
    a = w.submit("alice", limit(), T0)
    b = w.submit("bob", limit(), T0)
    assert a.order_id != b.order_id
    assert writer().submit("alice", limit(), T0).order_id == a.order_id


# Acceptance: gate, funds, reservations -------------------------------------------------------


def test_a_closed_gate_rejects_with_its_reason_and_holds_nothing():
    gate = Gate(open=False, reason="stale")
    w = writer(gate)
    order = w.submit("alice", limit(), T0)
    assert (order.status, order.reject_reason) == (OrderStatus.REJECTED, "stale")
    assert gate.calls == [("BTC-USD", T0)] and w.accounts.reservations == {}


def test_insufficient_funds_rejects():
    w = writer(usd="50")
    order = w.submit("alice", limit(qty="1", price="100.00"), T0)
    assert order.reject_reason == "insufficient_funds"
    assert w.accounts.balance("alice", "USD").available == D("50")


def test_reservations_cover_the_worst_case():
    w = writer()
    buy = w.submit("alice", limit(qty="0.5", price="100.00"), T0)
    # 50.00 notional + 120 bps taker fee (0.60), the higher of the two fees
    assert w.accounts.reservations[buy.order_id].remaining == D("50.60")
    sell = w.submit("alice", limit(cid="s", side="sell", qty="0.25", price="101.00"), T0)
    assert w.accounts.reservations[sell.order_id].remaining == D("0.25")
    mkt = w.submit("alice", market(qty="1"), T0)
    # best ask 100.50 widened by the 50 bps band, rounded up to the tick: 101.01
    assert w.band_prices[mkt.order_id] == D("101.01")
    assert w.accounts.reservations[mkt.order_id].remaining == D("101.01") + D("1.212120")
    usd = w.accounts.balance("alice", "USD")
    assert usd.reserved == D("50.60") + D("102.222120")
    assert usd.available + usd.reserved == D("100000")


def test_a_market_order_needs_the_opposite_side():
    w = writer(Gate(ask=None))
    assert w.submit("alice", market(), T0).reject_reason == "no_liquidity"
    assert w.submit("alice", market(cid="m2", side="sell"), T0).status is OrderStatus.PENDING


def test_band_edges_round_outward():
    btc = DEFAULT_INSTRUMENTS["BTC-USD"]
    assert band_price(D("100.50"), "buy", btc) == D("101.01")
    assert band_price(D("100.00"), "sell", btc) == D("99.50")
    assert band_price(D("100.01"), "sell", btc) == D("99.50")  # 99.509... rounds down


def test_orders_take_effect_after_the_delay():
    order = writer().submit("alice", limit(), T0)
    assert (order.status, order.created_at_ns, order.effective_at_ns) == (
        OrderStatus.PENDING,
        T0,
        T0 + DELAY,
    )


# Fills, fees and the state machine ------------------------------------------------------------


def test_partial_then_full_fill_settles_double_entry():
    w = writer()
    order = w.submit("alice", limit(qty="0.5", price="100.00"), T0)
    w.activate(order.order_id, T0 + DELAY)
    f1 = w.record_fill(
        order.order_id, D("100.00"), D("0.2"), Liquidity.MAKER, FillCause.PRINT, T0 + 10**9
    )
    assert (f1.fee, order.status) == (D("0.12"), OrderStatus.PARTIALLY_FILLED)
    f2 = w.record_fill(
        order.order_id, D("100.00"), D("0.3"), Liquidity.MAKER, FillCause.PRINT, T0 + 2 * 10**9
    )
    assert order.status is OrderStatus.FILLED and f2.fee == D("0.18")
    acct = w.accounts
    assert acct.balance("alice", "BTC").available == D("10.5")
    # Paid 50.00 + 0.30 fees; the taker-fee headroom (0.30) went back to available.
    assert acct.balance("alice", "USD").total == D("100000") - D("50.30")
    assert acct.balance("alice", "USD").reserved == 0 and acct.reservations == {}
    assert acct.balance(FEES, "USD").available == D("0.30")
    assert acct.balance(MARKET, "BTC").available == D("-0.5")
    assert all(v == 0 for v in acct.totals().values())


def test_many_small_fills_never_pay_more_fee_than_one():
    w = writer()
    order = w.submit("alice", limit(qty="0.000010", price="1.00", product="BTC-USD"), T0)
    for i in range(1000):
        w.record_fill(
            order.order_id, D("1.00"), D("0.00000001"), Liquidity.TAKER, FillCause.TAKER, T0 + i
        )
    # 0.00001 notional at 120 bps is 1.2e-7 exactly. Rounding each 1e-8 fill up on its own would
    # charge 1e-8 a fill, 1e-5 in all: 83 times the fee.
    assert order.fees == D("0.00000012") and order.status is OrderStatus.FILLED


def test_a_minimum_size_sale_never_pays_more_fee_than_it_earns():
    w = writer()
    order = w.submit("alice", limit(side="sell", qty="0.00001", price="0.01"), T0)
    fill = w.record_fill(
        order.order_id, D("0.01"), D("0.00001"), Liquidity.TAKER, FillCause.TAKER, T0
    )
    assert fill.notional == D("0.0000001") and fill.fee == D("0.00000001")
    assert w.accounts.balance("alice", "USD").available == D("100000") + D("0.00000009")


def test_fills_worse_than_the_limit_or_beyond_the_band_are_refused():
    w = writer()
    buy = w.submit("alice", limit(qty="0.5", price="100.00"), T0)
    with pytest.raises(InvalidTransition, match="worse than the limit"):
        w.record_fill(buy.order_id, D("100.01"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0)
    mkt = w.submit("alice", market(qty="0.5"), T0)
    with pytest.raises(ValueError, match="outside the market band"):
        w.record_fill(mkt.order_id, D("101.02"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0)
    w.record_fill(mkt.order_id, D("101.01"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0)
    with pytest.raises(ValueError, match="never rests"):
        w.record_fill(mkt.order_id, D("100.50"), D("0.1"), Liquidity.MAKER, FillCause.PRINT, T0)
    with pytest.raises(InvalidTransition, match="maker"):
        w.record_fill(buy.order_id, D("100"), D("0.1"), Liquidity.MAKER, FillCause.TAKER, T0)
    with pytest.raises(InvalidTransition, match="remaining"):
        w.record_fill(buy.order_id, D("100"), D("0.6"), Liquidity.TAKER, FillCause.TAKER, T0)


def test_market_remainder_expires_and_releases_funds():
    w = writer()
    mkt = w.submit("alice", market(qty="1"), T0)
    w.record_fill(mkt.order_id, D("100.50"), D("0.4"), Liquidity.TAKER, FillCause.TAKER, T0)
    w.expire(mkt.order_id, T0 + DELAY)
    assert mkt.status is OrderStatus.EXPIRED and w.accounts.reservations == {}
    with pytest.raises(InvalidTransition):
        w.activate(mkt.order_id, T0)
    usd = w.accounts.balance("alice", "USD")
    assert usd.reserved == 0 and usd.available == D("100000") - D("40.20") - mkt.fees


def test_terminal_states_are_final():
    w = writer()
    order = w.submit("alice", limit(), T0)
    w.cancel_now(order.order_id, T0)
    for action in (
        lambda: w.cancel_now(order.order_id, T0),
        lambda: w.activate(order.order_id, T0),
        lambda: w.record_fill(
            order.order_id, D("100"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0
        ),
        lambda: w.reject_on_effect(order.order_id, "stale", T0),
    ):
        with pytest.raises(InvalidTransition):
            action()
    with pytest.raises(InvalidTransition, match="only a market order"):
        w.expire(w.submit("alice", limit(cid="c2"), T0).order_id, T0)


def test_rejecting_on_effect_releases_funds():
    w = writer()
    order = w.submit("alice", limit(), T0)
    w.reject_on_effect(order.order_id, "stale", T0 + DELAY)
    assert order.reject_reason == "stale" and w.accounts.balance("alice", "USD").reserved == 0


# Cancels -------------------------------------------------------------------------------------


def test_cancel_goes_through_the_delay_and_is_idempotent():
    w = writer()
    order = w.submit("alice", limit(), T0)
    got = w.request_cancel("alice", T0 + 5, order_id=order.order_id)
    assert got is order and order.status is OrderStatus.PENDING  # still live until it takes effect
    assert order.cancel_effective_at_ns == T0 + 5 + DELAY
    w.request_cancel("alice", T0 + 9, client_order_id="c1")  # again, by key: unchanged
    assert order.cancel_requested_at_ns == T0 + 5
    assert [e["type"] for e in w.events] == ["order_accepted", "cancel_requested"]
    w.cancel_now(order.order_id, order.cancel_effective_at_ns)
    assert w.request_cancel("alice", T0 + 10**9, order_id=order.order_id).status is (
        OrderStatus.CANCELLED
    )


def test_fills_during_a_pending_cancel_stand():
    w = writer()
    order = w.submit("alice", limit(qty="0.5"), T0)
    w.activate(order.order_id, T0 + DELAY)
    w.request_cancel("alice", T0 + DELAY + 1, order_id=order.order_id)
    w.record_fill(
        order.order_id, D("100.00"), D("0.2"), Liquidity.MAKER, FillCause.PRINT, T0 + DELAY + 2
    )
    w.cancel_now(order.order_id, order.cancel_effective_at_ns)
    assert (order.status, order.filled_quantity) == (OrderStatus.CANCELLED, D("0.2"))
    assert w.accounts.balance("alice", "USD").reserved == 0


def test_cancels_only_reach_your_own_orders():
    w = writer()
    w.accounts.deposit("bob", "USD", D("100"))
    order = w.submit("alice", limit(), T0)
    with pytest.raises(OrderError) as raised:
        w.request_cancel("bob", T0, order_id=order.order_id)
    assert raised.value.reason == "unknown_order"
    with pytest.raises(OrderError):
        w.request_cancel("alice", T0, order_id="nope")


# Serialization and config --------------------------------------------------------------------


def test_money_serializes_as_strings():
    w = writer()
    order = w.submit("alice", limit(), T0)
    fill = w.record_fill(order.order_id, D("99.99"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0)
    for record in (order.to_dict(), fill.to_dict(), *(e for e in w.events)):
        text = json.dumps(record)  # no Decimal or float leaks into JSON
        assert "99.99" not in text or '"99.99"' in text
    assert fill.to_dict()["price"] == "99.99" and order.to_dict()["filled_quantity"] == "0.1"
    assert isinstance(json.loads(json.dumps(order.to_dict()))["fees"], str)


def test_fee_defaults_and_overrides():
    fees = FeeSchedule()
    assert (fees.maker_bps, fees.taker_bps) == (D("60"), D("120"))
    assert fees.fee(D("1000"), Liquidity.MAKER) == D("6.00")
    assert fees.fee(D("0.000001"), Liquidity.TAKER) == D("0.00000002")  # 1.2e-8 rounds up
    other = FeeSchedule.from_env({"EXCHANGE_MAKER_FEE_BPS": "25", "EXCHANGE_TAKER_FEE_BPS": "50"})
    assert (other.maker_bps, other.taker_bps) == (D("25"), D("50"))
    with pytest.raises(ValueError):
        FeeSchedule(maker_bps=D("-1"))


def test_market_band_is_configurable_per_pair():
    table = instruments_from_env(
        {"EXCHANGE_MARKET_BAND_BPS": "40", "EXCHANGE_MARKET_BAND_BPS_SOL_USD": "80"}
    )
    assert table["BTC-USD"].market_band_bps == D("40")
    assert table["SOL-USD"].market_band_bps == D("80")
    assert instruments_from_env({})["ETH-USD"].market_band_bps == D("50")
    with pytest.raises(ValueError):
        instruments_from_env({"EXCHANGE_MARKET_BAND_BPS": "0"})


def test_system_accounts_cannot_trade_or_receive_deposits():
    w = writer()
    for system in (MARKET, FEES, EXTERNAL):
        with pytest.raises(OrderError):
            w.submit(system, limit(), T0)
    with pytest.raises(ValueError):
        w.accounts.deposit(FEES, "USD", D("1"))
