"""Hypothesis properties of the order model and account writer over random sequences (#7).

Random interleavings of deposits, orders (market and limit, all pairs, several accounts),
duplicate submissions, gate changes, cancel requests and their execution, and fills at prices
the order allows. After every step:

- money is neither created nor destroyed: each asset sums to zero over all accounts (deposits
  come from EXTERNAL, so the system as a whole holds exactly nothing);
- no agent balance, available or reserved, is ever negative;
- an account's reserved funds equal what its live orders still hold, and a finished order holds
  nothing;
- an order never fills more than its quantity, never at a price worse than its limit or band,
  and never leaves a terminal state;
- one key names one order.

And the same sequence replayed into a fresh writer produces byte-identical events.
"""

import json
from decimal import ROUND_UP
from decimal import Decimal as D

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    FillCause,
    GateDecision,
    InvalidTransition,
    Liquidity,
    OrderError,
    OrderStatus,
    OrderType,
    Side,
)
from simex.exchange.accounts import SYSTEM_ACCOUNTS

ACCOUNTS = ("alice", "bob", "carol")
PRODUCTS = tuple(DEFAULT_INSTRUMENTS)
MAKER_CAUSES = (FillCause.PRINT, FillCause.TRADE_THROUGH, FillCause.BOOK_CROSS)
T0 = 1_792_036_800_000_000_000


class Gate:
    def __init__(self):
        self.open = True
        self.mid = {"BTC-USD": D("100.00"), "ETH-USD": D("20.00"), "SOL-USD": D("3.000")}

    def check(self, product_id, now_ns):
        if not self.open:
            return GateDecision(False, "book_not_synced")
        tick = DEFAULT_INSTRUMENTS[product_id].price_tick
        mid = self.mid[product_id]
        return GateDecision(True, None, mid - tick, mid + tick)


def ticks(product, n):
    return DEFAULT_INSTRUMENTS[product].price_tick * n


def sizes(product, n):
    return DEFAULT_INSTRUMENTS[product].min_size * n


def apply_step(w: AccountWriter, gate: Gate, step: tuple, now: int) -> None:
    """One step of a generated sequence; refusals the writer is allowed to make are swallowed."""
    kind = step[0]
    try:
        if kind == "deposit":
            _, account, asset, amount = step
            w.accounts.deposit(account, asset, amount)
        elif kind == "gate":
            gate.open = step[1]
        elif kind == "submit":
            _, account, cid, product, side, otype, n_qty, n_price = step
            payload = {
                "client_order_id": cid,
                "product_id": product,
                "side": side,
                "type": otype,
                "quantity": str(sizes(product, n_qty)),
            }
            if otype == "limit":
                payload["limit_price"] = str(gate.mid[product] + ticks(product, n_price))
            w.submit(account, payload, now)
        elif kind == "cancel":
            _, account, cid = step
            w.request_cancel(account, now, client_order_id=cid)
        elif kind == "fill":
            _, pick, frac, n_off, maker = step
            live = sorted(o.order_id for o in w.orders.values() if not o.status.terminal)
            if not live:
                return
            order = w.orders[live[pick % len(live)]]
            instrument = DEFAULT_INSTRUMENTS[order.product_id]
            # n_off ticks better than the limit or band edge for the agent; negative is worse,
            # an illegal fill the writer must refuse.
            if order.request.type is OrderType.LIMIT:
                edge = order.request.limit_price
                liquidity = Liquidity.MAKER if maker else Liquidity.TAKER
            else:
                edge = w.band_prices[order.order_id]
                liquidity = Liquidity.TAKER
            offset = ticks(order.product_id, n_off)
            price = edge - offset if order.side is Side.BUY else edge + offset
            if price <= 0:
                return
            cause = MAKER_CAUSES[n_off % 3] if liquidity is Liquidity.MAKER else FillCause.TAKER
            qty = (order.remaining * frac).quantize(instrument.size_increment)
            if qty <= 0:
                qty = order.remaining
            if n_off < 0:
                before = (order.status, order.filled_quantity, len(w.events))
                try:
                    w.record_fill(order.order_id, price, qty, liquidity, cause, now)
                except (InvalidTransition, ValueError):
                    assert (order.status, order.filled_quantity, len(w.events)) == before
                    return
                raise AssertionError(f"a fill at {price}, worse than {edge}, was accepted")
            w.record_fill(order.order_id, price, qty, liquidity, cause, now)
        elif kind == "activate":
            for order in sorted(w.orders.values(), key=lambda o: o.order_id):
                if (
                    order.status is OrderStatus.PENDING
                    and order.request.type is OrderType.LIMIT
                    and order.effective_at_ns <= now
                ):
                    w.activate(order.order_id, now)
        elif kind == "settle":
            # What the engine does when time passes: cancels and market remainders finish.
            for order in sorted(w.orders.values(), key=lambda o: o.order_id):
                if order.status.terminal:
                    continue
                if order.cancel_effective_at_ns is not None and order.cancel_effective_at_ns <= now:
                    w.cancel_now(order.order_id, now)
                elif order.request.type is OrderType.MARKET and order.effective_at_ns <= now:
                    w.expire(order.order_id, now)
    except OrderError as exc:
        # the only refusals the writer may raise rather than record
        assert exc.reason in ("idempotency_key_reused", "unknown_order"), exc
    except InvalidTransition:
        pass  # e.g. activating a partly filled order; the writer must refuse it, never corrupt


assets = st.sampled_from(["USD", "BTC", "ETH", "SOL"])
step = st.one_of(
    st.tuples(
        st.just("deposit"),
        st.sampled_from(ACCOUNTS),
        assets,
        st.integers(1, 10**6).map(lambda n: D(n) / 100),
    ),
    st.tuples(st.just("gate"), st.booleans()),
    st.tuples(
        st.just("submit"),
        st.sampled_from(ACCOUNTS),
        st.sampled_from([f"k{i}" for i in range(8)]),  # few keys: plenty of reuse
        st.sampled_from(PRODUCTS),
        st.sampled_from(["buy", "sell"]),
        st.sampled_from(["limit", "market"]),
        st.integers(1, 5000),
        st.integers(-50, 50),
    ),
    st.tuples(
        st.just("cancel"), st.sampled_from(ACCOUNTS), st.sampled_from([f"k{i}" for i in range(8)])
    ),
    *[
        st.tuples(
            st.just("fill"),
            st.integers(0, 50),
            st.sampled_from([D("0.1"), D("0.5"), D("1")]),
            st.integers(-3, 20),
            st.booleans(),
        )
    ]
    * 3,  # weighted: fills are what the money properties are about
    st.tuples(st.just("activate")),
    st.tuples(st.just("activate")),
    st.tuples(st.just("settle")),
)


def check_invariants(w: AccountWriter) -> None:
    fees_by_order: dict[str, D] = {}
    for event in w.events:
        if event["type"] == "fill":
            fill = event["fill"]
            fees_by_order[fill["order_id"]] = fees_by_order.get(fill["order_id"], D(0)) + D(
                fill["fee"]
            )
    acct = w.accounts
    for asset, total in acct.totals().items():
        assert total == 0, f"{asset} created or destroyed: {total}"
    held: dict[tuple[str, str], D] = {}
    for order_id, reservation in acct.reservations.items():
        assert not w.orders[order_id].status.terminal, "a finished order still holds funds"
        assert reservation.remaining >= 0
        key = (reservation.account_id, reservation.asset)
        held[key] = held.get(key, D(0)) + reservation.remaining
    for account, balances in acct.balances.items():
        if account in SYSTEM_ACCOUNTS:
            continue
        for asset, balance in balances.items():
            assert balance.available >= 0 and balance.reserved >= 0, (account, asset, balance)
            assert balance.reserved == held.get((account, asset), D(0)), (account, asset)
    assert acct.balance("_fees", "USD").available >= 0
    for order in w.orders.values():
        assert 0 <= order.filled_quantity <= order.request.quantity
        if order.filled_quantity and order.request.limit_price is not None:
            avg = order.filled_notional / order.filled_quantity
            if order.side is Side.BUY:
                assert avg <= order.request.limit_price
            else:
                assert avg >= order.request.limit_price
        if order.status is OrderStatus.REJECTED:
            assert order.filled_quantity == 0
        if order.status is OrderStatus.FILLED:
            assert order.remaining == 0
        # The fee is the rounded-up fee on the running totals, never a sum of per-fill roundings.
        expected_fee = (
            order.maker_notional * w.fees.rate(Liquidity.MAKER)
            + order.taker_notional * w.fees.rate(Liquidity.TAKER)
        ).quantize(w.fees.fee_increment, rounding=ROUND_UP)
        assert order.fees == expected_fee == fees_by_order.get(order.order_id, D(0))
    assert len(w.by_key) == len(w.orders) == len(set(w.by_key.values()))


FUNDING = {"USD": D("100000"), "BTC": D("50"), "ETH": D("500"), "SOL": D("10000")}


def fresh():
    gate = Gate()
    w = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate, 200_000_000)
    for account in ACCOUNTS[:2]:  # carol starts empty: her orders are mostly refused for funds
        for asset, amount in FUNDING.items():
            w.accounts.deposit(account, asset, amount)
    return w, gate


class WriterMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.w, self.gate = fresh()
        self.now = T0
        self.terminal: dict[str, tuple] = {}

    @rule(s=step, dt=st.integers(0, 10**9))
    def do(self, s, dt):
        self.now += dt
        apply_step(self.w, self.gate, s, self.now)

    @invariant()
    def money_and_orders(self):
        check_invariants(self.w)

    @invariant()
    def terminal_orders_never_change(self):
        for order_id, snapshot in self.terminal.items():
            order = self.w.orders[order_id]
            assert (order.status, order.filled_quantity, order.fees) == snapshot
        for order in self.w.orders.values():
            if order.status.terminal and order.order_id not in self.terminal:
                self.terminal[order.order_id] = (order.status, order.filled_quantity, order.fees)

    @precondition(lambda self: self.w.orders)
    @rule()
    def duplicates_return_the_same_order(self):
        order = next(iter(self.w.orders.values()))
        before = len(self.w.events)
        again = self.w.submit(order.account_id, order.request, self.now)
        assert again is order and len(self.w.events) == before


TestWriterMachine = WriterMachine.TestCase
TestWriterMachine.settings = settings(
    max_examples=300,
    stateful_step_count=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(st.tuples(step, st.integers(0, 10**9)), min_size=20, max_size=120))
def test_replaying_the_same_sequence_gives_byte_identical_events(steps):
    runs = []
    for _ in range(2):
        w, gate = fresh()
        now = T0
        for s, dt in steps:
            now += dt
            apply_step(w, gate, s, now)
        check_invariants(w)
        runs.append(json.dumps(w.events, sort_keys=True))
    assert runs[0] == runs[1]
