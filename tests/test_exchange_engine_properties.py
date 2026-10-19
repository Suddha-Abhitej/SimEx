"""Hypothesis properties of the fill simulator over random synthetic markets and orders (#8).

After every market event:
- money is conserved and no agent balance is negative (the order-model invariants still hold);
- maker fills come only from the venue's activity: PRINT and TRADE_THROUGH fills only while a
  print is being applied, BOOK_CROSS only while a book update is; and the maker volume one print
  produces never exceeds that print;
- nothing fills while the book isn't SYNCED;
- a taker fill happens only when an order takes effect, at a price on the book at that moment,
  and never takes more at a level than the level held (after depletion);
- a market buy never pays above its band edge, a limit never fills worse than its limit.

And the same stream replayed into a fresh engine gives byte-identical events.
"""

import json
from decimal import Decimal as D

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from test_exchange_properties import check_invariants

from simex.book import BookState
from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, GateDecision
from simex.exchange.engine import CancelAttribution, EngineSettings, PairEngine
from simex.marketdata import BookUpdate, ResyncMarker, Trade

P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
MS = 1_000_000
TICK = D("0.01")
MID = D("100.00")


class Gate:
    def __init__(self):
        self.engine = None

    def check(self, product_id, now_ns):
        book = self.engine.book
        if not book.ready:
            return GateDecision(False, "book_not_synced")
        bid, ask = book.best_bid(), book.best_ask()
        return GateDecision(True, None, bid[0] if bid else None, ask[0] if ask else None)


def price(n):
    return MID + TICK * n


level = st.tuples(st.integers(-12, 12), st.sampled_from([D(0), D("0.5"), D("1"), D("2.5")]))
diff = st.tuples(st.just("diff"), st.lists(level, max_size=4))
# Prints that hit where our orders rest: sells at bid prices, buys at ask prices.
near_print = st.one_of(
    st.tuples(
        st.just("print"),
        st.integers(-8, -1),
        st.sampled_from([D("0.3"), D("1"), D("3")]),
        st.just("sell"),
    ),
    st.tuples(
        st.just("print"),
        st.integers(1, 8),
        st.sampled_from([D("0.3"), D("1"), D("3")]),
        st.just("buy"),
    ),
)
any_print = st.tuples(
    st.just("print"),
    st.integers(-12, 12),
    st.sampled_from([D("0.3"), D("1"), D("3")]),
    st.sampled_from(["buy", "sell"]),
)
# Weighted: diffs and prints often, a resync now and then (about 2% of market steps).
_market = {
    "diff": diff,
    "near": near_print,
    "any": any_print,
    "snapshot": st.tuples(st.just("snapshot")),
    "marker": st.tuples(st.just("marker")),
}
market_step = st.sampled_from(
    ["diff"] * 35 + ["near"] * 40 + ["any"] * 15 + ["snapshot"] * 8 + ["marker"] * 2
).flatmap(_market.__getitem__)
order_step = st.tuples(
    st.just("order"),
    st.sampled_from(["k0", "k1", "k2", "k3", "k4", "k5"]),
    st.sampled_from(["buy", "sell"]),
    st.sampled_from(["limit", "limit", "market"]),
    st.sampled_from([D("0.5"), D("1"), D("2"), D("5")]),
    st.integers(-8, 8),
)
cancel_step = st.tuples(st.just("cancel"), st.sampled_from(["k0", "k1", "k2", "k3", "k4", "k5"]))
steps = st.lists(
    st.tuples(
        st.one_of(market_step, market_step, market_step, order_step, order_step, cancel_step),
        st.integers(0, 400),
    ),
    min_size=10,
    max_size=80,
)


def snapshot_levels():
    bids = tuple((price(-i), D("0.5")) for i in range(1, 8))
    asks = tuple((price(i), D("0.5")) for i in range(1, 8))
    return bids, asks


class Run:
    """A market and orders driven through one engine, with every fill checked in context."""

    def __init__(self, mode=CancelAttribution.PROPORTIONAL):
        gate = Gate()
        self.writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate, 200 * MS)
        for asset, amount in (("USD", "100000"), ("BTC", "100")):
            self.writer.accounts.deposit("alice", asset, D(amount))
        self.engine = PairEngine(P, self.writer, EngineSettings(cancel_attribution=mode))
        gate.engine = self.engine
        self.now = T0
        self.ids = 0
        self.engine.on_event(self._book(*snapshot_levels(), snapshot=True))

    def _book(self, bids, asks, snapshot=False):
        self.ids += 1
        return BookUpdate(
            "gemini",
            P,
            snapshot,
            tuple(bids),
            tuple(asks),
            (self.ids, self.ids),
            False,
            self.now,
            self.now,
        )

    def step(self, s, dt):
        self.now += dt * MS
        kind = s[0]
        if kind == "order":
            _, cid, side, otype, qty, n = s
            payload = {
                "client_order_id": cid,
                "product_id": P,
                "side": side,
                "type": otype,
                "quantity": str(qty),
            }
            if otype == "limit":
                payload["limit_price"] = str(price(n))
            try:
                order = self.writer.submit("alice", payload, self.now)
            except Exception:  # a reused key with a different order: refused, nothing changes
                return
            self.engine.submit(order)
            return
        if kind == "cancel":
            try:
                self.writer.request_cancel("alice", self.now, client_order_id=s[1])
            except Exception:
                pass
            return
        if kind == "diff":
            bids = tuple((price(-abs(n) - 1), q) for n, q in s[1] if n % 2 == 0)
            asks = tuple((price(abs(n) + 1), q) for n, q in s[1] if n % 2 == 1)
            event = self._book(bids, asks)
        elif kind == "snapshot":
            event = self._book(*snapshot_levels(), snapshot=True)
        elif kind == "marker":
            event = ResyncMarker("gemini", P, f"r{self.ids}", "update_id_gap", self.now)
        else:
            _, n, size, aggressor = s
            self.ids += 1
            event = Trade("gemini", P, str(self.ids), price(n), size, aggressor, self.now, self.now)
        self.apply(event)

    def apply(self, event):
        engine, writer = self.engine, self.writer
        before_events = len(writer.events)
        synced_before = engine.book.state is BookState.SYNCED
        book_before = {"ask": dict(engine.book.asks.items()), "bid": dict(engine.book.bids.items())}
        depleted_before = {k: v.quantity for k, v in engine.depletion.items()}
        effective = {
            o.order_id
            for o in writer.orders.values()
            if o.order_id in engine.pending and o.effective_at_ns <= self.now
        }
        engine.on_event(event)
        new_fills = [e["fill"] for e in writer.events[before_events:] if e["type"] == "fill"]
        taken: dict[tuple[str, D], D] = {}
        maker_from_print = D(0)
        for fill in new_fills:
            cause, qty, px = fill["cause"], D(fill["quantity"]), D(fill["price"])
            order = writer.orders[fill["order_id"]]
            resynced_now = isinstance(event, BookUpdate) and event.snapshot
            assert synced_before or resynced_now, "a fill while not SYNCED"
            if cause == "taker":
                assert fill["order_id"] in effective, "a taker fill outside taking effect"
                side = "ask" if fill["side"] == "buy" else "bid"
                key = (side, px)
                taken[key] = taken.get(key, D(0)) + qty
                live = book_before[side].get(px, D(0)) - depleted_before.get(key, D(0))
                assert taken[key] <= live, f"took {taken[key]} at {px}, the book held {live}"
            elif cause in ("print", "trade_through"):
                assert isinstance(event, Trade), f"a {cause} fill without a print"
                maker_from_print += qty
            else:
                assert isinstance(event, BookUpdate), "a book_cross fill without a book update"
            limit = order.request.limit_price
            if limit is not None:
                assert px <= limit if order.side.value == "buy" else px >= limit
            edge = None if limit is not None else writer.band_prices.get(order.order_id)
            if edge is not None:
                assert px <= edge if order.side.value == "buy" else px >= edge
        if isinstance(event, Trade):
            assert maker_from_print <= event.size, "more maker volume than the print"
        check_invariants(writer)

    def events(self):
        return json.dumps(self.writer.events, sort_keys=True)


@settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(steps)
def test_fills_come_only_from_the_market_and_money_is_conserved(seq):
    for mode in CancelAttribution:
        run = Run(mode)
        for s, dt in seq:
            run.step(s, dt)
        run.engine.advance(run.now + 10**9)
        check_invariants(run.writer)


@settings(max_examples=100, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(steps)
def test_replay_is_byte_identical(seq):
    outputs = []
    for _ in range(2):
        run = Run()
        for s, dt in seq:
            run.step(s, dt)
        run.engine.advance(run.now + 10**9)
        outputs.append(run.events())
    assert outputs[0] == outputs[1]
