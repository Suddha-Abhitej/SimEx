"""The fill simulator (#8, ADR 0014): scenarios on hand-written books and prints."""

from decimal import Decimal as D

import pytest

from simex.book import BookState
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    OrderStatus,
)
from simex.exchange.engine import BookCross, CancelAttribution, EngineSettings, PairEngine
from simex.marketdata import BookUpdate, ResyncMarker, Trade

P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
MS = 1_000_000
DELAY = 200 * MS
_ids = iter(range(1, 10**9))


class Gate:
    def __init__(self, engine_ref):
        self.engine_ref = engine_ref
        self.open = True

    def check(self, product_id, now_ns):
        book = self.engine_ref[0].book
        if not self.open:
            return GateDecision(False, "stale")
        if not book.ready:
            return GateDecision(False, "book_not_synced")
        bid, ask = book.best_bid(), book.best_ask()
        return GateDecision(True, None, bid[0] if bid else None, ask[0] if ask else None)


def make(attribution=CancelAttribution.PROPORTIONAL, **settings):
    ref = []
    gate = Gate(ref)
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate, DELAY)
    for asset, amount in (("USD", "1000000"), ("BTC", "100")):
        writer.accounts.deposit("alice", asset, D(amount))
    engine = PairEngine(P, writer, EngineSettings(cancel_attribution=attribution, **settings))
    ref.append(engine)
    return engine, writer, gate


def book(t, bids=(), asks=(), snapshot=False):
    n = next(_ids)
    return BookUpdate(
        "gemini",
        P,
        snapshot,
        tuple((D(p), D(q)) for p, q in bids),
        tuple((D(p), D(q)) for p, q in asks),
        (n, n),
        False,
        t,
        t,
    )


def trade(t, price, size, aggressor):
    return Trade("gemini", P, str(next(_ids)), D(price), D(size), aggressor, t, t)


SNAP_BIDS = [("100.00", "2"), ("99.99", "3"), ("99.98", "5")]
SNAP_ASKS = [("100.01", "1"), ("100.02", "2"), ("100.10", "4"), ("101.00", "50")]


def started(**kw):
    engine, writer, gate = make(**kw)
    engine.on_event(book(T0, SNAP_BIDS, SNAP_ASKS, snapshot=True))
    return engine, writer, gate


def order(writer, engine, cid, side, kind, qty, price=None, t=T0):
    payload = {"client_order_id": cid, "product_id": P, "side": side, "type": kind, "quantity": qty}
    if price is not None:
        payload["limit_price"] = price
    o = writer.submit("alice", payload, t)
    engine.submit(o)
    return o


def fills(writer, order_id=None):
    return [
        (D(e["fill"]["price"]), D(e["fill"]["quantity"]), e["fill"]["cause"])
        for e in writer.events
        if e["type"] == "fill" and (order_id is None or e["fill"]["order_id"] == order_id)
    ]


# Taker: market orders, the band and the depletion map ----------------------------------------


def test_a_market_order_walks_the_book_after_the_delay():
    engine, writer, _ = started()
    o = order(writer, engine, "m1", "buy", "market", "2.5")
    engine.advance(T0 + DELAY - 1)
    assert o.status is OrderStatus.PENDING  # not before its effective time
    engine.advance(T0 + DELAY)
    assert fills(writer, o.order_id) == [
        (D("100.01"), D("1"), "taker"),
        (D("100.02"), D("1.5"), "taker"),
    ]
    assert o.status is OrderStatus.FILLED and writer.accounts.reservations == {}


def test_the_band_stops_a_walk_and_the_rest_expires():
    engine, writer, _ = started()
    # best ask 100.01, 50 bps band: edge 100.51; 101.00 is outside it
    o = order(writer, engine, "m1", "buy", "market", "10")
    engine.advance(T0 + DELAY)
    assert sum(q for _, q, _ in fills(writer, o.order_id)) == D("7")  # 1 + 2 + 4 inside the band
    assert o.status is OrderStatus.EXPIRED
    assert engine.stats.counts["band_binds"] == 1
    assert writer.accounts.balance("alice", "USD").reserved == 0


def test_a_thin_book_fills_what_exists_without_counting_the_band():
    engine, writer, _ = make()
    engine.on_event(book(T0, [("100", "1")], [("100.01", "0.3")], snapshot=True))
    o = order(writer, engine, "m1", "buy", "market", "1")
    engine.advance(T0 + DELAY)
    assert o.filled_quantity == D("0.3") and o.status is OrderStatus.EXPIRED
    assert engine.stats.counts["band_binds"] == 0


def test_depletion_stops_two_orders_eating_the_same_liquidity():
    engine, writer, _ = started()
    a = order(writer, engine, "a", "buy", "market", "1")
    b = order(writer, engine, "b", "buy", "market", "1")
    engine.advance(T0 + DELAY)
    assert fills(writer, a.order_id) == [(D("100.01"), D("1"), "taker")]
    assert fills(writer, b.order_id) == [(D("100.02"), D("1"), "taker")]


@pytest.mark.parametrize("how", ["venue_update", "snapshot", "age"])
def test_depletion_entries_expire_and_are_counted(how):
    engine, writer, _ = started()
    order(writer, engine, "a", "buy", "market", "1")
    engine.advance(T0 + DELAY)
    assert ("ask", D("100.01")) in engine.depletion
    t = T0 + DELAY + MS
    if how == "venue_update":
        engine.on_event(book(t, asks=[("100.01", "0.8")]))
    elif how == "snapshot":
        engine.on_event(book(t, SNAP_BIDS, SNAP_ASKS, snapshot=True))
    else:
        engine.advance(T0 + DELAY + engine.settings.depletion_max_age_ns + 1)
    assert ("ask", D("100.01")) not in engine.depletion
    assert engine.stats.counts[f"depletion_expired:{how}"] >= 1


def test_orders_are_rejected_on_effect_when_the_book_is_not_synced_or_the_gate_closes():
    engine, writer, gate = started()
    a = order(writer, engine, "a", "buy", "market", "1")
    engine.on_event(ResyncMarker("gemini", P, "r1", "update_id_gap", T0 + MS))
    engine.advance(T0 + DELAY)
    assert (a.status, a.reject_reason) == (OrderStatus.REJECTED, "book_not_synced")
    engine.on_event(book(T0 + DELAY + MS, SNAP_BIDS, SNAP_ASKS, snapshot=True))
    b = order(writer, engine, "b", "buy", "market", "1", t=T0 + DELAY + 2 * MS)
    gate.open = False
    engine.advance(T0 + 2 * DELAY + 2 * MS)
    assert b.reject_reason == "stale"
    assert writer.accounts.balance("alice", "USD").reserved == 0
    assert engine.stats.counts["rejected_on_effect:book_not_synced"] == 1


# Limit orders: the crossing part, then the queue ---------------------------------------------


def test_a_crossing_limit_fills_as_taker_then_rests_at_the_back_of_the_queue():
    engine, writer, _ = started()
    o = order(writer, engine, "l1", "buy", "limit", "3", "100.01")
    engine.advance(T0 + DELAY)
    assert fills(writer, o.order_id) == [(D("100.01"), D("1"), "taker")]
    assert o.status is OrderStatus.PARTIALLY_FILLED
    assert engine.resting[o.order_id].ahead == 0  # no venue bid at 100.01: front of the queue
    passive = order(writer, engine, "l2", "buy", "limit", "1", "99.99", t=T0 + DELAY)
    engine.advance(T0 + 2 * DELAY)
    assert engine.resting[passive.order_id].ahead == D("3")


def rest_bid(qty="1", price="100.00", **kw):
    engine, writer, gate = started(**kw)
    o = order(writer, engine, "l1", "buy", "limit", qty, price)
    engine.advance(T0 + DELAY)
    return engine, writer, o


def test_a_print_at_the_price_clears_ahead_then_fills():
    engine, writer, o = rest_bid()
    assert engine.resting[o.order_id].ahead == D("2")
    engine.on_event(trade(T0 + DELAY + MS, "100.00", "1.5", "sell"))
    assert o.filled_quantity == 0 and engine.resting[o.order_id].ahead == D("0.5")
    engine.on_event(trade(T0 + DELAY + 2 * MS, "100.00", "1", "sell"))
    assert fills(writer, o.order_id) == [(D("100.00"), D("0.5"), "print")]
    assert engine.stats.counts["maker_fills:print"] == 1


def test_our_orders_at_one_price_queue_by_arrival_and_share_the_print():
    engine, writer, a = rest_bid()
    b = order(writer, engine, "l2", "buy", "limit", "1", "100.00", t=T0 + DELAY)
    engine.advance(T0 + 2 * DELAY)
    engine.on_event(trade(T0 + 2 * DELAY + MS, "100.00", "2.6", "sell"))
    # 2 venue ahead of both, then a (0.6 of it); b is behind a and gets nothing
    assert a.filled_quantity == D("0.6") and b.filled_quantity == 0
    assert engine.resting[b.order_id].ahead == 0


def test_a_trade_through_fills_capped_by_the_print():
    engine, writer, o = rest_bid(qty="2")
    engine.on_event(trade(T0 + DELAY + MS, "99.99", "0.7", "sell"))
    assert fills(writer, o.order_id) == [(D("100.00"), D("0.7"), "trade_through")]
    engine.on_event(trade(T0 + DELAY + 2 * MS, "99.98", "5", "sell"))
    assert o.status is OrderStatus.FILLED
    assert engine.stats.quantities["maker_fills:trade_through"] == D("2")


def test_prints_on_the_other_side_or_at_worse_prices_do_nothing():
    engine, writer, o = rest_bid()
    engine.on_event(trade(T0 + DELAY + MS, "100.01", "1", "buy"))  # took asks
    engine.on_event(trade(T0 + DELAY + 2 * MS, "100.00", "1", "buy"))  # buy aggressor
    assert o.filled_quantity == 0 and engine.resting[o.order_id].ahead == D("2")


def test_under_fill_the_venue_book_crossing_our_price_fills_as_maker():
    # An improving bid inside a wide spread: no venue bid at its price, so it's alone there.
    engine, writer, _ = make(book_cross=BookCross.FILL)
    engine.on_event(book(T0, [("100.00", "2")], [("100.10", "1")], snapshot=True))
    o = order(writer, engine, "l1", "buy", "limit", "1", "100.05")
    engine.advance(T0 + DELAY)
    assert engine.resting[o.order_id].ahead == 0
    engine.on_event(book(T0 + DELAY + MS, asks=[("100.04", "0.4")]))  # a new ask below our bid
    assert fills(writer, o.order_id) == [(D("100.05"), D("0.4"), "book_cross")]
    assert engine.stats.counts["maker_fills:book_cross"] == 1
    # The crossing ask is now used: another update elsewhere doesn't fill again.
    engine.on_event(book(T0 + DELAY + 2 * MS, bids=[("99.90", "1")]))
    assert o.filled_quantity == D("0.4")


def test_by_default_a_cross_fills_nothing_and_puts_us_first_in_the_queue():
    engine, writer, o = rest_bid()  # 1 at 100.00 with 2 ahead
    assert engine.resting[o.order_id].ahead == D("2")
    t = T0 + DELAY + MS
    # The maker requotes: the bid at 100.00 goes, an ask appears at our price. No print.
    engine.on_event(book(t, bids=[("100.00", "0")], asks=[("100.00", "3")]))
    assert o.filled_quantity == 0 and engine.stats.counts["maker_fills:book_cross"] == 0
    assert engine.resting[o.order_id].ahead == 0 and engine.resting[o.order_id].crossed
    engine.on_event(book(t + MS, asks=[("100.00", "2")]))  # still crossed: counted once
    assert engine.book.state is BookState.SYNCED
    assert engine.stats.counts["crosses_marked"] == 1
    # First in the queue for good: bids rejoin at our price behind us, a snapshot comes.
    engine.on_event(book(t + 2 * MS, bids=[("100.00", "5")], asks=[("100.00", "0")]))
    engine.on_event(book(t + 3 * MS, [("100.00", "5")], SNAP_ASKS, snapshot=True))
    later = order(writer, engine, "l2", "buy", "limit", "1", "100.00", t=t + 3 * MS)
    engine.advance(t + 3 * MS + DELAY)
    assert engine.resting[o.order_id].ahead == 0
    assert engine.resting[later.order_id].ahead == D("5")
    # Only prints fill it: one at our price fills us before the later order.
    engine.on_event(trade(t + 4 * MS + DELAY, "100.00", "0.6", "sell"))
    assert fills(writer, o.order_id) == [(D("100.00"), D("0.6"), "print")]
    assert later.filled_quantity == 0
    # A print through our price fills the rest.
    engine.on_event(trade(t + 5 * MS + DELAY, "99.95", "1", "sell"))
    assert o.filled_quantity == 1
    assert [c for _, _, c in fills(writer, o.order_id)] == ["print", "trade_through"]


# Shrinks: fills or cancels -------------------------------------------------------------------


def test_a_shrink_explained_by_a_print_is_not_also_a_cancel():
    engine, writer, o = rest_bid()
    t = T0 + DELAY + MS
    engine.on_event(book(t, bids=[("100.00", "1")]))  # shrank by 1
    engine.on_event(trade(t + 40 * MS, "100.00", "1", "sell"))  # its print, 40 ms later
    engine.advance(t + 2 * engine.settings.match_window_ns)
    # The print cleared 1 of the 2 ahead; the shrink was that same fill, not a cancel.
    assert engine.resting[o.order_id].ahead == D("1")
    assert engine.stats.quantities["cancels:proportional"] == 0


def test_a_print_that_arrives_first_claims_the_shrink_that_follows():
    engine, writer, o = rest_bid()
    t = T0 + DELAY + MS
    engine.on_event(trade(t, "100.00", "1", "sell"))
    engine.on_event(book(t + 30 * MS, bids=[("100.00", "1")]))
    engine.advance(t + 2 * engine.settings.match_window_ns)
    assert engine.resting[o.order_id].ahead == D("1")
    assert engine.stats.quantities["cancels:proportional"] == 0


@pytest.mark.parametrize(
    ("mode", "ahead"),
    [
        (CancelAttribution.PROPORTIONAL, D("1")),  # 2 x (1 cancelled / 2 there)
        (CancelAttribution.PESSIMISTIC, D("2")),  # behind us
        (CancelAttribution.OPTIMISTIC, D("1")),  # ahead of us: all of it
    ],
)
def test_an_unexplained_shrink_is_a_cancel_attributed_by_mode(mode, ahead):
    engine, writer, o = rest_bid(attribution=mode)
    t = T0 + DELAY + MS
    engine.on_event(book(t, bids=[("100.00", "1")]))
    engine.advance(t + engine.settings.match_window_ns + 1)
    assert engine.resting[o.order_id].ahead == ahead
    assert o.filled_quantity == 0  # a cancel moves us up, never fills us


def test_the_bracket_orders_fill_rates():
    """Same market, three settings: pessimistic <= proportional <= optimistic."""
    filled = {}
    for mode in CancelAttribution:
        engine, writer, o = rest_bid(qty="2", attribution=mode)
        t = T0 + DELAY + MS
        engine.on_event(book(t, bids=[("100.00", "3")]))  # 1 joins behind us
        engine.on_event(book(t + MS, bids=[("100.00", "1.5")]))  # 1.5 cancelled
        engine.advance(t + 600 * MS)
        engine.on_event(trade(t + 700 * MS, "100.00", "2", "sell"))
        filled[mode] = o.filled_quantity
    assert (
        filled[CancelAttribution.PESSIMISTIC]
        <= filled[CancelAttribution.PROPORTIONAL]
        <= filled[CancelAttribution.OPTIMISTIC]
    )
    assert filled[CancelAttribution.PESSIMISTIC] == 0
    assert filled[CancelAttribution.OPTIMISTIC] == D("1.5")


# Resyncs, pauses, cancels, duplicates --------------------------------------------------------


def test_prints_while_not_synced_are_ignored_and_counted_and_ahead_is_capped():
    engine, writer, o = rest_bid()
    engine.on_event(ResyncMarker("gemini", P, "r", "refresh", T0 + DELAY + MS))
    engine.on_event(trade(T0 + DELAY + 2 * MS, "99.00", "3", "sell"))  # would trade through
    assert o.filled_quantity == 0
    assert engine.stats.quantities["prints_ignored_unsynced"] == D("3")
    assert engine.book.state is BookState.RESYNCING
    engine.on_event(book(T0 + DELAY + 3 * MS, [("100.00", "0.5")], SNAP_ASKS, snapshot=True))
    assert engine.resting[o.order_id].ahead == D("0.5")  # min(2, new size)


def test_prints_before_the_first_snapshot_are_counted_apart_from_a_resync():
    engine, writer, _ = make()
    engine.on_event(trade(T0, "100.00", "1", "sell"))  # no book yet: warm-up
    engine.on_event(book(T0 + MS, SNAP_BIDS, SNAP_ASKS, snapshot=True))
    engine.on_event(ResyncMarker("gemini", P, "r", "refresh", T0 + 2 * MS))
    engine.on_event(trade(T0 + 3 * MS, "100.00", "2", "sell"))  # during a resync
    q = engine.stats.quantities
    assert (q["prints_before_first_snapshot"], q["prints_ignored_unsynced"]) == (D("1"), D("2"))


def test_a_cancel_takes_effect_after_the_delay_and_fills_in_between_stand():
    engine, writer, o = rest_bid()
    t = T0 + DELAY + MS
    writer.request_cancel("alice", t, order_id=o.order_id)
    engine.on_event(trade(t + MS, "99.99", "0.3", "sell"))  # a trade-through before it bites
    engine.advance(t + DELAY)
    assert (o.status, o.filled_quantity) == (OrderStatus.CANCELLED, D("0.3"))
    assert o.order_id not in engine.resting
    assert writer.accounts.balance("alice", "USD").reserved == 0


def test_duplicate_prints_are_counted_once():
    engine, writer, o = rest_bid(qty="2")
    print_ = trade(T0 + DELAY + MS, "99.99", "0.5", "sell")
    engine.on_event(print_)
    engine.on_event(print_)
    assert o.filled_quantity == D("0.5")


def test_engine_settings_from_env():
    s = EngineSettings.from_env(
        {
            "EXCHANGE_MATCH_WINDOW_MS": "150",
            "EXCHANGE_DEPLETION_MAX_AGE_SECONDS": "3",
            "EXCHANGE_CANCEL_ATTRIBUTION": "pessimistic",
            "EXCHANGE_MATCH_WINDOW_MS_ETH_USD": "500",
            "EXCHANGE_BOOK_CROSS": "fill",
        }
    )
    assert (s.match_window_ns, s.depletion_max_age_ns) == (150 * MS, 3_000_000_000)
    assert (s.window_ns("ETH-USD"), s.window_ns("BTC-USD")) == (500 * MS, 150 * MS)
    assert EngineSettings().window_ns("SOL-USD") == 300 * MS
    assert s.cancel_attribution is CancelAttribution.PESSIMISTIC
    assert (s.book_cross, EngineSettings().book_cross) == (BookCross.FILL, BookCross.PRINT)
    assert EngineSettings.from_env({}) == EngineSettings()


def test_an_order_reaching_the_engine_after_its_effective_time_is_refused():
    engine, writer, _ = started()
    engine.advance(T0 + 10 * DELAY)  # the engine is already past T0 + DELAY
    o = order(writer, engine, "late", "buy", "limit", "1", "99.00", t=T0)
    assert o.status is OrderStatus.REJECTED and o.order_id not in engine.pending
    assert engine.stats.counts["rejected_on_effect:arrived_after_effective"] == 1
    assert writer.accounts.balance("alice", "USD").reserved == 0


def test_orders_for_another_pair_are_refused():
    engine, writer, _ = started()
    payload = {
        "client_order_id": "x",
        "product_id": "ETH-USD",
        "side": "buy",
        "type": "limit",
        "quantity": "1",
        "limit_price": "20.00",
    }
    writer.accounts.deposit("alice", "USD", D("100"))
    other = writer.submit("alice", payload, T0)
    with pytest.raises(ValueError):
        engine.submit(other)
