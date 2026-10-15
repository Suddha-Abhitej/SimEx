"""The order book library (ADR 0012): unit tests and Hypothesis properties, synthetic data only."""

from decimal import Decimal as D

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from simex.book import BookNotReady, BookState, OrderBook, Outcome
from simex.marketdata import BookUpdate, ResyncMarker, Trade

VENUE, PRODUCT = "gemini", "BTC-USD"
_ids = iter(range(1, 10**9))


def update(bids=(), asks=(), snapshot=False, awaiting=False, product=PRODUCT) -> BookUpdate:
    n = next(_ids)
    return BookUpdate(
        venue=VENUE,
        product_id=product,
        snapshot=snapshot,
        bids=tuple((D(p), D(q)) for p, q in bids),
        asks=tuple((D(p), D(q)) for p, q in asks),
        update_ids=(n, n) if snapshot else (n - 1, n),
        awaiting_snapshot=awaiting,
        exchange_ts_ns=1_000 + n,
        received_at_ns=2_000 + n,
    )


def marker() -> ResyncMarker:
    return ResyncMarker(VENUE, PRODUCT, "r1", "update_id_gap", 3_000)


def snapshot_book() -> OrderBook:
    book = OrderBook(VENUE, PRODUCT)
    book.apply(
        update(
            bids=[("100.00", "1"), ("99.50", "2"), ("99.00", "3")],
            asks=[("100.50", "0.5"), ("101.00", "1.5"), ("102.00", "4")],
            snapshot=True,
        )
    )
    return book


# Unit tests ------------------------------------------------------------------------------------


def test_empty_book_answers_nothing():
    book = OrderBook(VENUE, PRODUCT)
    assert book.state is BookState.EMPTY and not book.ready
    for query in (book.best_bid, book.best_ask, book.spread, book.mid):
        with pytest.raises(BookNotReady):
            query()
    with pytest.raises(BookNotReady):
        book.walk("buy", D(1))
    assert book.apply(update(bids=[("1", "1")])) is Outcome.SKIPPED_NOT_SYNCED


def test_snapshot_then_queries_are_exact_decimals():
    book = snapshot_book()
    assert book.state is BookState.SYNCED
    assert book.best_bid() == (D("100.00"), D("1"))
    assert book.best_ask() == (D("100.50"), D("0.5"))
    assert book.spread() == D("0.50")
    assert book.mid() == D("100.25")
    bids, asks = book.depth(2)
    assert bids == [(D("100.00"), D("1")), (D("99.50"), D("2"))]
    assert asks == [(D("100.50"), D("0.5")), (D("101.00"), D("1.5"))]
    assert book.level_counts() == (3, 3)
    assert book.last_update_id is not None and book.received_at_ns is not None


def test_update_sets_full_size_and_zero_removes():
    book = snapshot_book()
    assert book.apply(update(bids=[("100.00", "7"), ("99.50", "0")])) is Outcome.APPLIED
    assert book.depth(5)[0] == [(D("100.00"), D("7")), (D("99.00"), D("3"))]
    book.apply(update(asks=[("100.25", "2")]))  # a new best ask
    assert book.best_ask() == (D("100.25"), D("2"))
    book.apply(update(asks=[("123.45", "0")]))  # removing a level that isn't there is a no-op
    assert book.level_counts() == (2, 4)


def test_a_snapshot_replaces_everything():
    book = snapshot_book()
    book.apply(update(bids=[("50", "1")], asks=[("60", "1")], snapshot=True))
    assert book.depth(10) == ([(D("50"), D("1"))], [(D("60"), D("1"))])


def test_marker_resyncing_snapshot_synced():
    book = snapshot_book()
    assert book.apply(marker()) is Outcome.RESYNC
    assert book.state is BookState.RESYNCING and book.level_counts() == (0, 0)
    with pytest.raises(BookNotReady):
        book.best_bid()
    assert book.apply(update(bids=[("100", "1")])) is Outcome.SKIPPED_NOT_SYNCED
    assert book.apply(update(bids=[("90", "1")], asks=[("91", "1")], snapshot=True)) is (
        Outcome.SNAPSHOT
    )
    assert book.state is BookState.SYNCED and book.best_bid() == (D("90"), D("1"))


def test_awaiting_snapshot_updates_are_skipped():
    book = snapshot_book()
    before = book.depth(10)
    assert book.apply(update(bids=[("100.00", "9")], awaiting=True)) is Outcome.SKIPPED_AWAITING
    assert book.depth(10) == before


def test_crossed_after_a_whole_message_drops_the_book():
    book = snapshot_book()
    # Within one message the bid moves up past the old ask, and the ask moves up too: only the
    # end state counts, and it is not crossed.
    assert book.apply(update(bids=[("100.75", "1")], asks=[("100.50", "0"), ("100.80", "1")])) is (
        Outcome.APPLIED
    )
    assert book.crossed == 0
    assert book.apply(update(bids=[("101.50", "1")])) is Outcome.CROSSED
    assert book.state is BookState.RESYNCING and book.crossed == 1
    with pytest.raises(BookNotReady):
        book.mid()


def test_crossed_snapshot_is_dropped_too():
    book = OrderBook(VENUE, PRODUCT)
    assert book.apply(update(bids=[("10", "1")], asks=[("9", "1")], snapshot=True)) is (
        Outcome.CROSSED
    )
    assert book.state is BookState.RESYNCING


def test_one_sided_book_is_synced_with_no_spread():
    book = OrderBook(VENUE, PRODUCT)
    book.apply(update(bids=[("10", "1")], snapshot=True))
    assert book.best_ask() is None and book.spread() is None and book.mid() is None


def test_walk_the_book():
    book = snapshot_book()
    buy = book.walk("buy", D("1"))  # 0.5 @ 100.50 + 0.5 @ 101.00
    assert (buy.filled, buy.notional, buy.levels, buy.complete) == (D("1"), D("100.75"), 2, True)
    assert buy.average_price == D("100.75")
    sell = book.walk("sell", D("3.5"))  # 1 @ 100 + 2 @ 99.50 + 0.5 @ 99
    assert sell.notional == D("100") + D("199.00") + D("49.5")
    assert sell.average_price == sell.notional / D("3.5") and sell.levels == 3
    too_big = book.walk("buy", D("10"))  # only 6 on the asks
    assert too_big.filled == D("6") and not too_big.complete
    assert book.level_counts() == (3, 3)  # walking doesn't change the book
    with pytest.raises(ValueError):
        book.walk("buy", D("0"))
    with pytest.raises(ValueError):
        book.walk("hold", D("1"))


def test_events_for_another_product_are_refused_and_trades_ignored():
    book = snapshot_book()
    with pytest.raises(ValueError):
        book.apply(update(bids=[("1", "1")], product="ETH-USD"))
    trade = Trade(VENUE, PRODUCT, "1", D("100"), D("1"), "buy", None, 0)
    assert book.apply(trade) is Outcome.IGNORED


# Properties against a naive model --------------------------------------------------------------

PRICES = [D(f"{100 + i * 0.25:.2f}") for i in range(12)]  # 100.00 .. 102.75
SIZES = [D("0"), D("0.5"), D("1"), D("2.25")]

levels = st.lists(st.tuples(st.sampled_from(PRICES), st.sampled_from(SIZES)), max_size=6)


@st.composite
def events(draw):
    kind = draw(st.sampled_from(["snapshot", "update", "update", "update", "marker"]))
    if kind == "marker":
        return ("marker",)
    return (kind, tuple(draw(levels)), tuple(draw(levels)), draw(st.booleans()))


def to_event(spec):
    if spec[0] == "marker":
        return marker()
    kind, bids, asks, awaiting = spec
    return update(
        bids=[(str(p), str(q)) for p, q in bids],
        asks=[(str(p), str(q)) for p, q in asks],
        snapshot=kind == "snapshot",
        awaiting=awaiting and kind == "update",
    )


class Model:
    """The rules, written as plainly as possible with dicts."""

    def __init__(self):
        self.state, self.bids, self.asks = "EMPTY", {}, {}

    def apply(self, spec):
        if spec[0] == "marker":
            self.state, self.bids, self.asks = "RESYNCING", {}, {}
            return
        kind, bids, asks, awaiting = spec
        if kind == "snapshot":
            self.bids, self.asks, self.state = {}, {}, "SYNCED"
        elif awaiting or self.state != "SYNCED":
            return
        for side, updates in ((self.bids, bids), (self.asks, asks)):
            for price, size in updates:
                if size == 0:
                    side.pop(price, None)
                else:
                    side[price] = size
        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            self.state, self.bids, self.asks = "RESYNCING", {}, {}


@settings(max_examples=300, deadline=None)
@given(st.lists(events(), max_size=40))
def test_book_matches_the_model_after_every_event(specs):
    book, model = OrderBook(VENUE, PRODUCT), Model()
    for spec in specs:
        book.apply(to_event(spec))
        model.apply(spec)
        assert book.state == model.state
        assert dict(book.bids) == model.bids and dict(book.asks) == model.asks
        # Invariants: no zero-size level; never crossed while SYNCED; ordering is best first.
        assert all(q > 0 for q in [*book.bids.values(), *book.asks.values()])
        if book.state is BookState.SYNCED:
            assert not book.is_crossed()
            bids, asks = book.depth(100)
            assert [p for p, _ in bids] == sorted(model.bids, reverse=True)
            assert [p for p, _ in asks] == sorted(model.asks)
        else:
            with pytest.raises(BookNotReady):
                book.best_bid()


@settings(max_examples=150, deadline=None)
@given(st.lists(events(), max_size=40))
def test_replaying_the_same_events_gives_the_same_book(specs):
    first, second = OrderBook(VENUE, PRODUCT), OrderBook(VENUE, PRODUCT)
    stream = [to_event(spec) for spec in specs]
    for event in stream:
        first.apply(event)
    for event in stream:
        second.apply(event)
    assert (first.state, dict(first.bids), dict(first.asks)) == (
        second.state,
        dict(second.bids),
        dict(second.asks),
    )
    assert first.last_update_id == second.last_update_id and first.crossed == second.crossed


@settings(max_examples=150, deadline=None)
@given(st.lists(events(), min_size=1, max_size=30), st.sampled_from([D("0.5"), D("3"), D("50")]))
def test_walk_never_overfills_and_prices_are_monotonic(specs, quantity):
    book = OrderBook(VENUE, PRODUCT)
    for spec in specs:
        book.apply(to_event(spec))
    if not book.ready:
        return
    for side, levels in (("buy", book.asks), ("sell", book.bids)):
        fill = book.walk(side, quantity)
        assert fill.filled <= quantity and fill.filled <= sum(levels.values(), D(0))
        assert fill.complete == (fill.filled == quantity)
        if fill.filled:
            best = levels.keys()[0]
            # Average price is never better than the best level.
            assert (fill.average_price >= best) if side == "buy" else (fill.average_price <= best)
