"""The fill simulator for one pair: the live book, resting orders' queues, and fills (ADR 0014).

One PairEngine per pair, one task driving it (one writer per pair for books and matching). It
owns its OrderBook and the queue state; balances and order records belong to the account writer,
which it calls to report fills and state changes.

Time is the market-data clock. `on_event` first does whatever became due by the event's
received_at_ns (orders and cancels taking effect, expiries), against the book as it stood before
the event, then applies the event. `advance` does the same with no event, for quiet periods.

Fills:

- Taker (market orders, the crossing part of limits): walk the opposite side best first, within
  the band edge or the limit, using live size minus the depletion map. A market order's unfilled
  rest expires; if it stopped at the band while the book had more beyond it, the band bound.
- Maker, by cause, and only ever from the venue's own activity:
  (a) PRINT: a print at the order's price clears `ahead`, then fills it;
  (b) TRADE_THROUGH: a print beyond the order's price fills it;
  (c) BOOK_CROSS: the venue's opposite side reaching the order's price. By default
  (EXCHANGE_BOOK_CROSS=print) this does not fill: the order is marked crossed with nothing ahead
  of it, for good, and only prints at or through its price fill it. Under `fill` (the optimistic
  bracket) the crossing liquidity fills it at its price.
  Maker volume from one print never exceeds the print, shared across our orders by price, then
  arrival.
- Queues: `ahead` starts at the live size (back of the queue); prints reduce it; a shrink no
  print claims within W is a cancel, attributed proportionally (or by the pessimistic or
  optimistic bracket setting); growth joins behind. After a snapshot,
  ahead = min(ahead, new size).
- Paused while the book isn't SYNCED: orders taking effect are rejected and prints are ignored
  (and counted: before the first snapshot apart from during a resync).
"""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from simex.book.orderbook import BookState, OrderBook, Outcome
from simex.marketdata import BookAudit, BookUpdate, ResyncMarker, Trade, TradeDeduper

from .attribution import Cancel, ShrinkMatcher, book_side_hit
from .fees import Liquidity
from .orders import FillCause, Order, OrderStatus, OrderType, Side
from .writer import AccountWriter

ZERO = Decimal(0)
SECOND = 1_000_000_000


class BookCross(StrEnum):
    # The default: the crossing liquidity may have been post-only, and our order would have
    # changed how the maker quoted, so a cross fills nothing; it puts us first in the queue.
    PRINT = "print"
    FILL = "fill"  # the optimistic bracket: the crossing order would have traded with ours


class CancelAttribution(StrEnum):
    PROPORTIONAL = "proportional"  # the default: a cancel is equally likely anywhere in the queue
    PESSIMISTIC = "pessimistic"  # every cancel is behind us: fills come from prints alone
    OPTIMISTIC = "optimistic"  # every cancel is ahead of us


@dataclass(frozen=True)
class EngineSettings:
    # W: how long a shrink waits for the print that explains it. Measured from the recordings
    # by tools/measure_fill_window.py (300 ms for all three pairs); see ADR 0014's Measured
    # section. match_window_ns_by_pair overrides it for one pair.
    match_window_ns: int = 300_000_000
    match_window_ns_by_pair: Mapping[str, int] = field(default_factory=dict)
    # A depletion entry ("we took X at p") ends at the venue's next update of that level, at the
    # next snapshot, or after this long, whichever is first.
    depletion_max_age_ns: int = 10 * SECOND
    cancel_attribution: CancelAttribution = CancelAttribution.PROPORTIONAL
    book_cross: BookCross = BookCross.PRINT

    def window_ns(self, product_id: str) -> int:
        return self.match_window_ns_by_pair.get(product_id, self.match_window_ns)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> EngineSettings:
        env = os.environ if env is None else env
        d = cls()

        def get(name: str, default: object) -> str:
            value = env.get(name, "").strip()
            return value if value else str(default)

        def ms(value: str) -> int:
            return int(float(value) * 1e6)

        prefix = "EXCHANGE_MATCH_WINDOW_MS_"  # EXCHANGE_MATCH_WINDOW_MS_SOL_USD: one pair
        by_pair = {
            name.removeprefix(prefix).replace("_", "-"): ms(value)
            for name, value in env.items()
            if name.startswith(prefix) and value.strip()
        }
        return cls(
            match_window_ns=ms(get("EXCHANGE_MATCH_WINDOW_MS", d.match_window_ns / 1e6)),
            match_window_ns_by_pair=by_pair,
            depletion_max_age_ns=int(
                float(get("EXCHANGE_DEPLETION_MAX_AGE_SECONDS", d.depletion_max_age_ns / 1e9)) * 1e9
            ),
            cancel_attribution=CancelAttribution(
                get("EXCHANGE_CANCEL_ATTRIBUTION", d.cancel_attribution.value)
            ),
            book_cross=BookCross(get("EXCHANGE_BOOK_CROSS", d.book_cross.value)),
        )


@dataclass
class Depletion:
    quantity: Decimal
    since_ns: int


@dataclass
class Resting:
    order_id: str
    side: Side
    price: Decimal
    ahead: Decimal  # venue quantity in front of the order at its price
    seq: int  # arrival order among our orders at the same price
    crossed: bool = False  # the venue's other side reached our price: first in the queue for good


@dataclass
class EngineStats:
    counts: Counter = field(default_factory=Counter)
    quantities: Counter = field(default_factory=Counter)  # Decimal totals

    def add(self, name: str, quantity: Decimal | None = None) -> None:
        self.counts[name] += 1
        if quantity is not None:
            self.quantities[name] += quantity


def _book_side(side: Side) -> str:
    """The book side our resting order sits on."""
    return "bid" if side is Side.BUY else "ask"


class PairEngine:
    def __init__(
        self,
        product_id: str,
        writer: AccountWriter,
        settings: EngineSettings | None = None,
        venue: str = "gemini",
    ) -> None:
        self.product_id = product_id
        self.writer = writer
        self.settings = settings or EngineSettings()
        self.instrument = writer.instruments[product_id]
        self.book = OrderBook(venue, product_id)
        self.depletion: dict[tuple[str, Decimal], Depletion] = {}
        self.resting: dict[str, Resting] = {}
        self.pending: list[str] = []  # accepted orders waiting for their effective time
        self.matcher = ShrinkMatcher(self.settings.window_ns(product_id))
        self.dedupe = TradeDeduper()
        self.stats = EngineStats()
        self.now_ns = 0
        self._seq = 0

    # Orders in -------------------------------------------------------------------------------

    def submit(self, order: Order) -> None:
        """Hand over an order the account writer accepted (PENDING). Rejected ones are ignored."""
        if order.product_id != self.product_id:
            raise ValueError(f"{order.product_id} order sent to the {self.product_id} engine")
        if order.status is OrderStatus.PENDING and order.order_id not in self.pending:
            self.pending.append(order.order_id)

    # Time ------------------------------------------------------------------------------------

    def on_event(self, event: object) -> None:
        received = getattr(event, "received_at_ns", None)
        if received is not None:
            self.advance(received)
        if isinstance(event, BookUpdate):
            self._on_book(event)
        elif isinstance(event, Trade):
            self._on_print(event)
        elif isinstance(event, ResyncMarker | BookAudit):
            self.book.apply(event)

    def advance(self, now_ns: int) -> None:
        """Do what is due by now_ns, in time order. Each order and cancel takes effect at its
        own effective time, after the expiries due by then, against the book as it stands
        (every event received before that time, none after). So the result is the same
        however often advance is called: live calls it at each watermark, replay at each
        event."""
        target = max(self.now_ns, now_ns)
        while (item := self._next_due(target)) is not None:
            when, order, is_cancel = item
            when = max(when, self.now_ns)  # never before what has already been applied
            self._expire(when)
            self.now_ns = when
            if is_cancel:
                del self.resting[order.order_id]
                if not order.status.terminal:
                    self.writer.cancel_now(order.order_id, when)
            else:
                self.pending.remove(order.order_id)
                self._take_effect(order, when)
        self._expire(target)
        self.now_ns = target

    def _next_due(self, by: int) -> tuple[int, Order, bool] | None:
        """The earliest order or cancel due by `by`: orders first come, first served (the
        pending list is in arrival order), an order before a cancel at the same moment."""
        best = None
        for index, order_id in enumerate(self.pending):
            order = self.writer.orders[order_id]
            if order.effective_at_ns <= by:
                key = (order.effective_at_ns, 0, index, "")
                if best is None or key < best[0]:
                    best = (key, order, False)
        for order_id in self.resting:
            order = self.writer.orders[order_id]
            due = order.cancel_effective_at_ns
            if due is not None and due <= by:
                key = (due, 1, 0, order_id)
                if best is None or key < best[0]:
                    best = (key, order, True)
        return None if best is None else (best[0][0], best[1], best[2])

    def _expire(self, now: int) -> None:
        for key, entry in sorted(self.depletion.items()):
            if now - entry.since_ns > self.settings.depletion_max_age_ns:
                del self.depletion[key]
                self.stats.add("depletion_expired:age")
        for cancel in self.matcher.expire(now):
            self._attribute_cancel(cancel)

    # Taking effect ---------------------------------------------------------------------------

    def _take_effect(self, order: Order, now: int) -> None:
        if order.status.terminal:
            return
        decision = self.writer.gate.check(self.product_id, now)
        reason = None if decision.open else (decision.reason or "market_closed")
        if reason is None and self.book.state is not BookState.SYNCED:
            reason = "book_not_synced"
        if reason is not None:
            self.stats.add(f"rejected_on_effect:{reason}")
            self.writer.reject_on_effect(order.order_id, reason, now)
            return
        request = order.request
        if request.type is OrderType.MARKET:
            self.stats.add("market_orders")
            edge = self.writer.band_prices[order.order_id]
            if self._take(order, edge, now) and order.remaining > 0:
                self.stats.add("band_binds")
            if not order.status.terminal:
                self.writer.expire(order.order_id, now)
            return
        assert request.limit_price is not None
        self._take(order, request.limit_price, now)
        if order.status.terminal:
            return
        if order.status is OrderStatus.PENDING:
            self.writer.activate(order.order_id, now)
        if order.cancel_effective_at_ns is not None and order.cancel_effective_at_ns <= now:
            self.writer.cancel_now(order.order_id, now)
            return
        levels = self.book.bids if order.side is Side.BUY else self.book.asks
        self._seq += 1
        self.resting[order.order_id] = Resting(
            order.order_id,
            order.side,
            request.limit_price,
            levels.get(request.limit_price, ZERO),
            self._seq,
        )

    def _available(self, book_side: str, price: Decimal, size: Decimal) -> Decimal:
        used = self.depletion.get((book_side, price))
        return max(ZERO, size - used.quantity) if used else size

    def _deplete(self, book_side: str, price: Decimal, quantity: Decimal, now: int) -> None:
        entry = self.depletion.get((book_side, price))
        if entry is None:
            self.depletion[(book_side, price)] = Depletion(quantity, now)
        else:
            entry.quantity += quantity

    def _take(self, order: Order, edge: Decimal, now: int) -> bool:
        """Walk the opposite side as taker up to `edge`. Returns True if liquidity remained
        beyond the edge when the order stopped short (the band or limit bound)."""
        buy = order.side is Side.BUY
        book_side = "ask" if buy else "bid"
        levels = self.book.asks if buy else self.book.bids
        for price, size in list(levels.items()):
            if (buy and price > edge) or (not buy and price < edge):
                return order.remaining > 0 and any(
                    self._available(book_side, p, s) > 0
                    for p, s in levels.items()
                    if (p > edge if buy else p < edge)
                )
            if order.remaining <= 0:
                return False
            take = min(self._available(book_side, price, size), order.remaining)
            if take <= 0:
                continue
            self.writer.record_fill(
                order.order_id, price, take, Liquidity.TAKER, FillCause.TAKER, now
            )
            self.stats.add("taker_fills", take)
            self._deplete(book_side, price, take, now)
        return False

    # Market data -----------------------------------------------------------------------------

    def _on_book(self, update: BookUpdate) -> None:
        now = update.received_at_ns
        if update.snapshot:
            outcome = self.book.apply(update)
            for _ in self.depletion:
                self.stats.add("depletion_expired:snapshot")
            self.depletion.clear()
            self.matcher.clear()
            if outcome is Outcome.SNAPSHOT:
                for resting in self.resting.values():
                    levels = self.book.bids if resting.side is Side.BUY else self.book.asks
                    resting.ahead = min(resting.ahead, levels.get(resting.price, ZERO))
                self._cross(now)
            return
        if self.book.state is not BookState.SYNCED or update.awaiting_snapshot:
            self.book.apply(update)
            return
        before = {("bid", p): self.book.bids.get(p, ZERO) for p, _ in update.bids}
        before.update({("ask", p): self.book.asks.get(p, ZERO) for p, _ in update.asks})
        if self.book.apply(update) is not Outcome.APPLIED:
            return  # crossed: the book dropped to RESYNCING and fills pause
        for (book_side, price), old in sorted(before.items()):
            if self.depletion.pop((book_side, price), None) is not None:
                self.stats.add("depletion_expired:venue_update")
            levels = self.book.bids if book_side == "bid" else self.book.asks
            new = levels.get(price, ZERO)
            if new < old:
                self.matcher.add_shrink(book_side, price, old - new, old, now)
        self._cross(now)

    def _on_print(self, trade: Trade) -> None:
        if not self.dedupe.is_new(trade):
            return
        if self.book.state is BookState.EMPTY:
            self.stats.add("prints_before_first_snapshot", trade.size)
            return
        if self.book.state is not BookState.SYNCED:
            self.stats.add("prints_ignored_unsynced", trade.size)  # during a resync
            return
        if trade.aggressor not in ("buy", "sell"):
            self.stats.add("prints_no_aggressor", trade.size)
            return
        now = trade.received_at_ns
        book_side = book_side_hit(trade.aggressor)
        self.matcher.add_print(book_side, trade.price, trade.size, now)
        ours = Side.BUY if book_side == "bid" else Side.SELL
        remaining = trade.size
        # Better prices first (a trade-through reaches them before the print's price), then our
        # arrival order.
        candidates = sorted(
            (r for r in self.resting.values() if r.side is ours),
            key=lambda r: (-r.price if ours is Side.BUY else r.price, r.seq),
        )
        through = [
            r
            for r in candidates
            if (r.price > trade.price if ours is Side.BUY else r.price < trade.price)
        ]
        for resting in through:
            if remaining <= 0:
                break
            remaining -= self._maker_fill(resting, remaining, FillCause.TRADE_THROUGH, now)
        at_price = [r for r in candidates if r.price == trade.price]
        real_taken = ZERO
        for resting in at_price:
            if remaining <= 0:
                break
            gap = max(ZERO, resting.ahead - real_taken)
            take = min(remaining, gap)
            real_taken += take
            remaining -= take
            if remaining > 0:
                remaining -= self._maker_fill(resting, remaining, FillCause.PRINT, now)
        for resting in at_price:
            resting.ahead = max(ZERO, resting.ahead - real_taken)

    def _maker_fill(self, resting: Resting, most: Decimal, cause: FillCause, now: int) -> Decimal:
        order = self.writer.orders[resting.order_id]
        quantity = min(order.remaining, most)
        if quantity <= 0:
            return ZERO
        self.writer.record_fill(
            order.order_id, resting.price, quantity, Liquidity.MAKER, cause, now
        )
        self.stats.add(f"maker_fills:{cause.value}", quantity)
        if order.status.terminal:
            del self.resting[resting.order_id]
        return quantity

    def _cross(self, now: int) -> None:
        """(c): the venue's opposite side reached a resting order's price.

        print (default): nothing fills. The order is marked crossed with nothing ahead of it,
        for good (later orders at its price queue behind it), and prints at or through its
        price fill it. fill: the crossing liquidity fills it at its price, as if the arriving
        order had traded with ours, and is recorded in the depletion map."""
        if self.book.state is not BookState.SYNCED:
            return
        fill = self.settings.book_cross is BookCross.FILL
        for ours, book_side, levels in (
            (Side.BUY, "ask", self.book.asks),
            (Side.SELL, "bid", self.book.bids),
        ):
            orders = sorted(
                (r for r in self.resting.values() if r.side is ours),
                key=lambda r: (-r.price if ours is Side.BUY else r.price, r.seq),
            )
            for resting in orders:
                if not fill:
                    best = next(iter(levels), None)
                    if best is None or (
                        best > resting.price if ours is Side.BUY else best < resting.price
                    ):
                        break
                    if not resting.crossed:
                        resting.crossed = True
                        resting.ahead = ZERO
                        self.stats.add("crosses_marked")
                    continue
                for price, size in list(levels.items()):
                    crossing = (
                        price <= resting.price if ours is Side.BUY else price >= resting.price
                    )
                    if not crossing or resting.order_id not in self.resting:
                        break
                    available = self._available(book_side, price, size)
                    if available <= 0:
                        continue
                    filled = self._maker_fill(resting, available, FillCause.BOOK_CROSS, now)
                    self._deplete(book_side, price, filled, now)

    def _attribute_cancel(self, cancel: Cancel) -> None:
        ours = Side.BUY if cancel.side == "bid" else Side.SELL
        mode = self.settings.cancel_attribution
        self.stats.add(f"cancels:{mode.value}", cancel.quantity)
        for resting in self.resting.values():
            if resting.side is not ours or resting.price != cancel.price or resting.ahead <= 0:
                continue
            if mode is CancelAttribution.PESSIMISTIC:
                continue
            if mode is CancelAttribution.OPTIMISTIC:
                cut = cancel.quantity
            else:
                cut = cancel.quantity * resting.ahead / cancel.size_before
            resting.ahead = max(ZERO, resting.ahead - cut)
