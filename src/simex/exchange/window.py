"""Measuring W from recordings: how long after (or before) a level shrinks does its print arrive?

For every level2 shrink on a SYNCED book and every print (deduplicated, with an aggressor):

- the time distribution comes from an offline pass that sees the whole recording: each print is
  paired with the shrinks nearest in time at its side and price, before or after it, within 5 s;
  print time minus shrink time, weighted by quantity. (An online matcher can't see the future: a
  print that comes before its shrink could claim an older cancel instead.)
- for each candidate W, the engine's own ShrinkMatcher runs online exactly as the engine does,
  and reports how much print quantity found its shrink and how much shrink quantity was left
  unmatched (what the engine treats as cancels).

W is the smallest candidate that matches at least 99% of the print quantity the wide window
matches: past that, a longer W only misreads cancels as fills. If even the wide window explains
under 80% of print quantity, prints and shrinks don't line up (wrong data, a bug, or liquidity the
book never shows), and no W is chosen: the report says so instead. Counts and milliseconds only; no
prices (ADR 0010).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal

from simex.book.orderbook import BookState, OrderBook
from simex.marketdata import BookAudit, BookUpdate, ResyncMarker, Trade, TradeDeduper, decode

from .attribution import ShrinkMatcher, book_side_hit

MS = 1_000_000
WIDE_MS = 5000
CANDIDATES_MS = (25, 50, 100, 150, 200, 300, 500, 750, 1000, 2000)
TARGET = Decimal("0.99")
MIN_EXPLAINED = Decimal("0.80")


@dataclass
class PairWindow:
    product_id: str
    matchers: dict[int, ShrinkMatcher]  # window in ms -> matcher
    deltas: list[tuple[int, Decimal]] = field(default_factory=list)  # (print - shrink ns, qty)
    shrink_log: dict = field(default_factory=lambda: defaultdict(list))  # (side, price) -> events
    print_log: dict = field(default_factory=lambda: defaultdict(list))
    prints_skipped_unsynced: int = 0

    def matched_print_share(self, window_ms: int) -> Decimal:
        q = self.matchers[window_ms].quantities
        return q["matched"] / q["print"] if q["print"] else Decimal(0)

    def unmatched_shrink_share(self, window_ms: int) -> Decimal:
        q = self.matchers[window_ms].quantities
        return q["cancel"] / q["shrink"] if q["shrink"] else Decimal(0)

    def unmatched_shrink_events(self, window_ms: int) -> Decimal:
        c = self.matchers[window_ms].counts
        return Decimal(c["shrink_unmatched"]) / c["shrink"] if c["shrink"] else Decimal(0)

    def chosen_ms(self) -> int | None:
        ceiling = self.matched_print_share(WIDE_MS)
        if ceiling < MIN_EXPLAINED:
            return None
        for window in CANDIDATES_MS:
            if self.matched_print_share(window) >= ceiling * TARGET:
                return window
        return WIDE_MS

    def percentile_ms(self, q: float, absolute: bool = False) -> float | None:
        """Quantity-weighted percentile of print time minus shrink time, in ms."""
        if not self.deltas:
            return None
        values = sorted(((abs(d) if absolute else d), w) for d, w in self.deltas)
        total = sum(w for _, w in values)
        target = Decimal(str(q)) * total
        running = Decimal(0)
        for value, weight in values:
            running += weight
            if running >= target:
                return value / MS
        return values[-1][0] / MS


def measure(values: Iterable[bytes], windows_ms=CANDIDATES_MS) -> dict[str, PairWindow]:
    """Run the matcher over recorded level2 and trade envelopes, in the order recorded."""
    windows = sorted({*windows_ms, WIDE_MS})
    pairs: dict[str, PairWindow] = {}
    books: dict[str, OrderBook] = {}
    dedupe = TradeDeduper()

    def pair(product: str, venue: str) -> PairWindow:
        if product not in pairs:
            books[product] = OrderBook(venue, product)
            matchers = {w: ShrinkMatcher(w * MS) for w in windows}
            pairs[product] = PairWindow(product, matchers)
        return pairs[product]

    last_ns: dict[str, int] = defaultdict(int)
    for value in values:
        for event in decode(value):
            result = pair(event.product_id, event.venue)
            book = books[event.product_id]
            now = getattr(event, "received_at_ns", 0)
            # Expiring every 100 ms is enough: a match itself checks |dt| <= W exactly.
            if now - last_ns[event.product_id] >= 100 * MS:
                for matcher in result.matchers.values():
                    matcher.expire(now)
            last_ns[event.product_id] = max(last_ns[event.product_id], now)
            if isinstance(event, BookUpdate):
                if event.snapshot:
                    book.apply(event)
                    for matcher in result.matchers.values():
                        matcher.clear()
                    continue
                if book.state is not BookState.SYNCED or event.awaiting_snapshot:
                    book.apply(event)
                    continue
                before = {("bid", p): book.bids.get(p, Decimal(0)) for p, _ in event.bids}
                before.update({("ask", p): book.asks.get(p, Decimal(0)) for p, _ in event.asks})
                book.apply(event)
                for (side, price), old in sorted(before.items()):
                    levels = book.bids if side == "bid" else book.asks
                    new = levels.get(price, Decimal(0))
                    if new < old:
                        result.shrink_log[(side, price)].append([now, old - new])
                        for matcher in result.matchers.values():
                            matcher.add_shrink(side, price, old - new, old, now)
            elif isinstance(event, Trade):
                if not dedupe.is_new(event) or event.aggressor not in ("buy", "sell"):
                    continue
                if book.state is not BookState.SYNCED:
                    result.prints_skipped_unsynced += 1
                    continue
                side = book_side_hit(event.aggressor)
                result.print_log[(side, event.price)].append((now, event.size))
                for matcher in result.matchers.values():
                    matcher.add_print(side, event.price, event.size, now)
            elif isinstance(event, ResyncMarker | BookAudit):
                book.apply(event)
    for product, result in pairs.items():
        for matcher in result.matchers.values():
            matcher.expire(last_ns[product] + 10 * WIDE_MS * MS)
        result.deltas = _nearest_pairs(result.shrink_log, result.print_log)
        result.shrink_log.clear()
        result.print_log.clear()
    return pairs


def _nearest_pairs(shrinks: dict, prints: dict) -> list[tuple[int, Decimal]]:
    """Offline: pair each print with the unclaimed shrinks nearest in time at its side and price
    (either direction, within the wide window). Returns (print - shrink ns, quantity) pairs."""
    deltas = []
    wide = WIDE_MS * MS
    for key, key_prints in prints.items():
        waiting = shrinks.get(key, [])
        for ts, size in sorted(key_prints):
            remaining = size
            candidates = sorted(
                (s for s in waiting if s[1] > 0 and abs(ts - s[0]) <= wide),
                key=lambda s: abs(ts - s[0]),
            )
            for shrink in candidates:
                if remaining <= 0:
                    break
                take = min(shrink[1], remaining)
                shrink[1] -= take
                remaining -= take
                deltas.append((ts - shrink[0], take))
    return deltas


def format_report(pairs: dict[str, PairWindow]) -> str:
    lines = []
    chosen = []
    for product, result in sorted(pairs.items()):
        wide = result.matchers[WIDE_MS]
        lines.append(
            f"{product}: {wide.counts['shrink']} shrinks, print quantity matched within "
            f"{WIDE_MS} ms: {result.matched_print_share(WIDE_MS):.1%}; prints skipped (book not "
            f"synced) {result.prints_skipped_unsynced}"
        )
        if result.deltas:
            pct = ", ".join(
                f"p{int(q * 100)} {result.percentile_ms(q):+.0f}" for q in (0.01, 0.5, 0.99)
            )
            abs_pct = ", ".join(
                f"p{int(q * 100)} {result.percentile_ms(q, absolute=True):.0f}"
                for q in (0.5, 0.9, 0.99)
            )
            lines.append(f"  print minus shrink, ms: {pct}; |difference|: {abs_pct}")
        lines.append("  W ms   prints matched   shrink qty unmatched   shrinks fully unmatched")
        for window in sorted(result.matchers):
            lines.append(
                f"  {window:>5}  {result.matched_print_share(window):>14.1%}   "
                f"{result.unmatched_shrink_share(window):>20.1%}   "
                f"{result.unmatched_shrink_events(window):>22.1%}"
            )
        w = result.chosen_ms()
        chosen.append(w)
        if w is not None:
            lines.append(f"  -> W for {product}: {w} ms")
        else:
            lines.append(
                f"  -> no W for {product}: within {WIDE_MS} ms only "
                f"{result.matched_print_share(WIDE_MS):.1%} of print quantity matches a shrink "
                f"(needs {MIN_EXPLAINED:.0%}); look at the data before trusting any W"
            )
        lines.append("")
    known = [w for w in chosen if w is not None]
    if known:
        lines.append(f"W (largest over pairs): {max(known)} ms")
    return "\n".join(lines)
