"""Telling fills from cancels: matching level shrinks to prints (ADR 0014).

A Gemini fill shows twice: a print on md.trades and a smaller level on md.level2, and the two
streams arrive at different moments. A shrink with no print to claim it is a cancel. The matcher
holds both sides for a window W: a shrink waits for a print at its side and price, a print waits
for the shrink it caused, and whatever is left of a shrink after W is reported as a cancel.

The same class runs in the fill engine and in tools/measure_fill_window.py, which sets W from the
recordings, so the measurement measures exactly the code that uses it.

Sides here are book sides: a sell aggressor's print hits the bids, a buy aggressor's the asks.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

ZERO = Decimal(0)
Key = tuple[str, Decimal]  # (book side "bid" or "ask", price)


def book_side_hit(aggressor: str) -> str:
    """The book side a print took liquidity from."""
    if aggressor == "sell":
        return "bid"
    if aggressor == "buy":
        return "ask"
    raise ValueError(f"unknown aggressor {aggressor!r}")


@dataclass
class _Pending:
    ts_ns: int
    remaining: Decimal
    size_before: Decimal  # shrinks only: the level's size just before it shrank
    original: Decimal = ZERO


@dataclass(frozen=True)
class Cancel:
    """A shrink no print claimed within W."""

    side: str
    price: Decimal
    quantity: Decimal
    size_before: Decimal
    ts_ns: int


class ShrinkMatcher:
    def __init__(
        self,
        window_ns: int,
        on_match: Callable[[str, Decimal, Decimal, int], None] | None = None,
    ) -> None:
        """on_match(side, price, quantity, print_ts - shrink_ts) is called for every matched
        part; the measurement uses it to build the time distribution."""
        self.window_ns = window_ns
        self.on_match = on_match
        self.shrinks: dict[Key, list[_Pending]] = defaultdict(list)
        self.prints: dict[Key, list[_Pending]] = defaultdict(list)
        self.quantities: Counter = Counter()  # Decimal totals by name
        self.counts: Counter = Counter()  # events by name

    def _match(self, waiting: list[_Pending], quantity: Decimal, ts_ns: int) -> list:
        """Claim up to `quantity` from waiting entries within the window: an exact size first,
        then the nearest in time. Returns (amount, entry) pairs."""
        claimed = []
        candidates = [e for e in waiting if abs(ts_ns - e.ts_ns) <= self.window_ns]
        candidates.sort(key=lambda e: (e.remaining != quantity, abs(ts_ns - e.ts_ns)))
        for entry in candidates:
            if quantity <= 0:
                break
            take = min(entry.remaining, quantity)
            entry.remaining -= take
            quantity -= take
            claimed.append((take, entry))
        waiting[:] = [e for e in waiting if e.remaining > 0]
        return claimed

    def add_shrink(
        self, side: str, price: Decimal, quantity: Decimal, size_before: Decimal, ts_ns: int
    ) -> Decimal:
        """A level shrank by `quantity`. Returns how much of it prints already claimed."""
        self.quantities["shrink"] += quantity
        self.counts["shrink"] += 1
        claimed = self._match(self.prints[(side, price)], quantity, ts_ns)
        matched = sum((amount for amount, _ in claimed), ZERO)
        for amount, entry in claimed:
            self._matched(side, price, amount, entry.ts_ns - ts_ns)
        if quantity - matched > 0:
            self.shrinks[(side, price)].append(
                _Pending(ts_ns, quantity - matched, size_before, quantity)
            )
        return matched

    def add_print(self, side: str, price: Decimal, quantity: Decimal, ts_ns: int) -> Decimal:
        """A print took `quantity` at the level. Returns how much of a waiting shrink it
        claimed; the rest waits for the shrink to arrive."""
        self.quantities["print"] += quantity
        claimed = self._match(self.shrinks[(side, price)], quantity, ts_ns)
        matched = sum((amount for amount, _ in claimed), ZERO)
        for amount, entry in claimed:
            self._matched(side, price, amount, ts_ns - entry.ts_ns)
        if quantity - matched > 0:
            self.prints[(side, price)].append(_Pending(ts_ns, quantity - matched, ZERO))
        return matched

    def _matched(self, side: str, price: Decimal, amount: Decimal, dt_ns: int) -> None:
        self.quantities["matched"] += amount
        if self.on_match is not None:
            self.on_match(side, price, amount, dt_ns)

    def expire(self, now_ns: int) -> list[Cancel]:
        """Shrinks older than W become cancels; prints older than W stop waiting."""
        cancels = []
        for (side, price), waiting in sorted(self.shrinks.items()):
            keep = []
            for entry in waiting:
                if now_ns - entry.ts_ns > self.window_ns:
                    self.quantities["cancel"] += entry.remaining
                    if entry.remaining == entry.original:
                        self.counts["shrink_unmatched"] += 1
                    cancels.append(
                        Cancel(side, price, entry.remaining, entry.size_before, entry.ts_ns)
                    )
                else:
                    keep.append(entry)
            waiting[:] = keep
        for waiting in self.prints.values():
            for entry in waiting:
                if now_ns - entry.ts_ns > self.window_ns:
                    self.quantities["print_unmatched"] += entry.remaining
            waiting[:] = [e for e in waiting if now_ns - e.ts_ns <= self.window_ns]
        self.shrinks = defaultdict(list, {k: v for k, v in self.shrinks.items() if v})
        self.prints = defaultdict(list, {k: v for k, v in self.prints.items() if v})
        return sorted(cancels, key=lambda c: (c.ts_ns, c.side, c.price))

    def clear(self) -> None:
        """The book was replaced (snapshot): nothing waiting can be matched any more."""
        for waiting in self.shrinks.values():
            self.quantities["shrink_cleared"] += sum((e.remaining for e in waiting), ZERO)
        for waiting in self.prints.values():
            self.quantities["print_cleared"] += sum((e.remaining for e in waiting), ZERO)
        self.shrinks.clear()
        self.prints.clear()
