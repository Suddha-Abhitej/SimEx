"""Gemini public market-data adapter: the live venue (ADR 0010, ADR 0011).

One connection to wss://ws.gemini.com?snapshot=-1 carries {symbol}@depth@100ms and
{symbol}@trade for every pair. What the live probe (tools/gemini_probe.py) established:

- A snapshot is a depthUpdate with U == u and the full book, sent after each SUBSCRIBE to a
  depth stream (also on a live connection). Ordinary diffs never had U == u.
- U and u come from one counter shared by every market on the exchange, so a jump in them is
  not a count of this product's lost updates.
- Each diff starts where the previous one ended: U == previous u. The first diff after a
  snapshot straddles it: U <= L < u, where L is the snapshot's u.
- Trade IDs are exchange-wide and not always in order within a symbol: lost trades can't be
  detected, and consumers dedupe by trade ID without assuming order.

Gap rule, per product after a snapshot: U > the previous u (or, for the first diff, U > L)
means updates were missed, and only that product's book is resynced, by resubscribing its depth
stream. Anything else the probe never saw (overlaps, stale diffs, U == u with no SUBSCRIBE
pending) is protocol drift: counted and logged as an error, never silently accepted.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass

from .venue import LEVEL2, TRADES, Anomaly, Item, Parsed

DEFAULT_URL = "wss://ws.gemini.com?snapshot=-1"


def venue_symbol(product_id: str) -> str:
    """ "BTC-USD" -> "BTCUSD"."""
    return product_id.replace("-", "").upper()


def _int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


@dataclass
class _Book:
    """Per-product chain state for the gap rule."""

    pending: bool = False  # a SUBSCRIBE to the depth stream awaits its snapshot
    last_u: int | None = None  # None: no baseline, nothing to check until a snapshot
    fresh: bool = False  # the next diff is the first after the snapshot
    dropped: bool = False  # the fault injector dropped one of this product's diffs


class GeminiAdapter:
    venue = "gemini"
    default_url = DEFAULT_URL
    channels = (LEVEL2, TRADES)
    gap_channels = (LEVEL2,)
    supports_audit = True

    def __init__(self, pairs: tuple[str, ...]) -> None:
        self.pairs = pairs
        self.products = {venue_symbol(p): p for p in pairs}
        self._ids = itertools.count(1)
        self._subscribe_ids: set[int] = set()
        self._ping_id: int | None = None
        self._audit_ids: dict[int, str] = {}  # depth request id -> product
        self.books: dict[str, _Book] = {}
        self.reset()

    # Requests ----------------------------------------------------------------------------------

    def _request(self, method: str, params: list[str] | dict | None = None) -> tuple[int, str]:
        request_id = next(self._ids)
        frame: dict = {"method": method, "id": request_id}
        if params is not None:
            frame["params"] = params
        return request_id, json.dumps(frame)

    @staticmethod
    def depth_stream(product_id: str) -> str:
        return f"{venue_symbol(product_id).lower()}@depth@100ms"

    @staticmethod
    def trade_stream(product_id: str) -> str:
        return f"{venue_symbol(product_id).lower()}@trade"

    def reset(self) -> None:
        self.books = {p: _Book() for p in self.pairs}
        self._subscribe_ids.clear()
        self._ping_id = None
        self._audit_ids.clear()

    def connect_frames(self) -> list[str]:
        streams = [self.depth_stream(p) for p in self.pairs]
        streams += [self.trade_stream(p) for p in self.pairs]
        request_id, frame = self._request("SUBSCRIBE", streams)
        self._subscribe_ids.add(request_id)
        for book in self.books.values():
            book.pending = True
        return [frame]

    def resync_frames(self, products: tuple[str, ...]) -> list[str]:
        """Resubscribe these depth streams: with ?snapshot=-1 each SUBSCRIBE sends a snapshot."""
        streams = [self.depth_stream(p) for p in products]
        for product in products:
            book = self.books[product]
            book.pending, book.last_u, book.fresh = True, None, False
        return [self._request("UNSUBSCRIBE", streams)[1], self._request("SUBSCRIBE", streams)[1]]

    def ping_frame(self) -> str | None:
        self._ping_id, frame = self._request("ping")
        return frame

    def audit_frame(self, product: str) -> str | None:
        """The one-shot depth request: top 100 levels a side at lastUpdateId, which the live probe
        measured as equal to the stream's latest u (ADR 0011, ADR 0012)."""
        request_id, frame = self._request("depth", {"symbol": venue_symbol(product).lower()})
        self._audit_ids[request_id] = product
        return frame

    def note_dropped(self, message: dict) -> None:
        product = self.products.get(str(message.get("s", "")).upper())
        if product is not None and "U" in message:
            self.books[product].dropped = True

    # Messages ----------------------------------------------------------------------------------

    def parse(self, message: dict) -> Parsed:
        if "id" in message and ("status" in message or "result" in message or "error" in message):
            return self._response(message)
        parsed = Parsed(exchange_ts_ns=_int(message.get("E")))  # E is epoch nanoseconds
        product = self.products.get(str(message.get("s", "")).upper())
        if product is None:
            return parsed  # not market data for our pairs
        if "U" in message and "u" in message:
            self._depth(product, message, parsed)
        elif "t" in message and "p" in message and "q" in message:
            parsed.items.append(Item(TRADES, product))
        return parsed

    def _response(self, message: dict) -> Parsed:
        status = message.get("status")
        request_id = message.get("id")
        if message.get("error") or (isinstance(status, int) and status >= 400):
            self._audit_ids.pop(request_id, None)
            return Parsed(error=json.dumps(message))
        if request_id == self._ping_id:
            self._ping_id = None
            return Parsed(pong=True)
        if (product := self._audit_ids.pop(request_id, None)) is not None:
            result = message.get("result")
            update_id = _int(result.get("lastUpdateId")) if isinstance(result, dict) else None
            if update_id is None:
                return Parsed(error=f"depth response without lastUpdateId: {json.dumps(message)}")
            return Parsed(audit=(product, update_id))
        return Parsed(subscribed=request_id in self._subscribe_ids)

    def _depth(self, product: str, message: dict, parsed: Parsed) -> None:
        first, last = _int(message.get("U")), _int(message.get("u"))
        book = self.books[product]
        if first is None or last is None or first > last:
            parsed.anomalies.append(self._drift("malformed_update_ids", product))
            parsed.items.append(Item(LEVEL2, product))
            return
        item = Item(LEVEL2, product, update_ids=(first, last))

        if first == last and book.pending:
            book.pending, book.last_u, book.fresh, book.dropped = False, last, True, False
            parsed.items.append(Item(LEVEL2, product, update_ids=(first, last), snapshot=True))
            return
        parsed.items.append(item)
        if first == last:
            # Never seen without a SUBSCRIBE pending: not trusted as a snapshot.
            parsed.anomalies.append(self._drift("unexpected_snapshot", product))
        if book.last_u is None:
            return  # waiting for a snapshot: nothing to chain to

        previous = book.last_u
        if book.fresh:
            if last <= previous:
                # Sent before the snapshot was taken; the next diff should straddle it.
                parsed.anomalies.append(self._drift("stale_after_snapshot", product, alarm=False))
                return
            book.fresh = False
            if first <= previous:
                book.last_u, book.dropped = last, False  # U <= L < u: straddles the snapshot
                return
        elif first == previous:
            book.last_u, book.dropped = last, False
            return
        elif first < previous:
            if last > previous:
                parsed.anomalies.append(self._drift("overlap", product))  # values are absolute
                book.last_u = last
            else:
                parsed.anomalies.append(self._drift("stale", product))
            return

        # first > previous: updates were missed. Stop checking until the resync's snapshot.
        parsed.anomalies.append(
            Anomaly(
                kind="gap",
                products=(product,),
                reason="update_id_gap",
                expected=previous,
                received=first,
                missing=None,  # IDs are exchange-wide: the jump isn't a count for this product
                id_jump=first - previous,
                injected=book.dropped,
            )
        )
        book.last_u, book.fresh, book.dropped = None, False, False

    @staticmethod
    def _drift(kind: str, product: str, alarm: bool = True) -> Anomaly:
        return Anomaly(kind=kind, products=(product,), alarm=alarm)
