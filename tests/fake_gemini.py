"""A scripted Gemini public WebSocket server for tests, with hand-written data only (ADR 0010).

It reproduces what the live probe measured, not real market data:
- one SUBSCRIBE carries several streams; responses are {"id": N, "status": 200}
- with ?snapshot=-1, every SUBSCRIBE to {symbol}@depth@100ms sends a snapshot: a depthUpdate
  with U == u and the whole book
- update IDs come from one exchange-wide counter; each diff starts at the previous diff's u
  (U == previous u), and the first diff after a snapshot straddles it (U < L < u)
- trades carry no "e" field and their IDs are exchange-wide
- {"method": "ping"} is answered with {"id": N, "status": 200}
- {"method": "depth", "params": {"symbol": "btcusd"}} is answered with the server's own book at
  lastUpdateId == the last u it sent for the symbol (ADR 0012). depth_mode can make it answer
  ahead of the stream ("early": the response, then the diff it matches), behind it ("late": a
  diff, then a response for the u before it) or mid-batch as live Gemini does about a third of
  the time ("inside": a response at an ID strictly inside the next diff, U < L < u);
  corrupt_depth corrupts the next N responses.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal

PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
SYMBOLS = {"BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD", "SOL-USD": "SOLUSD"}
SYMBOLS_BY_VENUE = {symbol: product for product, symbol in SYMBOLS.items()}

# Hand-written books: a few levels each side, prices and sizes as strings.
BOOKS = {
    "BTCUSD": ([["100.00", "1.5"], ["99.50", "2"]], [["100.50", "0.75"], ["101.00", "3"]]),
    "ETHUSD": ([["20.00", "10"], ["19.90", "4.25"]], [["20.10", "6"], ["20.20", "1"]]),
    "SOLUSD": ([["3.000", "100"], ["2.990", "50"]], [["3.010", "80"], ["3.020", "20"]]),
}


class FakeGemini:
    def __init__(self, snapshot_on_resubscribe: bool = True) -> None:
        self.snapshot_on_resubscribe = snapshot_on_resubscribe
        self.connections = 0
        self.requests: list[tuple[int, float, dict]] = []  # (connection, monotonic, request)
        self.snapshot_query = None
        self._ws = None
        self._ids = 1_000_000  # the exchange-wide update-ID counter
        self._last_u: dict[str, int] = {}
        self._trade_id = 5_000
        self.subscribed: set[str] = set()
        self.depth_subscribes: dict[str, int] = {}  # symbol -> depth SUBSCRIBEs on this connection
        self.books: dict[str, tuple[dict[str, str], dict[str, str]]] = {}  # symbol -> bids, asks
        self.sent_u: dict[str, int] = {}  # symbol -> u of the last depth message sent
        self.depth_mode = "now"  # "now", "early" or "late"
        self.corrupt_depth = 0  # corrupt this many of the next depth responses

    async def handler(self, ws) -> None:
        self.connections += 1
        connection = self.connections
        self._ws = ws
        self.snapshot_query = "snapshot=-1" in ws.request.path
        self.subscribed = set()
        self._last_u = {}
        self.depth_subscribes = {}  # a new connection always gets its snapshots
        self.sent_u = {}
        async for raw in ws:
            request = json.loads(raw)
            self.requests.append((connection, time.monotonic(), request))
            method, request_id = request["method"], request["id"]
            streams = request.get("params") or []
            if method == "depth":
                await self.depth(request_id, request["params"]["symbol"].upper())
                continue
            if method == "UNSUBSCRIBE":
                self.subscribed.difference_update(streams)
            elif method == "SUBSCRIBE":
                new = [s for s in streams if s not in self.subscribed]
                self.subscribed.update(streams)
            await ws.send(json.dumps({"id": request_id, "status": 200}))
            if method == "SUBSCRIBE" and self.snapshot_query:
                for stream in new:
                    if stream.endswith("@depth@100ms"):
                        symbol = stream.split("@")[0].upper()
                        self.depth_subscribes[symbol] = self.depth_subscribes.get(symbol, 0) + 1
                        if self.depth_subscribes[symbol] == 1 or self.snapshot_on_resubscribe:
                            await self.snapshot(symbol)

    def _advance(self, step: int = 37) -> int:
        """Other markets on the exchange use IDs too, so the counter moves by more than one."""
        self._ids += step
        return self._ids

    async def snapshot(self, symbol: str) -> None:
        bids, asks = BOOKS[symbol]
        last = self._advance()
        # The next diff starts a little before the snapshot's ID and ends after it.
        self._last_u[symbol] = last - 5
        await self._send(
            {"E": time.time_ns(), "U": last, "u": last, "a": asks, "b": bids, "s": symbol}
        )

    async def update(self, product_id: str, skip: bool = False, bids=None, asks=None) -> None:
        """A diff for a product; skip=True loses the diff before it (a gap on this product).
        By default the diff removes the 99.50 bid (a no-op once it's gone)."""
        await self._send(self._diff(SYMBOLS[product_id], skip, bids, asks))

    def _diff(self, symbol: str, skip: bool = False, bids=None, asks=None) -> dict:
        if skip:
            self._last_u[symbol] = self._advance()
        first = self._last_u.get(symbol, self._ids)
        last = self._advance()
        self._last_u[symbol] = last
        return {
            "E": time.time_ns(),
            "U": first,
            "u": last,
            "a": asks if asks is not None else [],
            "b": bids if bids is not None else [["99.50", "0"]],
            "s": symbol,
        }

    async def depth(self, request_id: int, symbol: str) -> None:
        if self.depth_mode == "early":
            # The venue answered at an L whose diff the client hasn't received yet.
            diff = self._diff(symbol, bids=self.bigger_best_bid(symbol))
            self._apply(diff)
            await self._depth_response(request_id, symbol, diff["u"])
            await self._send(diff)
        elif self.depth_mode == "inside":
            # The venue answered at an ID in the middle of the next 100 ms batch.
            inside = self._advance(1)
            await self._depth_response(request_id, symbol, inside)
            await self.update(SYMBOLS_BY_VENUE[symbol], bids=self.bigger_best_bid(symbol))
        elif self.depth_mode == "late":
            # The answer is for the u before a diff the client already has.
            before = self.sent_u[symbol]
            response = self._top(symbol)
            self._last_u[symbol] = before  # the next diff starts exactly at L: L is a boundary
            await self.update(SYMBOLS_BY_VENUE[symbol], bids=self.bigger_best_bid(symbol))
            await self._depth_response(request_id, symbol, before, response)
        else:
            await self._depth_response(request_id, symbol, self.sent_u[symbol])

    def bigger_best_bid(self, symbol: str) -> list[list[str]]:
        """A diff's bids that add one to the best bid's size: changes the book, never crosses it."""
        price, size = self._top(symbol)[0][0]
        return [[price, str(Decimal(size) + 1)]]

    def _top(self, symbol: str) -> tuple[list, list]:
        bids, asks = self.books[symbol]
        top_bids = sorted(bids.items(), key=lambda level: Decimal(level[0]), reverse=True)
        top_asks = sorted(asks.items(), key=lambda level: Decimal(level[0]))
        return [list(level) for level in top_bids[:100]], [list(level) for level in top_asks[:100]]

    async def _depth_response(self, request_id, symbol, last_update_id, top=None) -> None:
        bids, asks = top or self._top(symbol)
        if self.corrupt_depth and bids:
            self.corrupt_depth -= 1
            bids = [[bids[0][0], str(Decimal(bids[0][1]) + 1)], *bids[1:]]
        result = {"lastUpdateId": last_update_id, "bids": bids, "asks": asks}
        await self._ws.send(
            json.dumps({"id": request_id, "status": 200, "result": result}, separators=(",", ":"))
        )

    def _apply(self, depth: dict) -> None:
        symbol = depth["s"]
        if depth["U"] == depth["u"]:
            self.books[symbol] = ({}, {})
        for side, levels in zip(self.books[symbol], (depth["b"], depth["a"]), strict=True):
            for price, size in levels:
                if Decimal(size) == 0:
                    side.pop(price, None)
                else:
                    side[price] = size

    async def trade(self, product_id: str) -> None:
        self._trade_id += 3  # exchange-wide trade IDs: other markets trade in between
        await self._ws.send(
            json.dumps(
                {
                    "E": time.time_ns(),
                    "m": True,
                    "p": "100.00",
                    "q": "0.25",
                    "s": SYMBOLS[product_id],
                    "t": self._trade_id,
                },
                separators=(",", ":"),
            )
        )

    async def _send(self, depth: dict) -> None:
        self._apply(depth)
        self.sent_u[depth["s"]] = depth["u"]
        await self._ws.send(json.dumps({"e": "depthUpdate", **depth}, separators=(",", ":")))

    def requests_of(self, method: str) -> list[tuple[int, float, dict]]:
        return [r for r in self.requests if r[2]["method"] == method]
