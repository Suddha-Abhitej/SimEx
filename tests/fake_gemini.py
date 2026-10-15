"""A scripted Gemini public WebSocket server for tests, with hand-written data only (ADR 0010).

It reproduces what the live probe measured, not real market data:
- one SUBSCRIBE carries several streams; responses are {"id": N, "status": 200}
- with ?snapshot=-1, every SUBSCRIBE to {symbol}@depth@100ms sends a snapshot: a depthUpdate
  with U == u and the whole book
- update IDs come from one exchange-wide counter; each diff starts at the previous diff's u
  (U == previous u), and the first diff after a snapshot straddles it (U < L < u)
- trades carry no "e" field and their IDs are exchange-wide
- {"method": "ping"} is answered with {"id": N, "status": 200}
"""

from __future__ import annotations

import json
import time

PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
SYMBOLS = {"BTC-USD": "BTCUSD", "ETH-USD": "ETHUSD", "SOL-USD": "SOLUSD"}

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

    async def handler(self, ws) -> None:
        self.connections += 1
        connection = self.connections
        self._ws = ws
        self.snapshot_query = "snapshot=-1" in ws.request.path
        self.subscribed = set()
        self._last_u = {}
        self.depth_subscribes = {}  # a new connection always gets its snapshots
        async for raw in ws:
            request = json.loads(raw)
            self.requests.append((connection, time.monotonic(), request))
            method, request_id = request["method"], request["id"]
            streams = request.get("params") or []
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

    async def update(self, product_id: str, skip: bool = False) -> None:
        """A diff for a product; skip=True loses the diff before it (a gap on this product)."""
        symbol = SYMBOLS[product_id]
        if skip:
            self._last_u[symbol] = self._advance()
        first = self._last_u.get(symbol, self._ids)
        last = self._advance()
        self._last_u[symbol] = last
        await self._send(
            {
                "E": time.time_ns(),
                "U": first,
                "u": last,
                "a": [],
                "b": [["99.50", "0"]],
                "s": symbol,
            }
        )

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
        await self._ws.send(json.dumps({"e": "depthUpdate", **depth}, separators=(",", ":")))

    def requests_of(self, method: str) -> list[tuple[int, float, dict]]:
        return [r for r in self.requests if r[2]["method"] == method]
