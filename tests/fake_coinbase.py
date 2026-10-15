"""A scripted Coinbase Advanced Trade WebSocket server for tests.

It numbers every message on a connection with one sequence_num counter (as the live feed does),
answers subscribe/unsubscribe with a subscriptions message, and sends a level2 snapshot per
product on a level2 subscribe. A duplicate subscribe sends no snapshot, as Coinbase does.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime

PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")


def now_ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"


class FakeCoinbase:
    def __init__(self, pairs=PAIRS, snapshot_on_resubscribe: bool = True) -> None:
        self.pairs = pairs
        self.snapshot_on_resubscribe = snapshot_on_resubscribe
        self.connections = 0
        self.ready_connections = 0
        self.requests: list[tuple[int, float, dict]] = []  # (connection, monotonic time, request)
        self._ws = None
        self._seq = 0

    async def handler(self, ws) -> None:
        self.connections += 1
        connection = self.connections
        self._ws, self._seq = ws, 0
        # Subscriptions are per channel and product, as on Coinbase.
        subscribed: dict[str, set[str]] = {}
        unsubscribed_level2: set[str] = set()
        async for raw in ws:
            request = json.loads(raw)
            self.requests.append((connection, time.monotonic(), request))
            channel, products = request["channel"], request["product_ids"]
            current = subscribed.setdefault(channel, set())
            if request["type"] == "unsubscribe":
                current.difference_update(products)
                if channel == "level2":
                    unsubscribed_level2.update(products)
                await self.ack(subscribed)
                continue
            new = [p for p in products if p not in current]  # a duplicate sends no snapshot
            current.update(products)
            await self.ack(subscribed)
            resubscribe = channel == "level2" and bool(unsubscribed_level2.intersection(products))
            if channel == "level2":
                for product in new:
                    if product not in unsubscribed_level2 or self.snapshot_on_resubscribe:
                        await self.snapshot(product)
            everything = all(
                set(self.pairs) <= subscribed.get(ch, set())
                for ch in ("level2", "market_trades", "heartbeats")
            )
            if everything and not resubscribe:
                self.ready_connections = connection

    async def send(self, channel: str, events: list) -> None:
        message = {
            "channel": channel,
            "client_id": "",
            "timestamp": now_ts(),
            "sequence_num": self._seq,
            "events": events,
        }
        self._seq += 1
        await self._ws.send(json.dumps(message, separators=(",", ":")))

    def skip(self, count: int) -> None:
        """Pretend `count` messages were sent and lost."""
        self._seq += count

    async def ack(self, subscribed: dict[str, set[str]]) -> None:
        subs = {ch: sorted(products) for ch, products in sorted(subscribed.items()) if products}
        await self.send("subscriptions", [{"subscriptions": subs}])

    async def snapshot(self, product: str) -> None:
        level = {
            "side": "bid",
            "event_time": now_ts(),
            "price_level": "100.00",
            "new_quantity": "1.5",
        }
        event = {"type": "snapshot", "product_id": product, "updates": [level]}
        await self.send("l2_data", [event])

    async def update(self, product: str) -> None:
        level = {
            "side": "offer",
            "event_time": now_ts(),
            "price_level": "100.01",
            "new_quantity": "0",
        }
        await self.send("l2_data", [{"type": "update", "product_id": product, "updates": [level]}])

    async def trade(self, product: str) -> None:
        trade = {
            "trade_id": "1",
            "product_id": product,
            "price": "100.00",
            "size": "0.01000000",
            "side": "BUY",
            "time": now_ts(),
        }
        await self.send("market_trades", [{"type": "update", "trades": [trade]}])

    def requests_of(self, kind: str, channel: str) -> list[tuple[int, float, dict]]:
        return [r for r in self.requests if r[2]["type"] == kind and r[2]["channel"] == channel]


async def wait_until(condition, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.005)
