"""Coinbase Advanced Trade adapter: research only, off by default (ADR 0010).

Coinbase's terms forbid using its data to benchmark AI agents, so this adapter only runs with
SIMEX_COINBASE_RESEARCH=1 and publishes to research.coinbase.md.*, never to the md.* topics the
book and matching engine read.

Gap rule (ADR 0008): sequence_num counts every message on the connection, so a gap means some
message was lost but not which product it was for; every product's book is invalidated.
"""

from __future__ import annotations

import json

from .envelope import parse_timestamp_ns
from .resync import FaultInjector
from .sequence import SequenceTracker
from .venue import HEARTBEATS, LEVEL2, TRADES, Anomaly, Item, Parsed

DEFAULT_URL = "wss://advanced-trade-ws.coinbase.com"
SUBSCRIBE_CHANNELS = (LEVEL2, TRADES, HEARTBEATS)

# Coinbase names a subscription channel differently from the "channel" field of its messages.
MESSAGE_CHANNELS = {
    "l2_data": LEVEL2,
    "market_trades": TRADES,
    "heartbeats": HEARTBEATS,
}


def product_ids(message: dict) -> list[str]:
    """Every product a market-data message carries, in first-seen order."""
    seen: dict[str, None] = {}
    for event in message.get("events") or []:
        if not isinstance(event, dict):
            continue
        if product := event.get("product_id"):
            seen[product] = None
        for trade in event.get("trades") or []:
            if isinstance(trade, dict) and (product := trade.get("product_id")):
                seen[product] = None
    return list(seen)


def snapshot_products(message: dict) -> list[str]:
    """Products that a level2 message carries a full snapshot for."""
    if MESSAGE_CHANNELS.get(message.get("channel", "")) != LEVEL2:
        return []
    return [
        event["product_id"]
        for event in message.get("events") or []
        if isinstance(event, dict) and event.get("type") == "snapshot" and event.get("product_id")
    ]


def exchange_timestamp_ns(message: dict) -> int | None:
    timestamp = message.get("timestamp")
    return parse_timestamp_ns(timestamp) if isinstance(timestamp, str) else None


def message_items(message: dict) -> list[Item]:
    """What to publish for one Coinbase message; empty if it is not market data.

    A message normally carries one product. If it carries several, the unchanged message is
    published once per product, keyed by that product, so every product's partition sees it.
    """
    channel = MESSAGE_CHANNELS.get(message.get("channel", ""))
    if channel is None:
        return []
    sequence_num = message.get("sequence_num")
    if not isinstance(sequence_num, int) or isinstance(sequence_num, bool):
        sequence_num = None
    products = [""] if channel == HEARTBEATS else (product_ids(message) or [""])
    snapshots = set(snapshot_products(message))
    return [
        Item(
            channel=channel,
            product_id=product,
            sequence_num=sequence_num,
            snapshot=channel == LEVEL2 and product in snapshots,
        )
        for product in products
    ]


class CoinbaseAdapter:
    venue = "coinbase"
    default_url = DEFAULT_URL
    channels = SUBSCRIBE_CHANNELS
    gap_channels = SUBSCRIBE_CHANNELS  # a per-connection counter can't say which was hit

    def __init__(self, pairs: tuple[str, ...], injector: FaultInjector | None = None) -> None:
        self.pairs = pairs
        self.injector = injector or FaultInjector()
        self.sequence = SequenceTracker()

    def reset(self) -> None:
        self.sequence.reset()

    def _frame(self, kind: str, channel: str, products: tuple[str, ...]) -> str:
        return json.dumps({"type": kind, "product_ids": list(products), "channel": channel})

    def connect_frames(self) -> list[str]:
        return [self._frame("subscribe", channel, self.pairs) for channel in SUBSCRIBE_CHANNELS]

    def resync_frames(self, products: tuple[str, ...]) -> list[str]:
        # Unsubscribing and resubscribing level2 sends fresh snapshots (measured, ADR 0008).
        return [
            self._frame("unsubscribe", LEVEL2, products),
            self._frame("subscribe", LEVEL2, products),
        ]

    def ping_frame(self) -> str | None:
        return None  # the heartbeats channel keeps traffic flowing

    def note_dropped(self, message: dict) -> None:
        sequence_num = message.get("sequence_num")
        if isinstance(sequence_num, int) and not isinstance(sequence_num, bool):
            self.injector.record_dropped(sequence_num)

    def parse(self, message: dict) -> Parsed:
        if message.get("type") == "error":
            return Parsed(error=str(message.get("message") or message))
        parsed = Parsed()
        try:
            parsed.exchange_ts_ns = exchange_timestamp_ns(message)
        except ValueError as exc:
            # Checked after the sequence number so a dropped bad message isn't counted as a gap.
            parsed.invalid = f"bad timestamp: {exc}"

        sequence_num = message.get("sequence_num")
        if isinstance(sequence_num, int) and not isinstance(sequence_num, bool):
            anomaly = self.sequence.observe(sequence_num)
            if anomaly is not None:
                injected = anomaly.kind == "gap" and self.injector.caused(
                    anomaly.expected, anomaly.received
                )
                parsed.anomalies.append(
                    Anomaly(
                        kind=anomaly.kind,
                        products=self.pairs,
                        reason="sequence_gap",
                        expected=anomaly.expected,
                        received=anomaly.received,
                        missing=anomaly.missing if anomaly.kind == "gap" else None,
                        injected=injected,
                    )
                )
        if parsed.invalid:
            return parsed
        if message.get("channel") == "subscriptions":
            parsed.subscribed = True
            return parsed
        parsed.items = message_items(message)
        return parsed
