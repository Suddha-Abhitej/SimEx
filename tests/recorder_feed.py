"""A synthetic md.* feed and an in-memory Kafka for the recorder tests (ADR 0013).

Everything here is generated: hand-picked price grids and random sizes, never a live feed
(ADR 0010). The feed has what the recorder must keep in order: snapshots and 30-minute
refreshes, diffs chained by update ID, audits computed from the generator's own book (so a
correct replay matches them all), a gap on one product with its resync marker, flagged updates
and new snapshot, a gap record, and sparse trades.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field

from simex.ingest.envelope import (
    build_audit_envelope,
    build_envelope,
    build_gap_record,
    build_resync_marker,
    topics_for,
)
from simex.ingest.venue import LEVEL2, TRADES, Item
from simex.recorder.source import TP, SourceRecord

TOPICS = topics_for("gemini")
PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
# Like the live keys: BTC and SOL share a partition.
PARTITION = {"BTC-USD": 0, "ETH-USD": 2, "SOL-USD": 0}
GAPS_PARTITION = 1
GRIDS = {  # (best bid, tick)
    "BTC-USD": (100_000, 1),
    "ETH-USD": (4_000, 0.5),
    "SOL-USD": (200, 0.01),
}
START_NS = 1_792_036_800_000_000_000  # 2026-10-15T04:00:00Z, on an hour boundary
SECOND = 1_000_000_000


def _price(product: str, side: str, level: int) -> str:
    best, tick = GRIDS[product]
    price = best - level * tick if side == "b" else best + (level + 1) * tick
    return f"{price:.2f}"


@dataclass
class _Book:
    bids: dict[str, str] = field(default_factory=dict)
    asks: dict[str, str] = field(default_factory=dict)

    def top(self):
        bids = sorted(self.bids.items(), key=lambda kv: -float(kv[0]))[:100]
        asks = sorted(self.asks.items(), key=lambda kv: float(kv[0]))[:100]
        return [list(x) for x in bids], [list(x) for x in asks]


def synthetic_feed(
    hours: float = 2.2, step_s: float = 1.0, seed: int = 7, start_ns: int = START_NS
) -> list[tuple[str, int, bytes, bytes, int]]:
    """(topic, partition, key, value, kafka_ts_ms) in publish order."""
    rng = random.Random(seed)
    out: list[tuple[str, int, bytes, bytes, int]] = []
    ids = 1_000_000
    books = {p: _Book() for p in PAIRS}
    last_u: dict[str, int] = {}
    awaiting: set[str] = set()
    t = start_ns
    end = start_ns + int(hours * 3600 * SECOND)
    next_refresh = {p: start_ns + i * 600 * SECOND for i, p in enumerate(PAIRS)}
    next_audit = {p: start_ns + (60 + i * 40) * SECOND for i, p in enumerate(PAIRS)}
    next_trade = {p: start_ns + (20 + 13 * i) * SECOND for i, p in enumerate(PAIRS)}
    gap_at = start_ns + int(3900 * SECOND)  # ETH loses a diff at 1h05m
    gap_done = False

    def emit(topic: str, product: str, envelope_value: bytes, at: int) -> None:
        partition = GAPS_PARTITION if topic == TOPICS.gaps else PARTITION[product]
        key = b"gap" if topic == TOPICS.gaps else product.encode()
        out.append((topic, partition, key, envelope_value, at // 1_000_000))

    def level2(product: str, first: int, last: int, bids, asks, snapshot: bool, at: int):
        payload = {
            "e": "depthUpdate",
            "E": at,
            "s": product.replace("-", ""),
            "U": first,
            "u": last,
            "b": bids,
            "a": asks,
        }
        env = build_envelope(
            json.dumps(payload, separators=(",", ":")),
            Item(LEVEL2, product, update_ids=(first, last), snapshot=snapshot),
            venue="gemini",
            topics=TOPICS,
            received_at_ns=at,
            exchange_ts_ns=at - 50_000_000,
            awaiting_snapshot=awaiting,
        )
        emit(TOPICS.level2, product, env.value, at)

    def snapshot(product: str, at: int) -> None:
        nonlocal ids
        book = books[product] = _Book()
        for level in range(25):
            book.bids[_price(product, "b", level)] = f"{rng.randint(1, 900) / 100:.2f}"
            book.asks[_price(product, "a", level)] = f"{rng.randint(1, 900) / 100:.2f}"
        ids += rng.randint(5, 40)
        bids, asks = book.top()
        level2(product, ids, ids, bids, asks, True, at)
        last_u[product] = ids - 3  # the next diff straddles the snapshot (ADR 0011)
        awaiting.discard(product)

    def diff(product: str, at: int) -> None:
        nonlocal ids
        book = books[product]
        changes = {"b": [], "a": []}
        for _ in range(rng.randint(1, 4)):
            side = rng.choice("ba")
            price = _price(product, side, rng.randint(0, 30))
            size = "0" if rng.random() < 0.2 else f"{rng.randint(1, 900) / 100:.2f}"
            levels = book.bids if side == "b" else book.asks
            if size == "0":
                levels.pop(price, None)
            else:
                levels[price] = size
            changes[side].append([price, size])
        ids += rng.randint(5, 40)
        level2(product, last_u[product], ids, changes["b"], changes["a"], False, at)
        last_u[product] = ids

    def audit(product: str, at: int, aligned: bool) -> None:
        bids, asks = books[product].top()
        update_id = last_u[product] if aligned else last_u[product] - 1
        raw = json.dumps(
            {
                "id": 9,
                "status": 200,
                "result": {"lastUpdateId": update_id, "bids": bids, "asks": asks},
            },
            separators=(",", ":"),
        )
        env = build_audit_envelope(
            raw,
            product_id=product,
            update_id=update_id,
            aligned=aligned,
            reason="aligned" if aligned else "inside_diff",
            received_at_ns=at,
            venue="gemini",
            topics=TOPICS,
        )
        emit(TOPICS.level2, product, env.value, at)

    def trade(product: str, at: int) -> None:
        payload = {
            "E": at,
            "m": rng.random() < 0.5,
            "p": _price(product, "b", 0),
            "q": "0.25",
            "s": product.replace("-", ""),
            "t": rng.randint(1, 10**9),
        }
        env = build_envelope(
            json.dumps(payload, separators=(",", ":")),
            Item(TRADES, product),
            venue="gemini",
            topics=TOPICS,
            received_at_ns=at,
            exchange_ts_ns=at - 40_000_000,
        )
        emit(TOPICS.trades, product, env.value, at)

    for product in PAIRS:
        snapshot(product, t)
        t += 10_000_000
    while t < end:
        for i, product in enumerate(PAIRS):
            at = t + i * 30_000_000
            if at >= next_refresh[product]:
                marker = build_resync_marker(
                    product, f"r{at}", "refresh", at, venue="gemini", topics=TOPICS
                )
                emit(TOPICS.level2, product, marker.value, at)
                snapshot(product, at + 1_000_000)
                next_refresh[product] += 1800 * SECOND
                continue
            if product == "ETH-USD" and not gap_done and at >= gap_at:
                gap_done = True
                ids += 77  # the lost diff
                last_u[product] = ids
                marker = build_resync_marker(
                    product, "g1", "update_id_gap", at, venue="gemini", topics=TOPICS
                )
                emit(TOPICS.level2, product, marker.value, at)
                record = build_gap_record(
                    venue="gemini",
                    gap_id="g1",
                    reason="update_id_gap",
                    products=[product],
                    channels=["level2"],
                    expected_sequence=None,
                    received_sequence=None,
                    missing_messages=None,
                    id_jump=77,
                    start=(at - SECOND, at - SECOND),
                    end=(at, at),
                    detected_at_ns=at,
                    resync_id="g1",
                    resync_action="resubscribe",
                    fault_injected=False,
                )
                emit(TOPICS.gaps, product, record, at)
                awaiting.add(product)
                diff(product, at + 1_000_000)  # flagged awaiting_snapshot
                snapshot(product, at + 300_000_000)
                continue
            diff(product, at)
            if at >= next_audit[product]:
                aligned = rng.random() < 0.65
                audit(product, at + 1_000_000, aligned)
                next_audit[product] += 120 * SECOND
            if at >= next_trade[product]:
                trade(product, at + 2_000_000)
                next_trade[product] += rng.randint(20, 400) * SECOND
        t += int(step_s * SECOND)
    return out


# In-memory Kafka --------------------------------------------------------------------------------


class Crash(Exception):
    """Stands in for the process dying at a chosen point."""


class FakeKafka:
    def __init__(self, retention_ms: dict[str, int] | None = None) -> None:
        self.logs: dict[TP, list[SourceRecord]] = {}
        self.log_start: dict[TP, int] = {}
        self.group: dict[TP, int] = {}  # committed offsets, survive "restarts"
        self.retention = retention_ms or {
            "md.level2": 86_400_000,
            "md.trades": 86_400_000,
            "md.gaps": 604_800_000,
        }
        self.visible_until_ms: int | None = None  # records after this aren't published yet
        for topic, partitions in (("md.level2", 3), ("md.trades", 3), ("md.gaps", 3)):
            for p in range(partitions):
                self.logs[(topic, p)] = []
                self.log_start[(topic, p)] = 0

    def load(self, feed) -> None:
        for topic, partition, key, value, ts_ms in feed:
            log = self.logs[(topic, partition)]
            log.append(SourceRecord(topic, partition, len(log), ts_ms, key, value))

    def visible_end(self, tp: TP) -> int:
        log = self.logs[tp]
        if self.visible_until_ms is None:
            return len(log)
        n = 0
        while n < len(log) and log[n].timestamp_ms <= self.visible_until_ms:
            n += 1
        return n

    def source(self, **kwargs) -> FakeSource:
        return FakeSource(self, **kwargs)


class FakeSource:
    def __init__(self, kafka: FakeKafka, batch: int = 500, crash_on_commit: int | None = None):
        self.kafka = kafka
        self.batch = batch
        self.positions: dict[TP, int] = {}
        self.commits = 0
        self.crash_on_commit = crash_on_commit  # raise Crash on this commit call (1-based)

    async def start(self) -> list[TP]:
        return sorted(self.kafka.logs)

    async def committed(self, tp: TP) -> int | None:
        return self.kafka.group.get(tp)

    async def beginning_offsets(self) -> dict[TP, int]:
        return dict(self.kafka.log_start)

    async def end_offsets(self) -> dict[TP, int]:
        return {tp: self.kafka.visible_end(tp) for tp in self.kafka.logs}

    def seek(self, tp: TP, offset: int) -> None:
        self.positions[tp] = offset

    async def fetch(self, timeout_ms: int) -> list[SourceRecord]:
        out = []
        for tp in sorted(self.kafka.logs):
            start = max(self.positions.get(tp, 0), self.kafka.log_start[tp])
            stop = min(start + self.batch, self.kafka.visible_end(tp))
            out.extend(self.kafka.logs[tp][start:stop])
            self.positions[tp] = max(start, stop)
        return out

    async def commit(self, offsets: dict[TP, int]) -> None:
        self.commits += 1
        if self.crash_on_commit == self.commits:
            raise Crash(f"crash during commit {self.commits}")
        self.kafka.group.update(offsets)

    async def retention_ms(self, topic: str) -> int | None:
        return self.kafka.retention.get(topic)

    async def stop(self) -> None:
        pass
