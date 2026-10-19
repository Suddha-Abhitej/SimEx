"""The live exchange runner: md.level2 and md.trades to one PairEngine per pair (ADR 0015).

    python -m simex.exchange     (make exchange; metrics on :9104)

Every record is merged per pair by (received_at, topic, partition, offset), as replay orders
it, and released to the engine only below the pair's watermark (simex.exchange.merge). After
each fetch the engine is advanced to just below the watermark, so an order takes effect at its
effective time only once every record received before then has been applied.

There is no order entry yet (the MCP server adds it); the engines hold the live books and the
queue state an order would meet. Cold start reads both topics from two snapshot-refresh
intervals ago, as the book service does (ADR 0009); until a partition catches up, its bound is
the received time of the records read from it, so the backlog is merged exactly too.

For the replay check, the runner writes one line per pair and UTC hour: how many records it
released and a SHA-256 over their (topic, partition, offset) in release order.
tools/merge_replay_check.py recomputes the same from the recordings.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from pathlib import Path

from simex.book.orderbook import BookState
from simex.marketdata import decode
from simex.recorder.schema import Unrecordable, header

from . import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, GateDecision
from .engine import EngineSettings, PairEngine
from .merge import Arrival, Bounds, PairMerge
from .runner_config import RunnerSettings
from .runner_metrics import RunnerMetrics
from .source import Fetched, MarketSource

log = logging.getLogger(__name__)

MS = 1_000_000
HOUR_NS = 3600 * 1_000_000_000


class BookReadyGate:
    """Open while the pair's book is SYNCED. #9 replaces it with the stale-data guard."""

    def __init__(self, engines: dict[str, PairEngine]) -> None:
        self.engines = engines

    def check(self, product_id: str, now_ns: int) -> GateDecision:
        book = self.engines[product_id].book
        if not book.ready:
            return GateDecision(False, "book_not_synced")
        bid, ask = book.best_bid(), book.best_ask()
        return GateDecision(True, None, bid[0] if bid else None, ask[0] if ask else None)


class HourDigests:
    """Per pair and UTC hour (of received time): the records released, in release order. An
    hour is complete when the runner released records from before it and after it; the first
    hour after a start and the hour open at shutdown are partial."""

    def __init__(self, path: str | Path | None) -> None:
        self.path = Path(path) if path else None
        self.open: dict[str, dict] = {}
        self.started: dict[str, int] = {}  # product -> first released received time
        self.lines: list[dict] = []  # everything written, for tests

    def add(self, arrival: Arrival, late: bool) -> None:
        product, hour = arrival.product, arrival.received_at_ns // HOUR_NS
        current = self.open.get(product)
        if current is not None and hour > current["hour"]:
            self._close(product, complete=self.started[product] < current["hour"] * HOUR_NS)
            current = None
        if current is None:
            self.started.setdefault(product, arrival.received_at_ns)
            current = self.open[product] = {
                "hour": hour,
                "sha": hashlib.sha256(),
                "records": 0,
                "late": 0,
            }
        current["sha"].update(digest_line(arrival.topic, arrival.partition, arrival.offset))
        current["records"] += 1
        current["late"] += late

    def _close(self, product: str, complete: bool) -> None:
        current = self.open.pop(product)
        line = {
            "product_id": product,
            "hour_start_ns": current["hour"] * HOUR_NS,
            "records": current["records"],
            "sha256": current["sha"].hexdigest(),
            "late": current["late"],
            "complete": complete,
        }
        self.lines.append(line)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(line) + "\n")

    def close_all(self) -> None:
        for product in list(self.open):
            self._close(product, complete=False)


def digest_line(topic: str, partition: int, offset: int) -> bytes:
    return f"{topic}\t{partition}\t{offset}\n".encode()


def replay_digests(recorded, products) -> dict[tuple[str, int], tuple[int, str]]:
    """The digests the runner should have written, from recordings read in replay order
    (simex.recorder.replay.read over level2 and market_trades): (product, hour start) ->
    (records, sha256)."""
    wanted = set(products)
    open_: dict[tuple[str, int], list] = {}
    for rec in recorded:
        if rec.product_id not in wanted:
            continue
        key = (rec.product_id, rec.received_at_ns // HOUR_NS * HOUR_NS)
        entry = open_.setdefault(key, [0, hashlib.sha256()])
        entry[0] += 1
        entry[1].update(digest_line(rec.topic, rec.partition, rec.offset))
    return {key: (n, sha.hexdigest()) for key, (n, sha) in open_.items()}


class ExchangeRunner:
    def __init__(
        self,
        settings: RunnerSettings,
        source: MarketSource,
        metrics: RunnerMetrics,
        *,
        wall_ns: Callable[[], int] = time.time_ns,
        engine_settings: EngineSettings | None = None,
    ) -> None:
        self.settings = settings
        self.source = source
        self.metrics = metrics
        self._wall_ns = wall_ns
        self.engines: dict[str, PairEngine] = {}
        self.writer = AccountWriter(
            DEFAULT_INSTRUMENTS,
            FeeSchedule.from_env(),
            BookReadyGate(self.engines),
            int(settings.order_delay_ms * MS),
        )
        engine_settings = engine_settings or EngineSettings.from_env()
        for product in settings.pairs:
            self.engines[product] = PairEngine(
                product, self.writer, engine_settings, settings.venue
            )
            metrics.set_state(product, BookState.EMPTY)
        self.bounds = Bounds(int(settings.holdback_ms * MS))
        self.merges = {p: PairMerge(p, settings.topics) for p in settings.pairs}
        self.channel = {settings.level2_topic: "level2", settings.trades_topic: "trades"}
        self.digests = HourDigests(settings.digest_path)
        self.watermarks: dict[str, int] = {}
        self.unparsed = 0

    async def start(self) -> None:
        partitions = await self.source.start()
        for topic, parts in partitions.items():
            self.bounds.add_partitions(topic, parts)
        target_ms = self._wall_ns() // MS - int(self.settings.lookback_seconds * 1000)
        # The ends first: "no record at or after the time" then means none existed when the
        # end was read, so starting there skips nothing. Read after, a partition's first record
        # could land between the two queries and be skipped.
        ends = await self.source.end_offsets()
        by_time = await self.source.offsets_for_time(target_ms)
        beginnings = await self.source.beginning_offsets()
        plan = []
        for topic, parts in partitions.items():
            for p in parts:
                offset = by_time.get((topic, p))
                offset = ends[(topic, p)] if offset is None else offset
                offset = max(offset, beginnings[(topic, p)])
                self.source.seek(topic, p, offset)
                plan.append(f"{topic} p{p} {offset}..{ends[(topic, p)]}")
        log.info("reading from %.0f s ago: %s", self.settings.lookback_seconds, ", ".join(plan))

    def handle(self, fetched: Fetched) -> None:
        """Merge one fetch and release what the watermarks allow."""
        caught_up = set(fetched.caught_up)
        now = self._wall_ns()
        for record in fetched.records:
            try:
                received = header(record.value)["received_at_ns"]
            except (Unrecordable, KeyError):
                self.unparsed += 1
                continue
            if not isinstance(received, int):
                self.unparsed += 1
                continue
            product = record.key.decode() if record.key else ""
            arrival = Arrival(
                record.topic, record.partition, record.offset, received, product, record.value
            )
            self.bounds.consumed(arrival)
            if (record.topic, record.partition) in caught_up:
                self.metrics.delivery_seconds.labels(self.channel[record.topic]).observe(
                    max(0, now - received) / 1e9
                )
            merge = self.merges.get(product)
            if merge is None:
                continue  # another product's record: it still bounds its partition
            if not merge.add(arrival):
                behind = max(merge.cleared, merge.released[0] if merge.released else 0)
                self.metrics.late.labels(product, self.channel[record.topic]).inc()
                self.metrics.late_seconds.observe(max(0, behind - received) / 1e9)
                self._apply(arrival, late=True)
        for topic, partition in fetched.caught_up:
            self.bounds.caught_up(topic, partition, fetched.observed_wall_ns)
        self.metrics.out_of_order.set(self.bounds.out_of_order)
        for key in self.bounds.bound:
            self.metrics.caught_up.labels(*map(str, key)).set(1 if key in caught_up else 0)
        self.release()

    def release(self) -> None:
        now = self._wall_ns()
        for product, merge in self.merges.items():
            watermark = merge.watermark(self.bounds)
            for arrival in merge.release(watermark):
                self._apply(arrival, late=False)
            engine = self.engines[product]
            if watermark - 1 > engine.now_ns:
                engine.advance(watermark - 1)
            self.watermarks[product] = watermark
            self.metrics.watermark_lag.labels(product).set(max(0, now - watermark) / 1e9)
            self.metrics.held.labels(product).set(merge.held)
            self.metrics.set_state(product, engine.book.state)

    def _apply(self, arrival: Arrival, late: bool) -> None:
        self.digests.add(arrival, late)
        self.metrics.events.labels(arrival.product, self.channel[arrival.topic]).inc()
        try:
            events = decode(arrival.value)
        except Exception:  # one bad record must not stop the engines; it is counted
            self.metrics.undecodable.labels(arrival.product).inc()
            log.exception("cannot decode %s offset %d", arrival.topic, arrival.offset)
            return
        engine = self.engines[arrival.product]
        for event in events:
            engine.on_event(event)

    async def run(self) -> None:
        await self.start()
        try:
            while True:
                self.handle(await self.source.fetch(timeout_ms=100))
        finally:
            self.digests.close_all()
            await self.source.stop()
