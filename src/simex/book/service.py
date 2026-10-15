"""The book service: one live OrderBook per product from md.level2, published to Valkey (ADR 0012).

- Cold start (ADR 0009): seek every md.level2 partition to 2x the ingestor's refresh interval
  ago and read forward; an EMPTY book skips updates until its snapshot. If a book is still EMPTY
  once every partition has caught up, the partitions are re-read once from the log start, feeding
  only the books that are still EMPTY; after that the book waits for a live snapshot.
- One writer per pair (CLAUDE.md): a dispatcher routes each record by its Kafka key (the product
  ID, so nothing is parsed) to that product's queue, and one asyncio task per product decodes and
  applies it. Order within a product is the partition's order.
- Every publish interval, each book's top levels and state go to Valkey with a TTL.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

from simex.ingest.kafka import level2_retention_problem
from simex.marketdata import decode

from .config import BookSettings
from .metrics import BookMetrics
from .orderbook import BookState, OrderBook, Outcome
from .record import book_key, book_record
from .source import Level2Source, plan_start_offsets

log = logging.getLogger(__name__)

INF = float("inf")
RESYNC_REASONS = {Outcome.RESYNC: "marker", Outcome.CROSSED: "crossed"}


class Store(Protocol):
    """The part of the Valkey client the service uses."""

    async def set(self, name: str, value: bytes, px: int) -> Any: ...


class BookService:
    def __init__(
        self,
        settings: BookSettings,
        source: Level2Source,
        store: Store,
        metrics: BookMetrics,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.settings = settings
        self.source = source
        self.store = store
        self.metrics = metrics
        self._clock_ns = clock_ns
        self.books = {p: OrderBook(settings.venue, p) for p in settings.pairs}
        self.queues: dict[str, asyncio.Queue[bytes]] = {p: asyncio.Queue() for p in settings.pairs}
        self.partition_of: dict[str, int] = {}  # learned from the records' keys
        self.position: dict[int, int] = {}  # next offset the dispatcher will see, per partition
        self.catch_up_to: dict[int, int] = {}  # end offsets at startup (or before a re-read)
        self.caught_up: set[int] = set()
        self.reread_until: dict[int, int] = {}  # during the log-start re-read
        self.reread_products: set[str] = set()  # the books that re-read feeds (EMPTY at its start)
        self.reread_done = False
        # Books that have finished catching up. A latch: set once the book's partition has
        # reached the end offset seen at startup and its queue has drained, and cleared again
        # only for the books the one-time log-start re-read feeds. In steady state a queued but
        # not yet applied record, or a quiet spell, must never look like catching up; staleness
        # is the reader's 5 s rule on received_at_ns.
        self.caught_up_books: set[str] = set()
        self.last_synced_ns: dict[str, int] = {}
        for product in settings.pairs:
            metrics.set_state(settings.venue, product, BookState.EMPTY)

    # Startup -----------------------------------------------------------------------------------

    async def start(self) -> None:
        partitions = await self.source.start()
        retention = await self.source.retention_ms()
        if retention is not None and (
            problem := level2_retention_problem(retention, self.settings.snapshot_refresh_seconds)
        ):
            log.error("%s (ADR 0009)", problem)
        now_ms = self._clock_ns() // 1_000_000
        target_ms = now_ms - int(self.settings.lookback_seconds * 1000)
        by_time = await self.source.offsets_for_time(target_ms)
        beginnings = await self.source.beginning_offsets()
        ends = await self.source.end_offsets()
        plan = plan_start_offsets(partitions, by_time, beginnings, ends)
        for partition, offset in plan.items():
            if by_time.get(partition) == beginnings[partition] and beginnings[partition] > 0:
                log.warning(
                    "partition %d: the %.0f s window reaches the log start; retention may be "
                    "shorter than the window",
                    partition,
                    self.settings.lookback_seconds,
                )
            self.source.seek(partition, offset)
            self.position[partition] = offset
            self.catch_up_to[partition] = ends[partition]
            if offset >= ends[partition]:
                self.caught_up.add(partition)
        self._beginnings = beginnings
        log.info(
            "reading %s from %.0f s ago: %s",
            self.settings.level2_topic,
            self.settings.lookback_seconds,
            ", ".join(f"p{p} {plan[p]}..{ends[p]}" for p in partitions),
        )

    def catching_up(self, product: str) -> bool:
        return product not in self.caught_up_books

    def update_catch_up(self) -> None:
        """Latch books that have caught up; call when the workers have applied what was queued."""
        every_partition = len(self.caught_up) == len(self.position)
        for product, queue in self.queues.items():
            if product in self.caught_up_books or not queue.empty():
                continue
            partition = self.partition_of.get(product)
            # A book with no record yet has caught up when every partition has.
            if every_partition if partition is None else partition in self.caught_up:
                self.caught_up_books.add(product)

    # Dispatch ----------------------------------------------------------------------------------

    def dispatch(self, records: list) -> None:
        for record in records:
            partition = record.partition
            self.position[partition] = record.offset + 1
            if partition not in self.caught_up and record.offset + 1 >= self.catch_up_to[partition]:
                self.caught_up.add(partition)
            product = record.key.decode() if record.key else ""
            if product not in self.queues:
                continue
            self.partition_of[product] = partition
            rereading = record.offset < self.reread_until.get(partition, -1)
            if rereading and product not in self.reread_products:
                continue  # the re-read only feeds books that had no snapshot when it started
            self.queues[product].put_nowait(record.value)

    def maybe_reread_from_start(self) -> None:
        """Once every partition has caught up, re-read from the log start for EMPTY books."""
        self.update_catch_up()
        if self.reread_done or len(self.caught_up) < len(self.position):
            return
        if not all(q.empty() for q in self.queues.values()):
            return  # the workers haven't applied everything yet
        empty = [p for p, book in self.books.items() if book.state is BookState.EMPTY]
        self.reread_done = True
        if not empty:
            return
        self.reread_products = set(empty)
        self.caught_up_books -= self.reread_products  # catching up again, from the log start
        log.warning(
            "no snapshot in the last %.0f s for %s; re-reading md.level2 from the log start once",
            self.settings.lookback_seconds,
            ", ".join(empty),
        )
        for partition, position in self.position.items():
            start = self._beginnings[partition]
            if start >= position:
                continue
            self.reread_until[partition] = position
            self.catch_up_to[partition] = position
            self.caught_up.discard(partition)
            self.source.seek(partition, start)
            self.position[partition] = start
            self.metrics.replayed_from_start.inc()

    async def dispatch_forever(self) -> None:
        while True:
            records = await self.source.fetch(timeout_ms=500)
            self.dispatch(records)
            await asyncio.sleep(0)  # let the workers apply what was queued
            self.maybe_reread_from_start()

    # Apply -------------------------------------------------------------------------------------

    def apply(self, product: str, value: bytes) -> None:
        """Decode one md.level2 message and apply it to the product's book."""
        book = self.books[product]
        venue = self.settings.venue
        started = time.perf_counter()
        for event in decode(value):
            if (event.venue, event.product_id) != (venue, product):
                continue  # not this book's (a record keyed differently from its envelope)
            outcome = book.apply(event)
            self.metrics.updates.labels(venue, product, str(outcome)).inc()
            if reason := RESYNC_REASONS.get(outcome):
                self.metrics.resyncs.labels(venue, product, reason).inc()
                if outcome is Outcome.CROSSED:
                    self.metrics.crossed.labels(venue, product).inc()
                    log.error("%s book crossed after a message; waiting for a snapshot", product)
            received = getattr(event, "received_at_ns", None)
            if outcome in (Outcome.APPLIED, Outcome.SNAPSHOT) and received is not None:
                lag = (self._clock_ns() - received) / 1e9
                self.metrics.ingest_to_book.labels(venue, product).observe(max(lag, 0.0))
        self.metrics.apply_seconds.labels(venue, product).observe(time.perf_counter() - started)

    async def work(self, product: str) -> None:
        queue = self.queues[product]
        while True:
            value = await queue.get()
            try:
                self.apply(product, value)
            except Exception:  # one bad message must not stop the book; it is logged loudly
                log.exception("cannot apply a message to the %s book", product)

    # Publish -----------------------------------------------------------------------------------

    def refresh_gauges(self, product: str, now_ns: int) -> None:
        book, venue = self.books[product], self.settings.venue
        self.metrics.set_state(venue, product, book.state)
        bids, asks = book.level_counts()
        self.metrics.levels.labels(venue, product, "bid").set(bids)
        self.metrics.levels.labels(venue, product, "ask").set(asks)
        self.metrics.catching_up.labels(venue, product).set(int(self.catching_up(product)))
        if book.ready:
            self.last_synced_ns[product] = now_ns

        def since(ns: int | None) -> float:
            return INF if ns is None else max(0.0, (now_ns - ns) / 1e9)

        self.metrics.seconds_since_snapshot.labels(venue, product).set(
            since(book.snapshot_received_at_ns)
        )
        self.metrics.seconds_since_update.labels(venue, product).set(since(book.received_at_ns))
        self.metrics.seconds_since_synced.labels(venue, product).set(
            0.0 if book.ready else since(self.last_synced_ns.get(product))
        )

    async def publish_once(self) -> None:
        now_ns = self._clock_ns()
        ttl_ms = int(self.settings.publish_ttl_seconds * 1000)
        for product, book in self.books.items():
            self.refresh_gauges(product, now_ns)
            value = book_record(
                book,
                depth=self.settings.publish_depth,
                published_at_ns=now_ns,
                catching_up=self.catching_up(product),
            )
            try:
                await self.store.set(book_key(book.venue, product), value, px=ttl_ms)
            except Exception as exc:  # Valkey down must not stop the books
                self.metrics.valkey_errors.inc()
                log.warning("cannot publish the %s book to Valkey: %r", product, exc)

    async def publish_forever(self) -> None:
        while True:
            await self.publish_once()
            await asyncio.sleep(self.settings.publish_interval_seconds)

    # Run ---------------------------------------------------------------------------------------

    async def run(self) -> None:
        await self.start()
        tasks = [asyncio.create_task(self.work(p), name=f"book-{p}") for p in self.books]
        tasks.append(asyncio.create_task(self.publish_forever(), name="publish"))
        tasks.append(asyncio.create_task(self.dispatch_forever(), name="dispatch"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.source.stop()
