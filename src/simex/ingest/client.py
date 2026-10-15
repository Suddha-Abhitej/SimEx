"""Long-running market-data client that publishes every message from one venue to Kafka.

The core here is venue-neutral (ADR 0011); a VenueAdapter (Gemini, or Coinbase for research)
knows the exchange. Gap handling (ADR 0008): when the adapter's gap rule finds lost data, the
client publishes a resync marker for each affected product to the level2 topic, a gap record to
the gaps topic, and asks the adapter for frames that resubscribe those products' level2 for fresh
snapshots, falling back to a reconnect if they don't arrive in time. On Coinbase a gap affects
every product (its counter is per connection); on Gemini, only the product whose IDs broke.

Snapshot refresh (ADR 0009): venues only send snapshots on subscribe or resync, so the client
also resyncs one product at a time on a schedule, giving the level2 topic a recent snapshot for
every product that a book builder can cold-start from. Refreshes are staggered so at most one book
is resyncing, share the rate limiter with gap resyncs, and give way to a gap or disconnect.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import websockets
from aiokafka.errors import KafkaError

from .backoff import Backoff
from .coinbase import CoinbaseAdapter
from .config import Settings
from .envelope import (
    GAPS_KEY,
    Envelope,
    build_envelope,
    build_gap_record,
    build_resync_marker,
    topics_for,
)
from .gemini import GeminiAdapter
from .metrics import IngestMetrics
from .resync import FaultInjector, GapRateMonitor, RateLimiter
from .venue import HEARTBEATS, Anomaly, Parsed, VenueAdapter

log = logging.getLogger(__name__)

PING_SERIES = ("", "ping")  # last_message_age series for ping replies


class Producer(Protocol):
    """The part of AIOKafkaProducer the client uses."""

    async def send(self, topic: str, value: bytes, key: bytes) -> asyncio.Future: ...


class StaleFeed(Exception):
    """No message arrived within the stale window."""


class ResyncTimeout(Exception):
    """A level2 resubscribe did not produce every product's snapshot in time."""


@dataclass
class Resync:
    id: str
    trigger: str  # "gap", "disconnect" or "refresh"
    started_mono: float
    method: str  # "resubscribe" or "reconnect": how the books are being recovered right now
    products: tuple[str, ...]
    resubscribed_at: float | None = None
    fallback: bool = False


@dataclass
class PendingDisconnect:
    """A disconnect gap whose end is known only once the next connection delivers a message."""

    gap_id: str
    resync_id: str
    start: tuple[int | None, int | None]
    detected_at_ns: int
    reason: str = field(default="disconnect")


def make_adapter(settings: Settings, injector: FaultInjector | None = None) -> VenueAdapter:
    if settings.venue == "coinbase":
        return CoinbaseAdapter(settings.pairs, injector)
    return GeminiAdapter(settings.pairs)


class Ingestor:
    def __init__(
        self,
        settings: Settings,
        producer: Producer,
        metrics: IngestMetrics,
        *,
        adapter: VenueAdapter | None = None,
        connect: Callable[..., Any] = websockets.connect,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
        backoff: Backoff | None = None,
    ) -> None:
        self.settings = settings
        self.producer = producer
        self.metrics = metrics
        self._connect = connect
        self._clock_ns = clock_ns
        self._mono = monotonic
        self._sleep = sleep
        self.backoff = backoff or Backoff(
            settings.backoff_initial_seconds, settings.backoff_max_seconds
        )
        self.injector = FaultInjector(settings.drop_every)
        self.adapter = adapter or make_adapter(settings, self.injector)
        if isinstance(self.adapter, CoinbaseAdapter):
            self.adapter.injector = self.injector
        self.venue = self.adapter.venue
        self.topics = topics_for(self.venue)
        metrics.bind(self.venue)
        self.url = settings.ws_url or self.adapter.default_url
        self.limiter = RateLimiter(settings.resync_min_interval_seconds, monotonic)
        self.gap_rate = GapRateMonitor(monotonic)
        self._last_gap_alarm: float | None = None

        pairs = settings.pairs
        # Every book starts invalid: nothing downstream has a snapshot yet.
        self.awaiting: set[str] = set(pairs)
        self.last_snapshot: dict[str, float] = {}
        self.last_message: dict[tuple[str, str], float] = {}
        self.resync: Resync | None = None
        self.resubscribe_wanted = False
        self.resubscribe_products: set[str] = set()
        self._pending_disconnect: PendingDisconnect | None = None
        self._last_seen: tuple[int | None, int | None] = (None, None)
        self._messages_on_connection = 0
        # When the next scheduled snapshot refresh is due; None until a connection is up.
        self.next_refresh_at: float | None = None
        # Application-level ping (venues without a heartbeats channel).
        self.next_ping_at: float | None = None
        self.ping_sent_at: float | None = None
        # Pairs with no snapshot yet on this connection, and when they must have one. A lost
        # snapshot breaks no sequence on Gemini, so without this a book could wait forever.
        self.unsynced_on_connection: set[str] = set()
        self.connect_snapshot_deadline: float | None = None

        metrics.feed_gaps_last_minute.set_function(self.gap_rate.count)
        data_channels = [ch for ch in self.adapter.channels if ch != HEARTBEATS]
        series = [(p, ch) for p in pairs for ch in data_channels]
        if HEARTBEATS in self.adapter.channels:
            series.append(("", HEARTBEATS))
        if self._pings_enabled:
            series.append(PING_SERIES)
        for product, channel in series:
            metrics.last_message_age.labels(product, channel).set_function(
                lambda key=(product, channel): self._seconds_since_message(key)
            )
        for product in pairs:
            metrics.awaiting_snapshot.labels(product).set(1)
            metrics.time_since_last_snapshot.labels(product).set_function(
                lambda p=product: self._seconds_since_snapshot(p)
            )
        if self.injector.enabled:
            for line in (
                "!" * 72,
                f"FAULT INJECTOR ON: dropping every {self.injector.drop_every}th message "
                "(SIMEX_INGEST_DROP_EVERY). Dev only; never run this in production.",
                "!" * 72,
            ):
                log.warning(line)

    @property
    def _pings_enabled(self) -> bool:
        return self.settings.ping_interval_seconds > 0 and HEARTBEATS not in self.adapter.channels

    def _seconds_since_snapshot(self, product: str) -> float:
        last = self.last_snapshot.get(product)
        return float("inf") if last is None else self._mono() - last

    def _seconds_since_message(self, key: tuple[str, str]) -> float:
        last = self.last_message.get(key)
        return float("inf") if last is None else self._mono() - last

    def _refresh_due_at(self) -> float | None:
        """When the next refresh can start, or None while none is scheduled or a resync runs.

        A gap or disconnect resync delivers fresh snapshots anyway, so refreshes wait for it.
        """
        if (
            self.next_refresh_at is None
            or self.resync is not None
            or self.awaiting
            or self.resubscribe_wanted
        ):
            return None
        return max(self.next_refresh_at, self.limiter.ready_at())

    def _set_awaiting(self, products: set[str]) -> None:
        self.awaiting = products
        for product in self.settings.pairs:
            self.metrics.awaiting_snapshot.labels(product).set(int(product in products))

    # Connection loop -------------------------------------------------------------------------

    async def run(self) -> None:
        """Connect, stream and reconnect forever. Stop it by cancelling the task."""
        while True:
            try:
                await self._session()
                reason = "closed"
            except StaleFeed:
                reason = "stale"
                log.warning("no message for %.0fs; reconnecting", self.settings.stale_after_seconds)
            except ResyncTimeout:
                reason = "resync_timeout"
                log.warning(
                    "resubscribe gave no snapshot for %s within %.0fs; reconnecting",
                    ", ".join(sorted(self.awaiting)),
                    self.settings.resync_timeout_seconds,
                )
            except websockets.ConnectionClosed as exc:
                reason = "closed"
                log.warning("%s closed the connection: %s", self.venue, exc)
            except (OSError, TimeoutError, websockets.InvalidHandshake) as exc:
                reason = "connect_failed"
                log.warning("cannot connect to %s: %s: %s", self.url, type(exc).__name__, exc)
            finally:
                self.metrics.connected.set(0)
            self.metrics.reconnects.labels(reason).inc()
            if self._messages_on_connection:
                await self._on_disconnect(fallback=reason == "resync_timeout")
            self._messages_on_connection = 0
            delay = self.backoff.next_delay()
            log.info("reconnecting in %.1fs (attempt %d)", delay, self.backoff.attempt)
            await self._sleep(delay)

    async def _session(self) -> None:
        async with self._connect(self.url, max_size=None, open_timeout=15, ping_interval=20) as ws:
            self.adapter.reset()
            # A fresh subscription sends fresh snapshots, so any earlier resubscribe is moot.
            self.resubscribe_wanted = False
            self.resubscribe_products.clear()
            if self.resync is not None:
                self.resync.resubscribed_at = None
            now = self._mono()
            if self.settings.snapshot_refresh_seconds > 0:
                self.next_refresh_at = now + self.settings.refresh_stagger_seconds
            self.ping_sent_at = None
            self.next_ping_at = now + self.settings.ping_interval_seconds
            self.unsynced_on_connection = set(self.settings.pairs)
            self.connect_snapshot_deadline = now + self.settings.resync_timeout_seconds
            for frame in self.adapter.connect_frames():
                await ws.send(frame)
            log.info(
                "subscribed to %s on %s for %s",
                ", ".join(self.adapter.channels),
                self.venue,
                ", ".join(self.settings.pairs),
            )
            last_message = self._mono()
            while True:
                timeout = self._next_timeout(last_message)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                except TimeoutError:
                    raw = None
                if raw is not None:
                    last_message = self._mono()
                    await self.handle(raw)
                elif self._mono() - last_message >= self.settings.stale_after_seconds:
                    raise StaleFeed
                await self._drive_resync(ws)
                await self._drive_refresh(ws)
                await self._drive_ping(ws)

    def _next_timeout(self, last_message: float) -> float:
        now = self._mono()
        deadlines = [last_message + self.settings.stale_after_seconds]
        if self.resubscribe_wanted:
            deadlines.append(self.limiter.ready_at())
        if self.resync is not None and self.resync.resubscribed_at is not None:
            deadlines.append(self.resync.resubscribed_at + self.settings.resync_timeout_seconds)
        if self.connect_snapshot_deadline is not None:
            deadlines.append(self.connect_snapshot_deadline)
        if (refresh_at := self._refresh_due_at()) is not None:
            deadlines.append(refresh_at)
        if self._pings_enabled and self.next_ping_at is not None:
            deadlines.append(self.next_ping_at)
        return max(0.001, min(deadlines) - now)

    async def _send_frames(self, ws: Any, frames: list[str]) -> None:
        for frame in frames:
            await ws.send(frame)

    async def _drive_resync(self, ws: Any) -> None:
        """Send a wanted resubscribe once the rate limit allows; fall back if it times out."""
        if self.resubscribe_wanted and self.limiter.try_acquire():
            products = tuple(p for p in self.settings.pairs if p in self.resubscribe_products)
            self.resubscribe_wanted = False
            self.resubscribe_products.clear()
            log.warning(
                "resync: unsubscribing and resubscribing level2 for %s", ", ".join(products)
            )
            await self._send_frames(ws, self.adapter.resync_frames(products))
            self.metrics.resyncs.labels("resubscribe", "gap").inc()
            if self.resync is not None:
                self.resync.method = "resubscribe"
                self.resync.resubscribed_at = self._mono()
        resync = self.resync
        if (
            resync is not None
            and resync.resubscribed_at is not None
            and self.awaiting
            and self._mono() - resync.resubscribed_at >= self.settings.resync_timeout_seconds
        ):
            raise ResyncTimeout
        deadline = self.connect_snapshot_deadline
        if deadline is not None and self._mono() >= deadline:
            if self.unsynced_on_connection & self.awaiting:
                raise ResyncTimeout
            self.connect_snapshot_deadline = None

    async def _drive_refresh(self, ws: Any) -> None:
        """Start the next scheduled snapshot refresh once it is due and the rate limit allows.

        Each slot refreshes the product with the oldest snapshot, so with N pairs every product
        is refreshed once per interval, interval / N apart. A slot whose oldest snapshot is
        younger than the stagger (a gap resync just refreshed everything) is skipped.
        """
        due_at = self._refresh_due_at()
        now = self._mono()
        if due_at is None or now < due_at:
            return
        stagger = self.settings.refresh_stagger_seconds
        product = min(self.settings.pairs, key=lambda p: self.last_snapshot.get(p, float("-inf")))
        if now - self.last_snapshot.get(product, float("-inf")) < stagger:
            self.next_refresh_at = now + stagger
            return
        if not self.limiter.try_acquire():
            return
        self.next_refresh_at = now + stagger
        self.resync = Resync(
            id=uuid.uuid4().hex,
            trigger="refresh",
            started_mono=now,
            method="resubscribe",
            products=(product,),
            resubscribed_at=now,
        )
        self.metrics.resyncs.labels("resubscribe", "refresh").inc()
        log.info("snapshot refresh: unsubscribing and resubscribing level2 for %s", product)
        # The marker goes out first, so updates still in flight for this product are flagged.
        await self._invalidate_books(self.resync.id, "refresh", self._clock_ns(), (product,))
        await self._send_frames(ws, self.adapter.resync_frames((product,)))

    async def _drive_ping(self, ws: Any) -> None:
        """Ping venues that have no heartbeats channel, so a quiet but healthy connection always
        has traffic within the stale window, and record the round trip."""
        now = self._mono()
        if not self._pings_enabled or self.next_ping_at is None or now < self.next_ping_at:
            return
        frame = self.adapter.ping_frame()
        if frame is None:
            self.next_ping_at = None
            return
        self.ping_sent_at = now
        # If no reply comes (lost or dropped by the fault injector), ping again after the stale
        # window; a dead connection is caught by the stale timer, since nothing at all arrives.
        self.next_ping_at = now + self.settings.stale_after_seconds
        await ws.send(frame)

    def _supersede_refresh(self, *, timed_out: bool = False) -> None:
        """A gap or disconnect resync takes over from a scheduled refresh in flight."""
        if self.resync is None or self.resync.trigger != "refresh":
            return
        [product] = self.resync.products
        outcome = "timeout" if timed_out else "superseded"
        self.metrics.snapshot_refreshes.labels(product, outcome).inc()
        log.warning("snapshot refresh for %s ended: %s", product, outcome)
        self.resync = None

    # Messages ----------------------------------------------------------------------------------

    async def handle(self, raw: str | bytes) -> list[Envelope]:
        """Process one WebSocket message: check it for gaps, measure lag, publish it."""
        received_at_ns = self._clock_ns()
        if self.injector.should_drop():
            self._drop_for_fault_injection(raw)
            return []
        try:
            message = json.loads(raw)
        except ValueError:
            log.warning("dropping non-JSON message: %.200r", raw)
            return []
        if not isinstance(message, dict):
            return []
        self._messages_on_connection += 1

        parsed = self.adapter.parse(message)
        if parsed.error is not None:
            self.metrics.exchange_errors.inc()
            log.error("%s error: %s", self.venue, parsed.error)
            return []
        here = (parsed.exchange_ts_ns, received_at_ns)

        if self._pending_disconnect is not None:
            await self._finish_disconnect_gap(here)
        for anomaly in parsed.anomalies:
            await self._on_anomaly(anomaly, here)
        self._last_seen = here

        if parsed.invalid is not None:
            log.warning("dropping message: %s", parsed.invalid)
            return []
        if parsed.pong:
            self._on_pong()
            return []
        if parsed.subscribed:
            self.metrics.connected.set(1)
            log.info("subscriptions confirmed by %s", self.venue)
            return []
        return await self._publish_items(raw, parsed, received_at_ns)

    def _on_pong(self) -> None:
        now = self._mono()
        if self.ping_sent_at is not None:
            self.metrics.ping_rtt.set(now - self.ping_sent_at)
        self.ping_sent_at = None
        self.next_ping_at = now + self.settings.ping_interval_seconds
        self.last_message[PING_SERIES] = now

    async def _publish_items(
        self, raw: str | bytes, parsed: Parsed, received_at_ns: int
    ) -> list[Envelope]:
        snapshots = [i.product_id for i in parsed.items if i.snapshot]
        snapshots = [p for p in snapshots if p in self.settings.pairs]
        for product in snapshots:
            self.last_snapshot[product] = self._mono()
            self.unsynced_on_connection.discard(product)
        if not self.unsynced_on_connection:
            self.connect_snapshot_deadline = None
        if snapshots and self.awaiting.intersection(snapshots):
            self._set_awaiting(self.awaiting.difference(snapshots))

        envelopes = [
            build_envelope(
                raw,
                item,
                venue=self.venue,
                topics=self.topics,
                received_at_ns=received_at_ns,
                exchange_ts_ns=parsed.exchange_ts_ns,
                awaiting_snapshot=self.awaiting,
            )
            for item in parsed.items
        ]
        if not envelopes:
            return []

        # Data is flowing, so the next disconnect starts backing off from the beginning.
        self.backoff.reset()
        first = envelopes[0]
        self.metrics.messages.labels(first.channel).inc()
        if (lag := first.lag_ns) is not None:
            self.metrics.feed_lag.labels(first.channel).observe(lag / 1e9)
            self.metrics.last_feed_lag.labels(first.channel).set(lag / 1e9)
        if len(envelopes) > 1:
            log.debug("message carries %d products; published once per product", len(envelopes))

        now = self._mono()
        for envelope in envelopes:
            self.last_message[(envelope.product_id, envelope.channel)] = now
            await self._publish(envelope)
        if self.resync is not None and not self.awaiting:
            self._complete_resync()
        return envelopes

    def _drop_for_fault_injection(self, raw: str | bytes) -> None:
        try:
            message = json.loads(raw)
        except ValueError:
            message = None
        if isinstance(message, dict):
            self.adapter.note_dropped(message)
        sequence_num = message.get("sequence_num") if isinstance(message, dict) else None
        self.metrics.fault_injected_drops.inc()
        log.warning(
            "FAULT INJECTOR: dropped received message #%d%s",
            self.injector.received,
            f" (sequence_num {sequence_num})" if isinstance(sequence_num, int) else "",
        )

    # Gaps and resyncs ------------------------------------------------------------------------

    async def _on_anomaly(self, anomaly: Anomaly, here: tuple[int | None, int | None]) -> None:
        if anomaly.kind == "gap":
            await self._on_gap(anomaly, here)
        elif anomaly.kind == "out_of_order":
            self.metrics.gaps.labels("out_of_order", "feed").inc()
            log.warning(
                "sequence out of order: expected %d, got %d", anomaly.expected, anomaly.received
            )
        else:
            for product in anomaly.products:
                self.metrics.protocol_drift.labels(product, anomaly.kind).inc()
            level = logging.ERROR if anomaly.alarm else logging.INFO
            log.log(
                level,
                "%s protocol drift on %s: %s%s",
                self.venue,
                ", ".join(anomaly.products),
                anomaly.kind,
                " (never seen in the probe; see ADR 0011)" if anomaly.alarm else "",
            )

    async def _on_gap(self, anomaly: Anomaly, here: tuple[int | None, int | None]) -> None:
        injected = anomaly.injected
        self.metrics.gaps.labels("gap", "injected" if injected else "feed").inc()
        if anomaly.missing is not None:
            self.metrics.missed_messages.inc(anomaly.missing)
        log.warning(
            "%s gap%s on %s: expected %s, got %s (%s)",
            anomaly.reason,
            " (fault injector)" if injected else "",
            ", ".join(anomaly.products),
            anomaly.expected,
            anomaly.received,
            f"{anomaly.missing} missing"
            if anomaly.missing is not None
            else f"ID jump {anomaly.id_jump}",
        )
        if not injected:
            self._check_gap_rate()

        detected_at_ns = self._clock_ns()
        products = anomaly.products
        self._supersede_refresh()
        # The resync in flight still delivers valid snapshots for books it hasn't recovered yet,
        # so another resubscribe is only needed for books that had already recovered.
        in_flight = self.resync is not None and self.resync.resubscribed_at is not None
        needs_resubscribe = not in_flight or bool(set(products) - self.awaiting)
        if self.resync is None:
            self.resync = Resync(
                id=uuid.uuid4().hex,
                trigger="gap",
                started_mono=self._mono(),
                method="resubscribe",
                products=products,
            )
        else:
            merged = set(self.resync.products) | set(products)
            self.resync.products = tuple(p for p in self.settings.pairs if p in merged)
        if not needs_resubscribe:
            action = "covered_by_resync_in_flight"
        elif self.limiter.ready():
            action = "resubscribe"
        else:
            action = "deferred"
            self.metrics.resyncs_suppressed.inc()
            log.info(
                "resync rate limit: resubscribe deferred %.1fs",
                self.limiter.ready_at() - self._mono(),
            )
        if needs_resubscribe:
            self.resubscribe_wanted = True
            self.resubscribe_products.update(products)

        await self._invalidate_books(self.resync.id, anomaly.reason, detected_at_ns, products)
        await self._publish_raw(
            self.topics.gaps,
            GAPS_KEY,
            build_gap_record(
                venue=self.venue,
                gap_id=uuid.uuid4().hex,
                reason=anomaly.reason,
                products=products,
                channels=self.adapter.gap_channels,
                expected_sequence=anomaly.expected,
                received_sequence=anomaly.received,
                missing_messages=anomaly.missing,
                id_jump=anomaly.id_jump,
                start=self._last_seen,
                end=here,
                detected_at_ns=detected_at_ns,
                resync_id=self.resync.id,
                resync_action=action,
                fault_injected=injected,
            ),
        )

    def _check_gap_rate(self) -> None:
        count = self.gap_rate.record()
        if count <= self.settings.gap_alert_per_minute:
            return
        now = self._mono()
        if self._last_gap_alarm is not None and now - self._last_gap_alarm < 60:
            return
        self._last_gap_alarm = now
        log.error(
            "%d sequence gaps in the last minute with the fault injector off (limit %d): "
            "%s's sequence semantics may have changed (see ADR 0008, ADR 0011)",
            count,
            self.settings.gap_alert_per_minute,
            self.venue,
        )

    async def _invalidate_books(
        self,
        resync_id: str,
        reason: str,
        detected_at_ns: int,
        products: tuple[str, ...] | None = None,
    ) -> None:
        products = self.settings.pairs if products is None else products
        for product in products:
            await self._publish(
                build_resync_marker(
                    product,
                    resync_id,
                    reason,
                    detected_at_ns,
                    venue=self.venue,
                    topics=self.topics,
                )
            )
        self._set_awaiting(self.awaiting | set(products))

    async def _on_disconnect(self, *, fallback: bool) -> None:
        """Anything sent while disconnected is lost: invalidate the books and record the gap."""
        detected_at_ns = self._clock_ns()
        self._supersede_refresh(timed_out=fallback)
        if self.resync is None:
            self.resync = Resync(
                id=uuid.uuid4().hex,
                trigger="disconnect",
                started_mono=self._mono(),
                method="reconnect",
                products=self.settings.pairs,
            )
        else:
            self.resync.method = "reconnect"
            self.resync.fallback = self.resync.fallback or fallback
            self.resync.products = self.settings.pairs
        self.metrics.resyncs.labels("reconnect", self.resync.trigger).inc()
        await self._invalidate_books(self.resync.id, "disconnect", detected_at_ns)
        self._pending_disconnect = PendingDisconnect(
            gap_id=uuid.uuid4().hex,
            resync_id=self.resync.id,
            start=self._last_seen,
            detected_at_ns=detected_at_ns,
        )

    async def _finish_disconnect_gap(self, here: tuple[int | None, int | None]) -> None:
        pending, self._pending_disconnect = self._pending_disconnect, None
        await self._publish_raw(
            self.topics.gaps,
            GAPS_KEY,
            build_gap_record(
                venue=self.venue,
                gap_id=pending.gap_id,
                reason=pending.reason,
                products=self.settings.pairs,
                channels=self.adapter.channels,
                expected_sequence=None,
                received_sequence=None,
                missing_messages=None,
                id_jump=None,
                start=pending.start,
                end=here,
                detected_at_ns=pending.detected_at_ns,
                resync_id=pending.resync_id,
                resync_action="reconnect",
                fault_injected=False,
            ),
        )

    def _complete_resync(self) -> None:
        resync, self.resync = self.resync, None
        self.resubscribe_wanted = False
        self.resubscribe_products.clear()
        elapsed = self._mono() - resync.started_mono
        if resync.trigger == "refresh":
            [product] = resync.products
            self.metrics.snapshot_refreshes.labels(product, "completed").inc()
            self.metrics.snapshot_refresh_duration.labels(product).observe(elapsed)
            self.metrics.last_snapshot_refresh_duration.labels(product).set(elapsed)
            log.info("snapshot refresh: fresh snapshot for %s in %.0f ms", product, elapsed * 1000)
            return
        self.metrics.resync_duration.labels(resync.method).observe(elapsed)
        log.warning(
            "resync via %s%s: snapshot received for %s in %.0f ms",
            resync.method,
            " (fallback after resubscribe timed out)" if resync.fallback else "",
            ", ".join(resync.products),
            elapsed * 1000,
        )

    # Kafka -------------------------------------------------------------------------------------

    async def _publish(self, envelope: Envelope) -> None:
        await self._publish_raw(envelope.topic, envelope.key, envelope.value)

    async def _publish_raw(self, topic: str, key: bytes, value: bytes) -> None:
        # send() only waits while the producer's buffer is full; delivery is confirmed later.
        try:
            future = await self.producer.send(topic, value=value, key=key)
        except KafkaError as exc:
            # Losing one message is better than stopping the feed; the counter makes it visible.
            self.metrics.publish_errors.labels(topic).inc()
            log.error("Kafka refused a message on %s: %s: %s", topic, type(exc).__name__, exc)
            return

        def delivered(fut: asyncio.Future) -> None:
            if fut.cancelled() or fut.exception() is not None:
                self.metrics.publish_errors.labels(topic).inc()
                log.error("Kafka rejected a message on %s: %r", topic, fut.exception())
            else:
                self.metrics.published.labels(topic).inc()

        future.add_done_callback(delivered)


class CoinbaseIngestor(Ingestor):
    """The ingestor with the Coinbase research adapter, whatever settings.venue says."""

    def __init__(self, settings: Settings, producer: Producer, metrics: IngestMetrics, **kw):
        super().__init__(settings, producer, metrics, adapter=CoinbaseAdapter(settings.pairs), **kw)


class GeminiIngestor(Ingestor):
    """The ingestor with the Gemini adapter, whatever settings.venue says."""

    def __init__(self, settings: Settings, producer: Producer, metrics: IngestMetrics, **kw):
        super().__init__(settings, producer, metrics, adapter=GeminiAdapter(settings.pairs), **kw)
