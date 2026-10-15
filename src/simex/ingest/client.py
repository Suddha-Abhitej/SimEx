"""Long-running Coinbase WebSocket client that publishes every market-data message to Kafka."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from typing import Any, Protocol

import websockets
from aiokafka.errors import KafkaError

from .backoff import Backoff
from .config import Settings
from .envelope import Envelope, build_envelopes
from .metrics import IngestMetrics
from .sequence import SequenceTracker

log = logging.getLogger(__name__)

SUBSCRIBE_CHANNELS = ("level2", "market_trades", "heartbeats")


class Producer(Protocol):
    """The part of AIOKafkaProducer the client uses."""

    async def send(self, topic: str, value: bytes, key: bytes) -> asyncio.Future: ...


class StaleFeed(Exception):
    """No message arrived within the stale window."""


class CoinbaseIngestor:
    def __init__(
        self,
        settings: Settings,
        producer: Producer,
        metrics: IngestMetrics,
        *,
        connect: Callable[..., Any] = websockets.connect,
        clock_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], Any] = asyncio.sleep,
        backoff: Backoff | None = None,
    ) -> None:
        self.settings = settings
        self.producer = producer
        self.metrics = metrics
        self._connect = connect
        self._clock_ns = clock_ns
        self._sleep = sleep
        self.backoff = backoff or Backoff(
            settings.backoff_initial_seconds, settings.backoff_max_seconds
        )
        self.sequence = SequenceTracker()

    async def run(self) -> None:
        """Connect, stream and reconnect forever. Stop it by cancelling the task."""
        while True:
            try:
                await self._session()
                reason = "closed"
            except StaleFeed:
                reason = "stale"
                log.warning("no message for %.0fs; reconnecting", self.settings.stale_after_seconds)
            except websockets.ConnectionClosed as exc:
                reason = "closed"
                log.warning("Coinbase closed the connection: %s", exc)
            except (OSError, TimeoutError, websockets.InvalidHandshake) as exc:
                reason = "connect_failed"
                log.warning(
                    "cannot connect to %s: %s: %s", self.settings.ws_url, type(exc).__name__, exc
                )
            finally:
                self.metrics.connected.set(0)
            self.metrics.reconnects.labels(reason).inc()
            delay = self.backoff.next_delay()
            log.info("reconnecting in %.1fs (attempt %d)", delay, self.backoff.attempt)
            await self._sleep(delay)

    async def _session(self) -> None:
        async with self._connect(
            self.settings.ws_url, max_size=None, open_timeout=15, ping_interval=20
        ) as ws:
            self.sequence.reset()
            for channel in SUBSCRIBE_CHANNELS:
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": list(self.settings.pairs),
                            "channel": channel,
                        }
                    )
                )
            log.info(
                "subscribed to %s for %s",
                ", ".join(SUBSCRIBE_CHANNELS),
                ", ".join(self.settings.pairs),
            )
            while True:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=self.settings.stale_after_seconds
                    )
                except TimeoutError:
                    raise StaleFeed from None
                await self.handle(raw)

    async def handle(self, raw: str | bytes) -> list[Envelope]:
        """Process one WebSocket message: check its sequence, measure lag, publish it."""
        received_at_ns = self._clock_ns()
        try:
            message = json.loads(raw)
        except ValueError:
            log.warning("dropping non-JSON message: %.200r", raw)
            return []
        if not isinstance(message, dict):
            return []

        if message.get("type") == "error":
            self.metrics.coinbase_errors.inc()
            log.error("Coinbase error: %s", message.get("message") or message)
            return []

        sequence_num = message.get("sequence_num")
        if isinstance(sequence_num, int) and not isinstance(sequence_num, bool):
            anomaly = self.sequence.observe(sequence_num)
            if anomaly is not None:
                self.metrics.sequence_gaps.labels(anomaly.kind).inc()
                if anomaly.kind == "gap":
                    self.metrics.missed_messages.inc(anomaly.missing)
                log.warning(
                    "sequence %s on %s: expected %d, got %d (%d missing)",
                    anomaly.kind.replace("_", " "),
                    message.get("channel"),
                    anomaly.expected,
                    anomaly.received,
                    anomaly.missing,
                )

        if message.get("channel") == "subscriptions":
            self.metrics.connected.set(1)
            log.info("subscriptions confirmed: %s", message.get("events"))
            return []

        try:
            envelopes = build_envelopes(raw, message, received_at_ns)
        except ValueError as exc:
            log.warning("dropping message with a bad timestamp: %s", exc)
            return []
        if not envelopes:
            log.debug("ignoring message on channel %r", message.get("channel"))
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

        for envelope in envelopes:
            await self._publish(envelope)
        return envelopes

    async def _publish(self, envelope: Envelope) -> None:
        # send() only waits while the producer's buffer is full; delivery is confirmed later.
        topic = envelope.topic
        try:
            future = await self.producer.send(topic, value=envelope.value, key=envelope.key)
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
