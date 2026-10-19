"""One pair's md.level2 and md.trades, merged by received time and released up to a watermark
(ADR 0015).

Replay (simex.recorder.replay) feeds the engine every record in (received_at_ns, topic,
partition, offset) order, Kafka offset order within each stream. Live, the two topics reach the
consumer independently, so a record received earlier can be consumed later. PairMerge holds each
stream's records and releases them in replay's order, but only below a watermark: the smallest
lower bound, over the pair's topics, on the received time of any record not yet consumed.

- A partition that is behind is bounded by the last received time consumed on it: the ingestor
  produces in receive order to an idempotent producer, so later offsets were received no
  earlier.
- A caught-up partition is bounded by the wall clock when that was observed, minus a hold-back
  that covers the ingestor-to-consumer delay.

Released events are exactly replay's sequence as long as the bounds hold. A record that arrives
below what has already been released is late: it is still applied, so the book stays right, and
counted, so the hold-back can be checked.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass

NEVER = -1  # a bound below every received time: nothing is released past it


@dataclass(frozen=True)
class Arrival:
    """One consumed record."""

    topic: str
    partition: int
    offset: int
    received_at_ns: int
    product: str
    value: bytes

    @property
    def key(self) -> tuple[int, str, int, int]:
        return (self.received_at_ns, self.topic, self.partition, self.offset)


class Bounds:
    """Per (topic, partition): no record not yet consumed was received before this time.
    Bounds only rise: a claim once made (caught up at a time) stays true."""

    def __init__(self, holdback_ns: int) -> None:
        self.holdback_ns = holdback_ns
        self.bound: dict[tuple[str, int], int] = {}
        self.partitions: dict[str, set[int]] = {}
        self.last_received: dict[tuple[str, int], int] = {}
        self.out_of_order = 0  # received time fell within a partition (never expected)

    def add_partitions(self, topic: str, partitions: Iterable[int]) -> None:
        for p in partitions:
            self.partitions.setdefault(topic, set()).add(p)
            self.bound.setdefault((topic, p), NEVER)

    def consumed(self, arrival: Arrival) -> int:
        """Record a consumed record. Returns how far its received time fell below the last one
        consumed from its partition (0 when in order)."""
        tp = (arrival.topic, arrival.partition)
        last = self.last_received.get(tp)
        backstep = 0
        if last is not None and arrival.received_at_ns < last:
            self.out_of_order += 1
            backstep = last - arrival.received_at_ns
        self.last_received[tp] = max(last or NEVER, arrival.received_at_ns)
        self._raise(tp, arrival.received_at_ns)
        return backstep

    def caught_up(self, topic: str, partition: int, observed_wall_ns: int) -> None:
        """The partition had nothing more to read as of observed_wall_ns (wall clock)."""
        self._raise((topic, partition), observed_wall_ns - self.holdback_ns)

    def _raise(self, tp: tuple[str, int], value: int) -> None:
        if value > self.bound.get(tp, NEVER):
            self.bound[tp] = value

    def for_topic(self, topic: str, partition: int | None) -> int:
        """The bound for one pair on a topic: its partition's, or, before the pair's partition
        is known, the lowest over the topic's partitions."""
        if partition is not None:
            return self.bound.get((topic, partition), NEVER)
        parts = self.partitions.get(topic)
        return min(self.bound[(topic, p)] for p in parts) if parts else NEVER


class PairMerge:
    def __init__(self, product: str, topics: Iterable[str]) -> None:
        self.product = product
        self.topics = tuple(topics)
        self.streams: dict[str, deque[Arrival]] = {t: deque() for t in self.topics}
        self.partition_of: dict[str, int] = {}  # learned from the records
        self.released: tuple[int, str, int, int] | None = None  # the last released key
        self.cleared = NEVER  # everything received at or before this has been released

    def add(self, arrival: Arrival) -> bool:
        """Hold a consumed record. Returns False if it is late: received at or before a time
        already cleared (the engine may have let an order take effect past it), or sorting
        before a released record. The caller applies a late record at once."""
        self.partition_of[arrival.topic] = arrival.partition
        if arrival.received_at_ns <= self.cleared or (
            self.released is not None and arrival.key < self.released
        ):
            return False
        self.streams[arrival.topic].append(arrival)
        return True

    def watermark(self, bounds: Bounds) -> int:
        return min(bounds.for_topic(t, self.partition_of.get(t)) for t in self.topics)

    def release(self, watermark: int) -> list[Arrival]:
        """Held records received strictly before the watermark, in replay order. A record at
        the watermark itself waits: another, not yet consumed, could share its time."""
        out = []
        self.cleared = max(self.cleared, watermark - 1)
        while True:
            heads = [s[0] for s in self.streams.values() if s]
            if not heads:
                return out
            first = min(heads, key=lambda a: a.key)
            if first.received_at_ns >= watermark:
                return out
            self.streams[first.topic].popleft()
            self.released = first.key
            out.append(first)

    @property
    def held(self) -> int:
        return sum(len(s) for s in self.streams.values())
