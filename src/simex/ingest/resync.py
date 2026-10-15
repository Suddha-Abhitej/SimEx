"""Building blocks for gap handling: resync rate limit, fault injector and gap-rate alarm."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable


class RateLimiter:
    """Allow an action at most once per min_interval seconds."""

    def __init__(self, min_interval: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.min_interval = min_interval
        self._clock = clock
        self.last: float | None = None

    def ready_at(self) -> float:
        return float("-inf") if self.last is None else self.last + self.min_interval

    def ready(self) -> bool:
        return self._clock() >= self.ready_at()

    def try_acquire(self) -> bool:
        if not self.ready():
            return False
        self.last = self._clock()
        return True


class FaultInjector:
    """Dev-only: drop every Nth received message to force sequence gaps. 0 means off."""

    def __init__(self, drop_every: int = 0) -> None:
        if drop_every < 0 or drop_every == 1:
            raise ValueError("SIMEX_INGEST_DROP_EVERY must be 0 (off) or at least 2")
        self.drop_every = drop_every
        self.received = 0
        self.dropped = 0
        self._dropped_sequences: deque[int] = deque(maxlen=1000)

    @property
    def enabled(self) -> bool:
        return self.drop_every > 0

    def should_drop(self) -> bool:
        if not self.enabled:
            return False
        self.received += 1
        if self.received % self.drop_every:
            return False
        self.dropped += 1
        return True

    def record_dropped(self, sequence_num: int | None) -> None:
        if sequence_num is not None:
            self._dropped_sequences.append(sequence_num)

    def caused(self, expected: int, received: int) -> bool:
        """True if a message this injector dropped falls inside the gap [expected, received)."""
        return any(expected <= seq < received for seq in self._dropped_sequences)


class GapRateMonitor:
    """Counts real (not injected) gaps over the last minute."""

    def __init__(self, clock: Callable[[], float] = time.monotonic, window: float = 60.0) -> None:
        self._clock = clock
        self.window = window
        self._times: deque[float] = deque()

    def _prune(self) -> None:
        cutoff = self._clock() - self.window
        while self._times and self._times[0] < cutoff:
            self._times.popleft()

    def record(self) -> int:
        self._times.append(self._clock())
        return self.count()

    def count(self) -> int:
        self._prune()
        return len(self._times)
