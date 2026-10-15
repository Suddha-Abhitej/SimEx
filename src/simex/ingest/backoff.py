"""Exponential backoff with full jitter for reconnects."""

from __future__ import annotations

import random
from collections.abc import Callable


class Backoff:
    """Delay before attempt n is uniform in [0, min(max, initial * 2**n)].

    Full jitter spreads reconnects out so restarts don't hit Coinbase in lockstep.
    """

    def __init__(
        self,
        initial: float = 1.0,
        maximum: float = 60.0,
        rng: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if initial <= 0 or maximum < initial:
            raise ValueError("need 0 < initial <= maximum")
        self.initial = initial
        self.maximum = maximum
        self.attempt = 0
        self._rng = rng

    def ceiling(self) -> float:
        return min(self.maximum, self.initial * 2 ** min(self.attempt, 32))

    def next_delay(self) -> float:
        delay = self._rng(0.0, self.ceiling())
        self.attempt += 1
        return delay

    def reset(self) -> None:
        self.attempt = 0
