"""Sequence-gap detection for one WebSocket connection.

Coinbase numbers every message on a connection with sequence_num, starting again on each new
connection. A jump means messages were dropped. This module only detects and reports gaps;
recovering the order book (snapshot resync) is issue #4.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SequenceAnomaly:
    kind: str  # "gap" (numbers skipped) or "out_of_order" (repeated or went backwards)
    expected: int
    received: int

    @property
    def missing(self) -> int:
        return self.received - self.expected if self.kind == "gap" else 0


class SequenceTracker:
    def __init__(self) -> None:
        self.last: int | None = None

    def reset(self) -> None:
        """Forget the last number; call on every new connection."""
        self.last = None

    def observe(self, sequence_num: int) -> SequenceAnomaly | None:
        last, self.last = self.last, sequence_num
        if last is None:
            return None
        expected = last + 1
        if sequence_num == expected:
            return None
        if sequence_num > expected:
            return SequenceAnomaly("gap", expected, sequence_num)
        # Keep the high-water mark so one stray old message doesn't look like a later gap.
        self.last = last
        return SequenceAnomaly("out_of_order", expected, sequence_num)
