"""Bounded writer notifications, separate from the authoritative ledger.

Only the journal lock crosses threads: reconciliation already changes account halts from its
worker. It protects event numbering, snapshots and wakeups, never order submission or matching.
"""

from __future__ import annotations

import asyncio
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass

DEFAULT_EVENT_BUFFER_SIZE = 10_000  # Provisional operational bound, ADR 0023.


@dataclass(eq=False)
class _Watcher:
    loop: asyncio.AbstractEventLoop
    changed: asyncio.Event
    pending: bool = False


class EventJournal:
    def __init__(self, size: int) -> None:
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise ValueError("event buffer size must be a positive integer")
        self.events: deque[dict] = deque(maxlen=size)
        self._sequence = 0
        self._lock = threading.Lock()
        self._watchers: set[_Watcher] = set()

    @property
    def sequence(self) -> int:
        with self._lock:
            return self._sequence

    def append(self, kind: str, **fields) -> None:
        with self._lock:
            self._sequence += 1
            self.events.append({"seq": self._sequence, "type": kind, **fields})
            for watcher in tuple(self._watchers):
                if not watcher.pending:
                    watcher.pending = True
                    # At most one scheduled notification per socket, including when a worker
                    # emits many halt transitions while the exchange loop is blocked.
                    try:
                        watcher.loop.call_soon_threadsafe(self._notify, watcher)
                    except RuntimeError:
                        # A closed notification loop cannot fail trading or replace the
                        # ledger exception that triggered a halt. Keep the event and notify
                        # the remaining watchers; this socket no longer has a working loop.
                        self._watchers.discard(watcher)

    def _notify(self, watcher: _Watcher) -> None:
        with self._lock:
            watcher.pending = False
            if watcher in self._watchers:
                watcher.changed.set()

    def after(self, sequence: int) -> tuple[int, int, tuple[dict, ...]]:
        with self._lock:
            oldest = self.events[0]["seq"] if self.events else self._sequence + 1
            return oldest, self._sequence, tuple(e for e in self.events if e["seq"] > sequence)

    @contextmanager
    def watch(self):
        watcher = _Watcher(asyncio.get_running_loop(), asyncio.Event())
        with self._lock:
            self._watchers.add(watcher)
        try:
            yield watcher.changed
        finally:
            with self._lock:
                self._watchers.discard(watcher)
