"""Cancellation of timed-out operations submitted to a private event loop."""

from __future__ import annotations

import asyncio
from concurrent.futures import CancelledError, Future
from typing import Any

from .adapter import LedgerHalted

CANCELLATION_GRACE_SECONDS = 0.5


def cancel_and_settle(
    loop: asyncio.AbstractEventLoop,
    future: Future[Any],
    task: asyncio.Task[Any] | None,
    grace_seconds: float,
) -> None:
    """Cancel the loop task and prove that it stopped using its connection."""
    if task is None:
        loop.call_soon_threadsafe(future.cancel)
        raise LedgerHalted("ledger operation could not be cancelled")
    loop.call_soon_threadsafe(task.cancel)
    try:
        future.result(timeout=grace_seconds)
    except TimeoutError as exc:
        raise LedgerHalted("ledger cancellation did not settle") from exc
    except CancelledError:
        pass
