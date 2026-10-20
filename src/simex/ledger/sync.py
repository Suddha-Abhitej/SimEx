"""Synchronous facade for the asyncpg ledger used by the synchronous exchange engine."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import CancelledError, Future
from typing import Any

import asyncpg

from simex.exchange.orders import Fill, Order

from .adapter import AccountState, LedgerAdapter, LedgerHalted
from .metrics import LedgerMetrics

HARD_TIMEOUT_SECONDS = 0.5
CANCELLATION_GRACE_SECONDS = 0.5
STARTUP_TIMEOUT_SECONDS = 5.0


class SyncLedger:
    """Run one asyncpg connection on a private event-loop thread.

    PairEngine is intentionally synchronous, so blocking at the account-writer boundary is the
    only way to make a committed ledger result precede its in-memory projection without changing
    the matching API. The connection never crosses the event-loop boundary.
    """

    def __init__(
        self,
        dsn: str,
        *,
        metrics: LedgerMetrics | None = None,
        hard_timeout_seconds: float = HARD_TIMEOUT_SECONDS,
        cancellation_grace_seconds: float = CANCELLATION_GRACE_SECONDS,
    ) -> None:
        if hard_timeout_seconds <= 0:
            raise ValueError("hard_timeout_seconds must be positive")
        if cancellation_grace_seconds <= 0:
            raise ValueError("cancellation_grace_seconds must be positive")
        self._dsn = dsn
        self.metrics = metrics
        self.hard_timeout_seconds = hard_timeout_seconds
        self.cancellation_grace_seconds = cancellation_grace_seconds
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="simex-ledger", daemon=True)
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._adapter: LedgerAdapter | None = None
        self._thread.start()
        if not self._ready.wait(STARTUP_TIMEOUT_SECONDS):
            raise RuntimeError("ledger connection startup timed out")
        if self._startup_error is not None:
            raise RuntimeError("could not start the ledger connection") from self._startup_error

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start())
        self._ready.set()
        if self._startup_error is not None:
            self._loop.close()
            return
        self._loop.run_forever()
        self._loop.run_until_complete(self._stop())
        self._loop.close()

    async def _start(self) -> None:
        connection: Any | None = None
        try:
            connection = await asyncpg.connect(self._dsn)
            self._adapter = LedgerAdapter(connection, metrics=self.metrics)
            await self._adapter.migrate()
        except BaseException as exc:
            if connection is not None:
                await connection.close()
            self._startup_error = exc
            self._ready.set()

    async def _stop(self) -> None:
        if self._adapter is not None:
            await self._adapter.connection.close()

    def _call(
        self,
        operation: Callable[[LedgerAdapter], Any],
        *,
        timeout: float | None = None,
    ) -> Any:
        adapter = self._adapter
        if adapter is None:
            raise RuntimeError("ledger connection is not available")

        async def invoke() -> Any:
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("ledger operation has no asyncio task")
            task_holder["task"] = task
            return await operation(adapter)

        task_holder: dict[str, asyncio.Task[Any]] = {}
        future: Future[Any] = asyncio.run_coroutine_threadsafe(invoke(), self._loop)
        started = time.monotonic_ns()
        effective_timeout = self.hard_timeout_seconds if timeout is None else timeout
        try:
            return future.result(timeout=effective_timeout)
        except TimeoutError:
            if self.metrics is not None:
                self.metrics.timeouts.inc()
            task = task_holder.get("task")
            if task is None:
                self._loop.call_soon_threadsafe(future.cancel)
                if self.metrics is not None:
                    self.metrics.halts.inc()
                raise LedgerHalted("ledger operation could not be cancelled") from None
            # Cancel the task on its own loop; cancelling the cross-thread Future is not enough
            # to prove that the asyncpg operation has stopped using the connection.
            self._loop.call_soon_threadsafe(task.cancel)
            try:
                future.result(timeout=self.cancellation_grace_seconds)
            except TimeoutError as exc:
                if self.metrics is not None:
                    self.metrics.halts.inc()
                raise LedgerHalted("ledger cancellation did not settle") from exc
            except CancelledError:
                pass
            raise TimeoutError from None
        finally:
            if self.metrics is not None:
                self.metrics.observe("writer_ack", time.monotonic_ns() - started)

    def record_fill(
        self,
        fill: Fill,
        instrument: Any,
        order: Order,
        reservation: Any | None,
        band_price: Any | None = None,
    ) -> None:
        started = time.monotonic_ns()
        for attempt in range(3):
            try:
                self._call(
                    lambda adapter: adapter.record_fill(
                        fill, instrument, order, reservation, band_price
                    ),
                    timeout=self.hard_timeout_seconds,
                )
                if self.metrics is not None:
                    self.metrics.observe("intent_to_ack", time.monotonic_ns() - started)
                return
            except TimeoutError as exc:
                if attempt == 2:
                    if self.metrics is not None:
                        self.metrics.halts.inc()
                    raise LedgerHalted("ledger timeout halted fill processing") from exc
                # The timed-out operation may have committed. Re-issuing is safe because the
                # fill_id primary key and ON CONFLICT handling make the write idempotent.
                time.sleep(0.05 * 2**attempt)
            except LedgerHalted:
                raise
            except Exception as exc:
                if self.metrics is not None:
                    self.metrics.halts.inc()
                raise LedgerHalted("ledger error halted fill processing") from exc

    def save_order(
        self, order: Order, reservation: Any | None, band_price: Any | None = None
    ) -> None:
        self._call(lambda adapter: adapter.save_order(order, reservation, band_price))

    def record_deposit(self, account_id: str, asset: str, amount: Any, event_key: str) -> None:
        self._call(lambda adapter: adapter.record_deposit(account_id, asset, amount, event_key))

    def load_account(self, account_id: str) -> AccountState:
        return self._call(lambda adapter: adapter.load_account(account_id))

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
