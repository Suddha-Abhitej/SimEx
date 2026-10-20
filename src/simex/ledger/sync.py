"""Synchronous facade for the asyncpg ledger used by the synchronous exchange engine."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any

import asyncpg

from simex.exchange.orders import Fill, Order

from .adapter import AccountState, LedgerAdapter


class SyncLedger:
    """Run one asyncpg connection on a private event-loop thread.

    PairEngine is intentionally synchronous, so blocking at the account-writer boundary is the
    only way to make a committed ledger result precede its in-memory projection without changing
    the matching API. The connection never crosses the event-loop boundary.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="simex-ledger", daemon=True)
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._adapter: LedgerAdapter | None = None
        self._thread.start()
        self._ready.wait()
        if self._startup_error is not None:
            raise RuntimeError("could not start the ledger connection") from self._startup_error

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start())
        self._ready.set()
        self._loop.run_forever()
        self._loop.run_until_complete(self._stop())
        self._loop.close()

    async def _start(self) -> None:
        try:
            connection = await asyncpg.connect(self._dsn)
            self._adapter = LedgerAdapter(connection)
            await self._adapter.migrate()
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()

    async def _stop(self) -> None:
        if self._adapter is not None:
            await self._adapter.connection.close()

    def _call(self, operation: Callable[[LedgerAdapter], Any]) -> Any:
        adapter = self._adapter
        if adapter is None:
            raise RuntimeError("ledger connection is not available")

        async def invoke() -> Any:
            return await operation(adapter)

        future: Future[Any] = asyncio.run_coroutine_threadsafe(invoke(), self._loop)
        return future.result()

    def record_fill(
        self,
        fill: Fill,
        instrument: Any,
        order: Order,
        reservation: Any | None,
        band_price: Any | None = None,
    ) -> None:
        self._call(
            lambda adapter: adapter.record_fill(fill, instrument, order, reservation, band_price)
        )

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
