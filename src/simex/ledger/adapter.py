"""Transactional, exactly-once fill persistence for the event-sourced ledger."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from simex.exchange.accounts import FEES, MARKET
from simex.exchange.instruments import Instrument
from simex.exchange.orders import Fill, Side

from .migration_runner import apply_migrations

FailureHook = Callable[[str], None | Awaitable[None]]


class LedgerIntegrityError(RuntimeError):
    """An immutable domain row was replayed with different values."""


class LedgerWriteFailure(RuntimeError):
    """A test or database failure prevented a transaction from committing."""


class LedgerHalted(RuntimeError):
    """Repeated ledger failures halt the caller rather than dropping a fill."""


@dataclass(frozen=True)
class Posting:
    account_id: str
    asset: str
    amount: Decimal
    entry_type: str


def fill_postings(fill: Fill, instrument: Instrument) -> tuple[Posting, ...]:
    """Return balanced signed postings for one fill, including the fee."""
    base, quote = instrument.base, instrument.quote
    notional = fill.notional
    if fill.side is Side.BUY:
        return (
            Posting(fill.account_id, quote, -(notional + fill.fee), "agent_payment"),
            Posting(MARKET, quote, notional, "market_proceeds"),
            Posting(FEES, quote, fill.fee, "fee"),
            Posting(MARKET, base, -fill.quantity, "market_delivery"),
            Posting(fill.account_id, base, fill.quantity, "agent_delivery"),
        )
    return (
        Posting(fill.account_id, base, -fill.quantity, "agent_delivery"),
        Posting(MARKET, base, fill.quantity, "market_delivery"),
        Posting(MARKET, quote, -notional, "market_proceeds"),
        Posting(fill.account_id, quote, notional - fill.fee, "agent_proceeds"),
        Posting(FEES, quote, fill.fee, "fee"),
    )


class LedgerAdapter:
    def __init__(
        self,
        connection: Any,
        *,
        retries: int = 3,
        backoff_seconds: float = 0.05,
        failure_hook: FailureHook | None = None,
    ) -> None:
        if retries < 1:
            raise ValueError("retries must be positive")
        self.connection = connection
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.failure_hook = failure_hook
        self.halted = False

    async def migrate(self) -> None:
        await apply_migrations(self.connection)

    async def record_fill(self, fill: Fill, instrument: Instrument) -> None:
        """Persist a fill or halt after repeated failures; never silently drop it."""
        if self.halted:
            raise LedgerHalted("ledger is halted")
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                await self._record_fill_once(fill, instrument)
                return
            except LedgerIntegrityError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.retries:
                    await asyncio.sleep(self.backoff_seconds * 2**attempt)
        self.halted = True
        raise LedgerHalted("ledger failure halted fill processing") from last_error

    async def _record_fill_once(self, fill: Fill, instrument: Instrument) -> None:
        payload = fill.to_dict()
        domain = (
            fill.fill_id,
            fill.order_id,
            fill.account_id,
            fill.product_id,
            fill.side.value,
            fill.price,
            fill.quantity,
            fill.liquidity.value,
            fill.cause.value,
            fill.fee,
            fill.fee_asset,
            fill.ts_ns,
        )
        event_key = f"fill:{fill.fill_id}"
        async with self.connection.transaction():
            await self._fail("before_fill")
            row = await self.connection.fetchrow(
                "SELECT fill_id, order_id, account_id, product_id, side, price, quantity, "
                "liquidity, cause, fee, fee_asset, ts_ns FROM fills WHERE fill_id = $1",
                fill.fill_id,
            )
            if row is not None:
                existing = tuple(
                    row[field]
                    for field in (
                        "fill_id",
                        "order_id",
                        "account_id",
                        "product_id",
                        "side",
                        "price",
                        "quantity",
                        "liquidity",
                        "cause",
                        "fee",
                        "fee_asset",
                        "ts_ns",
                    )
                )
                if existing != domain:
                    raise LedgerIntegrityError(f"fill payload conflict for {fill.fill_id}")
                return
            await self.connection.execute(
                "INSERT INTO fills (fill_id, order_id, account_id, product_id, side, price, "
                "quantity, liquidity, cause, fee, fee_asset, ts_ns, source_key) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                *domain[:12],
                event_key,
            )
            await self._fail("after_fill")
            for _index, posting in enumerate(fill_postings(fill, instrument)):
                await self.connection.execute(
                    "INSERT INTO ledger_postings (posting_id, transaction_id, fill_id, account_id, "
                    "asset, amount, entry_type) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    uuid.uuid4(),
                    event_key,
                    fill.fill_id,
                    posting.account_id,
                    posting.asset,
                    posting.amount,
                    posting.entry_type,
                )
            await self._fail("after_postings")
            await self.connection.execute(
                "INSERT INTO ledger_events (event_id, event_key, aggregate_type, aggregate_id, "
                "aggregate_seq, event_type, occurred_at_ns, payload) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
                uuid.uuid4(),
                event_key,
                "fill",
                fill.order_id,
                int(fill.fill_id.rsplit("-", 1)[-1]),
                "fill",
                fill.ts_ns,
                json.dumps(payload),
            )
            await self._fail("after_event")
            await self.connection.execute(
                "INSERT INTO outbox (outbox_id, event_key, event_type, payload) "
                "VALUES ($1,$2,$3,$4::jsonb)",
                uuid.uuid4(),
                event_key,
                "fill",
                json.dumps(payload),
            )
            await self._fail("after_outbox")
        await self._fail("after_commit")

    async def _fail(self, boundary: str) -> None:
        if self.failure_hook is None:
            return
        result = self.failure_hook(boundary)
        if hasattr(result, "__await__"):
            await result
