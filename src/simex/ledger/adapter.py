"""Transactional, exactly-once fill persistence for the event-sourced ledger."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from time import monotonic_ns
from typing import Any

from simex.exchange.accounts import EXTERNAL, FEES, MARKET
from simex.exchange.instruments import Instrument
from simex.exchange.orders import Fill, Order, Side

from .metrics import LedgerMetrics
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


@dataclass(frozen=True)
class AccountState:
    orders: tuple[dict[str, Any], ...]
    reservations: tuple[dict[str, Any], ...]
    balances: tuple[dict[str, Any], ...]


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
        metrics: LedgerMetrics | None = None,
    ) -> None:
        if retries < 1:
            raise ValueError("retries must be positive")
        self.connection = connection
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.failure_hook = failure_hook
        self.metrics = metrics
        self.halted = False

    async def migrate(self) -> None:
        await apply_migrations(self.connection)

    async def record_fill(
        self,
        fill: Fill,
        instrument: Instrument,
        order: Order | None = None,
        reservation: Any | None = None,
        band_price: Decimal | None = None,
    ) -> None:
        """Persist a fill or halt after repeated failures; never silently drop it."""
        if self.halted:
            raise LedgerHalted("ledger is halted")
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                await self._record_fill_once(fill, instrument, order, reservation, band_price)
                return
            except LedgerIntegrityError:
                if self.metrics is not None:
                    self.metrics.conflicts.inc()
                raise
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.retries:
                    await asyncio.sleep(self.backoff_seconds * 2**attempt)
        self.halted = True
        if self.metrics is not None:
            self.metrics.halts.inc()
        raise LedgerHalted("ledger failure halted fill processing") from last_error

    async def _record_fill_once(
        self,
        fill: Fill,
        instrument: Instrument,
        order: Order | None,
        reservation: Any | None,
        band_price: Decimal | None,
    ) -> None:
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
        transaction_started = monotonic_ns()
        async with self.connection.transaction():
            self._observe("transaction_begin", monotonic_ns() - transaction_started)
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
                if order is not None:
                    await self._save_order(order, reservation, band_price)
                return
            await self.connection.execute(
                "INSERT INTO fills (fill_id, order_id, account_id, product_id, side, price, "
                "quantity, liquidity, cause, fee, fee_asset, ts_ns, source_key) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                *domain[:12],
                event_key,
            )
            self._observe("fill_insert", monotonic_ns() - transaction_started)
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
            self._observe("postings", monotonic_ns() - transaction_started)
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
            self._observe("event", monotonic_ns() - transaction_started)
            await self._fail("after_event")
            await self.connection.execute(
                "INSERT INTO outbox (outbox_id, event_key, event_type, payload) "
                "VALUES ($1,$2,$3,$4::jsonb)",
                uuid.uuid4(),
                event_key,
                "fill",
                json.dumps(payload),
            )
            self._observe("outbox", monotonic_ns() - transaction_started)
            await self._fail("after_outbox")
            if order is not None:
                await self._save_order(order, reservation, band_price)
        await self._fail("after_commit")
        self._observe("commit", monotonic_ns() - transaction_started)

    def _observe(self, phase: str, elapsed_ns: int) -> None:
        if self.metrics is not None:
            self.metrics.observe(phase, elapsed_ns)

    async def save_order(
        self, order: Order, reservation: Any | None = None, band_price: Decimal | None = None
    ) -> None:
        async with self.connection.transaction():
            await self._save_order(order, reservation, band_price)

    async def _save_order(
        self, order: Order, reservation: Any | None, band_price: Decimal | None
    ) -> None:
        request = order.request.to_dict()
        await self.connection.execute(
            "INSERT INTO orders (order_id, account_id, client_order_id, request, status, "
            "created_at_ns, effective_at_ns, filled_quantity, filled_notional, maker_notional, "
            "taker_notional, fees, fill_count, reject_reason, cancel_requested_at_ns, "
            "cancel_effective_at_ns, closed_at_ns, market_band_price) VALUES "
            "($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) "
            "ON CONFLICT (order_id) DO UPDATE SET status=EXCLUDED.status, "
            "filled_quantity=EXCLUDED.filled_quantity, filled_notional=EXCLUDED.filled_notional, "
            "maker_notional=EXCLUDED.maker_notional, taker_notional=EXCLUDED.taker_notional, "
            "fees=EXCLUDED.fees, fill_count=EXCLUDED.fill_count, "
            "reject_reason=EXCLUDED.reject_reason, "
            "cancel_requested_at_ns=EXCLUDED.cancel_requested_at_ns, "
            "cancel_effective_at_ns=EXCLUDED.cancel_effective_at_ns, "
            "closed_at_ns=EXCLUDED.closed_at_ns, "
            "market_band_price=EXCLUDED.market_band_price",
            order.order_id,
            order.account_id,
            order.request.client_order_id,
            json.dumps(request),
            order.status.value,
            order.created_at_ns,
            order.effective_at_ns,
            order.filled_quantity,
            order.filled_notional,
            order.maker_notional,
            order.taker_notional,
            order.fees,
            order.fill_count,
            order.reject_reason,
            order.cancel_requested_at_ns,
            order.cancel_effective_at_ns,
            order.closed_at_ns,
            band_price,
        )
        if reservation is None:
            await self.connection.execute(
                "DELETE FROM account_reservations WHERE order_id = $1", order.order_id
            )
        else:
            await self.connection.execute(
                "INSERT INTO account_reservations (order_id, account_id, asset, remaining) "
                "VALUES ($1,$2,$3,$4) ON CONFLICT (order_id) DO UPDATE SET "
                "account_id=EXCLUDED.account_id, asset=EXCLUDED.asset, "
                "remaining=EXCLUDED.remaining",
                order.order_id,
                reservation.account_id,
                reservation.asset,
                reservation.remaining,
            )

    async def record_deposit(
        self, account_id: str, asset: str, amount: Decimal, event_key: str
    ) -> None:
        if amount <= 0:
            raise ValueError("deposit amount must be positive")
        async with self.connection.transaction():
            await self.connection.execute(
                "INSERT INTO ledger_postings "
                "(posting_id, transaction_id, fill_id, account_id, asset, amount, entry_type) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                uuid.uuid4(),
                event_key,
                None,
                account_id,
                asset,
                amount,
                "deposit",
            )
            await self.connection.execute(
                "INSERT INTO ledger_postings "
                "(posting_id, transaction_id, fill_id, account_id, asset, amount, entry_type) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7)",
                uuid.uuid4(),
                event_key,
                None,
                EXTERNAL,
                asset,
                -amount,
                "deposit",
            )

    async def load_account(self, account_id: str) -> AccountState:
        orders = await self.connection.fetch(
            "SELECT order_id, account_id, client_order_id, request->>'product_id' AS product_id, "
            "request->>'side' AS side, request->>'type' AS type, "
            "request->>'quantity' AS quantity, request->>'limit_price' AS limit_price, status, "
            "created_at_ns, effective_at_ns, filled_quantity, filled_notional, maker_notional, "
            "taker_notional, fees, fill_count, reject_reason, cancel_requested_at_ns, "
            "cancel_effective_at_ns, closed_at_ns, market_band_price "
            "FROM orders WHERE account_id=$1",
            account_id,
        )
        reservations = await self.connection.fetch(
            "SELECT order_id, account_id, asset, remaining FROM account_reservations "
            "WHERE account_id=$1",
            account_id,
        )
        balances = await self.connection.fetch(
            "SELECT account_id, asset, SUM(amount) AS total FROM ledger_postings "
            "WHERE account_id=$1 GROUP BY account_id, asset",
            account_id,
        )
        return AccountState(
            tuple(dict(row) for row in orders),
            tuple(dict(row) for row in reservations),
            tuple(dict(row) for row in balances),
        )

    async def _fail(self, boundary: str) -> None:
        if self.failure_hook is None:
            return
        result = self.failure_hook(boundary)
        if hasattr(result, "__await__"):
            await result
