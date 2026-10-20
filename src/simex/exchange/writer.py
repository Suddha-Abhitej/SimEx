"""The account writer: accepts orders and cancels, holds funds, settles fills (ADR 0014).

It owns every order record and every balance; one task drives it, so nothing here locks. The
pair engines (#8) own matching and the book, and report back through `record_fill`, `activate`,
`cancel_now`, `expire` and `reject_on_effect`. Every change is appended to `events` with a
sequence number, for the ledger to persist later.

Idempotency: (account_id, client_order_id) names one order forever. The same key with the same
request returns that order as it is now, whether it was accepted or rejected; the same key with
a different request is refused with `idempotency_key_reused`. A malformed request (no usable key,
bad JSON types) is refused without being recorded.

Hooks for the next issues, deliberately not implemented here:
- the stale-data guard (#9) is the `MarketGate` passed in: checked when an order arrives here,
  and again by the engine when it takes effect;
- the anti-cheat delay (#10) is `delay_ns`: an order or cancel takes effect at arrival + delay,
  on the market-data clock.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Protocol

from .accounts import (
    SYSTEM_ACCOUNTS,
    Accounts,
    InsufficientFunds,
    Reservation,
    required_reservation,
)
from .fees import FeeSchedule, Liquidity
from .gate import GateDecision
from .instruments import BPS, Instrument
from .orders import (
    Fill,
    FillCause,
    Order,
    OrderError,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    order_id_for,
)


class MarketGate(Protocol):
    def check(self, product_id: str, now_ns: int) -> GateDecision:
        """Whether orders for the pair may be accepted now, and the best prices if so."""


class AccountLedger(Protocol):
    def save_order(
        self, order: Order, reservation: object | None, band_price: Decimal | None = None
    ) -> None: ...

    def record_fill(
        self,
        fill: Fill,
        instrument: Instrument,
        order: Order,
        reservation: object | None,
        band_price: Decimal | None = None,
    ) -> None: ...

    def record_deposit(
        self, account_id: str, asset: str, amount: Decimal, event_key: str
    ) -> None: ...

    def load_account(self, account_id: str): ...


def band_price(best: Decimal, side: Side, instrument: Instrument) -> Decimal:
    """The furthest price a market order may reach: the best opposite price moved by the band,
    rounded outward to the price grid (so a buy's reservation always covers it)."""
    band = instrument.market_band_bps / BPS
    if side == Side.BUY:
        return (best * (1 + band) / instrument.price_tick).to_integral_value(
            rounding=ROUND_UP
        ) * instrument.price_tick
    return (best * (1 - band) / instrument.price_tick).to_integral_value(
        rounding=ROUND_DOWN
    ) * instrument.price_tick


class AccountWriter:
    def __init__(
        self,
        instruments: Mapping[str, Instrument],
        fees: FeeSchedule,
        gate: MarketGate,
        delay_ns: int,
        accounts: Accounts | None = None,
        ledger: AccountLedger | None = None,
    ) -> None:
        if delay_ns < 0:
            raise ValueError("delay_ns can't be negative")
        self.instruments = dict(instruments)
        self.fees = fees
        self.gate = gate
        self.delay_ns = delay_ns
        self.accounts = accounts or Accounts()
        self.ledger = ledger
        self.halted = False
        self.orders: dict[str, Order] = {}
        self.by_key: dict[tuple[str, str], str] = {}
        self.band_prices: dict[str, Decimal] = {}  # market order -> the band edge it may reach
        self.events: list[dict] = []

    def _persist_order(self, order: Order) -> None:
        if self.ledger is None:
            return
        try:
            self.ledger.save_order(
                order,
                self.accounts.reservations.get(order.order_id),
                self.band_prices.get(order.order_id),
            )
        except Exception:
            self.halted = True
            raise

    def _require_running(self) -> None:
        if self.halted:
            raise RuntimeError("account writer is halted")

    def restore(self, account_id: str) -> None:
        """Replace this writer's account state with the committed ledger state."""
        if self.ledger is None:
            raise ValueError("a ledger is required for restart recovery")
        state = self.ledger.load_account(account_id)
        for row in state.balances:
            self.accounts.balance(account_id, row["asset"]).available = Decimal(row["total"])
        for row in state.reservations:
            reservation = self.accounts.reservations[row["order_id"]] = Reservation(
                account_id, row["asset"], Decimal(row["remaining"])
            )
            balance = self.accounts.balance(account_id, reservation.asset)
            balance.reserved += reservation.remaining
            balance.available -= reservation.remaining
        for row in state.orders:
            order = Order.from_record(row)
            self.orders[order.order_id] = order
            self.by_key[(account_id, order.request.client_order_id)] = order.order_id
            if row["market_band_price"] is not None:
                self.band_prices[order.order_id] = Decimal(row["market_band_price"])

    def deposit(self, account_id: str, asset: str, amount: Decimal, event_key: str) -> None:
        self._require_running()
        if self.ledger is not None:
            self.ledger.record_deposit(account_id, asset, amount, event_key)
        self.accounts.deposit(account_id, asset, amount)

    def _emit(self, kind: str, **fields) -> None:
        self.events.append({"seq": len(self.events) + 1, "type": kind, **fields})

    # Orders ------------------------------------------------------------------------------------

    def submit(self, account_id: str, request: OrderRequest | Mapping, now_ns: int) -> Order:
        """Accept or reject an order. Raises OrderError only for a malformed request or a reused
        idempotency key; every other refusal is a recorded REJECTED order."""
        self._require_running()
        if not isinstance(account_id, str) or not account_id or account_id in SYSTEM_ACCOUNTS:
            raise OrderError("bad_account", repr(account_id))
        if not isinstance(request, OrderRequest):
            request = OrderRequest.parse(request)
        key = (account_id, request.client_order_id)
        if (existing := self.by_key.get(key)) is not None:
            order = self.orders[existing]
            if order.request != request:
                raise OrderError(
                    "idempotency_key_reused",
                    f"{request.client_order_id} already names a different order",
                )
            return order

        order = Order(
            order_id=order_id_for(account_id, request.client_order_id),
            account_id=account_id,
            request=request,
            created_at_ns=now_ns,
            effective_at_ns=now_ns + self.delay_ns,
        )
        self.orders[order.order_id] = order
        self.by_key[key] = order.order_id
        reason = self._acceptance_problem(order, now_ns)
        if reason is not None:
            order.reject(reason, now_ns)
            self._persist_order(order)
            self._emit("order_rejected", order=order.to_dict())
        else:
            self._persist_order(order)
            self._emit("order_accepted", order=order.to_dict())
        return order

    def _acceptance_problem(self, order: Order, now_ns: int) -> str | None:
        request = order.request
        instrument = self.instruments.get(request.product_id)
        if instrument is None:
            return "unknown_product"
        try:
            request.check(instrument)
        except OrderError as exc:
            return exc.reason
        decision = self.gate.check(request.product_id, now_ns)
        if not decision.open:
            return decision.reason or "market_closed"
        if request.type is OrderType.LIMIT:
            assert request.limit_price is not None
            price = request.limit_price
        else:
            best = decision.best_ask if request.side is Side.BUY else decision.best_bid
            if best is None:
                return "no_liquidity"
            price = band_price(best, request.side, instrument)
            self.band_prices[order.order_id] = price
        asset, amount = required_reservation(
            request.side, request.quantity, price, instrument, self.fees
        )
        try:
            self.accounts.reserve(order.order_id, order.account_id, asset, amount)
        except InsufficientFunds:
            self.band_prices.pop(order.order_id, None)
            return "insufficient_funds"
        return None

    def request_cancel(
        self,
        account_id: str,
        now_ns: int,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> Order:
        """Ask to cancel. It takes effect after the same delay as an order (ADR 0014), so fills
        in between stand. Idempotent: a finished order, or one already being cancelled, is
        returned unchanged."""
        self._require_running()
        if order_id is None and client_order_id is not None:
            order_id = self.by_key.get((account_id, client_order_id))
        order = self.orders.get(order_id) if order_id else None
        if order is None or order.account_id != account_id:
            raise OrderError("unknown_order", str(order_id or client_order_id))
        if order.status.terminal or order.cancel_requested_at_ns is not None:
            return order
        order.cancel_requested_at_ns = now_ns
        order.cancel_effective_at_ns = now_ns + self.delay_ns
        self._persist_order(order)
        self._emit("cancel_requested", order=order.to_dict())
        return order

    # Called by the pair engine (#8) -----------------------------------------------------------

    def record_fill(
        self,
        order_id: str,
        price: Decimal,
        quantity: Decimal,
        liquidity: Liquidity,
        cause: FillCause,
        ts_ns: int,
    ) -> Fill:
        order = self.orders[order_id]
        self._require_running()
        if order.request.type is OrderType.MARKET and liquidity is Liquidity.MAKER:
            raise ValueError("a market order never rests, so it can't fill as maker")
        if order.request.type is OrderType.MARKET:
            edge = self.band_prices[order_id]
            if (order.side is Side.BUY and price > edge) or (
                order.side is Side.SELL and price < edge
            ):
                raise ValueError(f"fill at {price} is outside the market band edge {edge}")
        instrument = self.instruments[order.product_id]
        fill = order.record_fill(price, quantity, liquidity, cause, ts_ns, self.fees, instrument)
        self.accounts.settle(fill, instrument)
        if self.ledger is not None:
            try:
                self.ledger.record_fill(
                    fill,
                    instrument,
                    order,
                    self.accounts.reservations.get(order.order_id),
                    self.band_prices.get(order.order_id),
                )
            except Exception:
                self.halted = True
                raise
        self._emit("fill", fill=fill.to_dict())
        if order.status is OrderStatus.FILLED:
            self._close(order)
        elif self.ledger is not None:
            self._persist_order(order)
        return fill

    def activate(self, order_id: str, ts_ns: int) -> None:
        self._require_running()
        order = self.orders[order_id]
        order.activate()
        self._persist_order(order)
        self._emit("order_activated", order_id=order_id, ts_ns=ts_ns)

    def cancel_now(self, order_id: str, ts_ns: int) -> None:
        self._require_running()
        order = self.orders[order_id]
        order.cancel(ts_ns)
        self._close(order)
        self._emit("order_cancelled", order=order.to_dict())

    def expire(self, order_id: str, ts_ns: int) -> None:
        self._require_running()
        order = self.orders[order_id]
        order.expire(ts_ns)
        self._close(order)
        self._emit("order_expired", order=order.to_dict())

    def reject_on_effect(self, order_id: str, reason: str, ts_ns: int) -> None:
        """The gate closed between arrival and taking effect (#9)."""
        self._require_running()
        order = self.orders[order_id]
        order.reject(reason, ts_ns)
        self._close(order)
        self._emit("order_rejected", order=order.to_dict())

    def _close(self, order: Order) -> None:
        self.accounts.release(order.order_id)
        self.band_prices.pop(order.order_id, None)
        self._persist_order(order)

    # Queries -----------------------------------------------------------------------------------

    def open_orders(self, account_id: str) -> list[Order]:
        return [
            o for o in self.orders.values() if o.account_id == account_id and not o.status.terminal
        ]
