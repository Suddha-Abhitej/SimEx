"""The account writer: accepts orders and cancels, holds funds, settles fills (ADR 0014).

It owns live orders and every balance; durable terminal history stays in the ledger (ADR 0027).
Without a ledger it retains historical orders for idempotency. One task drives order state. The
pair engines (#8) own matching and the book, and report back through `record_fill`, `activate`,
`cancel_now`, `expire` and `reject_on_effect`. Every change is appended to `events` with a
sequence number in a bounded notification buffer. Fill notifications follow ledger commit;
the buffer is not durable history.

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

import threading
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Protocol

from .accounts import (
    SYSTEM_ACCOUNTS,
    Accounts,
    InsufficientFunds,
    Reservation,
    required_reservation,
)
from .events import DEFAULT_EVENT_BUFFER_SIZE, EventJournal
from .fees import FeeSchedule, Liquidity
from .gate import GateDecision
from .instruments import BPS, Instrument
from .order_metrics import OrderObserver
from .orders import (
    Fill,
    FillCause,
    Order,
    OrderError,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
    field_error,
    order_id_for,
)
from .risk import RiskLimits


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

    def load_order(self, account_id: str, *, client_order_id=None, order_id=None): ...


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


def _gate_details(reason: str, product_id: str) -> dict[str, str]:
    rules = {
        "stale_level2": "Wait for fresh level2 data before resubmitting",
        "feed_lagging": "Wait until feed lag recovers before resubmitting",
    }
    rule = rules.get(reason.split(":", 1)[0])
    return {} if rule is None else {"rule": rule, "product_id": product_id, "detail": rule}


def _market_notional_details(details: Mapping[str, str], quote: str) -> dict[str, str]:
    safe = dict(details)
    # A band-derived notional divided by the caller's quantity reveals a price.
    safe.pop("attempted", None)
    safe["rule"] = (
        "Band-bounded notional must not exceed configured max_notional limit "
        f"{safe.get('limit', '(not recorded)')} {quote}; the reservation also "
        "requires band-bounded notional plus fee. Reduce quantity or use a "
        "limit order to know its notional exactly"
    )
    return safe


class AccountWriter:
    def __init__(
        self,
        instruments: Mapping[str, Instrument],
        fees: FeeSchedule,
        gate: MarketGate,
        delay_ns: int,
        accounts: Accounts | None = None,
        ledger: AccountLedger | None = None,
        *,
        risk_limits: RiskLimits | None = None,
        event_buffer_size: int = DEFAULT_EVENT_BUFFER_SIZE,
        order_observer: OrderObserver | None = None,
    ) -> None:
        if delay_ns < 0:
            raise ValueError("delay_ns can't be negative")
        self.instruments = dict(instruments)
        self.fees = fees
        self.gate = gate
        self.delay_ns = delay_ns
        self.accounts = accounts or Accounts()
        self.ledger = ledger
        self.risk_limits = risk_limits
        self._order_observer = order_observer
        if risk_limits is not None and not set(risk_limits.max_quantity) <= set(self.instruments):
            raise ValueError("risk limits contain an unknown product")
        self._halted = False
        self.halted_accounts: set[str] = set()
        self._balance_lock = threading.RLock()
        self.applied_sequences: dict[str, int] = {}
        self.orders: dict[str, Order] = {}
        self.by_key: dict[tuple[str, str], str] = {}
        self._submission_resolution: tuple[str, str, Order | None] | None = None
        self.band_prices: dict[str, Decimal] = {}  # market order -> the band edge it may reach
        self.stream_id = uuid.uuid4().hex
        self._journal = EventJournal(event_buffer_size)
        self.events = self._journal.events

    @property
    def event_sequence(self) -> int:
        return self._journal.sequence

    def events_after(self, sequence: int) -> tuple[int, int, tuple[dict, ...]]:
        return self._journal.after(sequence)

    def watch_events(self):
        return self._journal.watch()

    @property
    def halted(self) -> bool:
        return self._halted

    @halted.setter
    def halted(self, value: bool) -> None:
        if value != self._halted:
            self._halted = value
            self._emit("halt", scope="writer", halted=value)

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
        if order.status.terminal:
            # Persistence precedes retirement. Current engine callers retain their local Order.
            self.orders.pop(order.order_id, None)
            self.by_key.pop((order.account_id, order.request.client_order_id), None)

    def _require_running(self, account_id: str | None = None) -> None:
        if self.halted or (account_id is not None and account_id in self.halted_accounts):
            raise RuntimeError("account writer is halted")

    def halt_account(self, account_id: str) -> None:
        if account_id not in self.halted_accounts:
            self.halted_accounts.add(account_id)
            self._emit("halt", scope="account", account_id=account_id, halted=True)

    def lift_account_halt(self, account_id: str) -> None:
        if account_id in self.halted_accounts:
            self.halted_accounts.discard(account_id)
            self._emit("halt", scope="account", account_id=account_id, halted=False)

    def balance_totals(self, account_id: str) -> dict[str, Decimal]:
        return {
            asset: balance.total for asset, balance in self.accounts.balances[account_id].items()
        }

    def balance_snapshot(self, account_id: str) -> tuple[dict[str, Decimal], int | None]:
        with self._balance_lock:
            return self.balance_totals(account_id), self.applied_sequences.get(account_id)

    def restore(self, account_id: str) -> None:
        """Replace this writer's account state with the committed ledger state."""
        if self.ledger is None:
            raise ValueError("a ledger is required for restart recovery")
        state = self.ledger.load_account(account_id)
        if state.latest_sequence is not None:
            self.applied_sequences[account_id] = int(state.latest_sequence)
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
            order = self._order_from_record(row)
            if order.status.terminal:
                continue
            self.orders[order.order_id] = order
            self.by_key[(account_id, order.request.client_order_id)] = order.order_id
            if (
                order.request.type is OrderType.MARKET
                and not order.status.terminal
                and row["market_band_price"] is not None
            ):
                self.band_prices[order.order_id] = Decimal(row["market_band_price"])

    def _order_from_record(self, row) -> Order:
        order = Order.from_record(row)
        if (
            order.request.type is OrderType.MARKET
            and order.reject_reason == "notional_limit"
            and "attempted" in order.reject_details
        ):
            # Old ledger rows may predate the privacy fix; do not replay their price leak.
            instrument = self.instruments.get(order.product_id)
            order.reject_details = _market_notional_details(
                order.reject_details, instrument.quote if instrument else "quote currency"
            )
        return order

    def lookup_order(self, account_id: str, *, client_order_id=None, order_id=None) -> Order | None:
        """Resolve an exact owner/key or owner/ID; absence is authoritative only after SQL."""
        order_id = order_id or None
        resolved_id = order_id or self.by_key.get((account_id, client_order_id))
        order = self.orders.get(resolved_id) if resolved_id else None
        if order is not None:
            return order if order.account_id == account_id else None
        if self.ledger is None:
            return None
        row = self.ledger.load_order(account_id, client_order_id=client_order_id, order_id=order_id)
        return None if row is None else self._order_from_record(row)

    @contextmanager
    def submission_lookup(self, account_id: str, client_order_id: str):
        """Share one writer-resolved lookup with the synchronous transport pre-check.

        Consumed at submit entry, never a reusable negative cache or caller-supplied freshness.
        There must be no await or other admission between this lookup and submit.
        """
        if self._submission_resolution is not None:
            raise RuntimeError("nested submission lookup")
        order = self.lookup_order(account_id, client_order_id=client_order_id)
        self._submission_resolution = (account_id, client_order_id, order)
        try:
            yield order
        finally:
            self._submission_resolution = None

    def deposit(self, account_id: str, asset: str, amount: Decimal, event_key: str) -> None:
        self._require_running(account_id)
        with self._balance_lock:
            if self.ledger is not None:
                sequence = self.ledger.record_deposit(account_id, asset, amount, event_key)
                if sequence is not None:
                    self.applied_sequences[account_id] = sequence
            self.accounts.deposit(account_id, asset, amount)

    def _emit(self, kind: str, **fields) -> None:
        self._journal.append(kind, **fields)

    # Orders ------------------------------------------------------------------------------------

    def _observe_order(self, outcome: str, product: str | None, reason: str = "") -> None:
        observer = self._order_observer
        if observer is None:
            return
        try:
            observer.observe(outcome, product, reason)
        except Exception:
            self._order_observer = None
            try:
                observer.disable()
            except Exception:
                pass

    def submit(self, account_id: str, request: OrderRequest | Mapping, now_ns: int) -> Order:
        """Accept or reject an order. Raises OrderError only for a malformed request or a reused
        idempotency key; every other refusal is a recorded REJECTED order."""
        resolution, self._submission_resolution = self._submission_resolution, None
        product = None
        failure = "internal_error"
        try:
            if self.halted:
                failure = "writer_halted"
            elif account_id in self.halted_accounts:
                failure = "account_halted"
            self._require_running(account_id)
            failure = "internal_error"
            if not isinstance(account_id, str) or not account_id or account_id in SYSTEM_ACCOUNTS:
                raise OrderError("bad_account", repr(account_id))
            if not isinstance(request, OrderRequest):
                request = OrderRequest.parse(request)
            product = request.product_id
            key = (account_id, request.client_order_id)
            order = (
                resolution[2]
                if resolution is not None and resolution[:2] == key
                else self.lookup_order(account_id, client_order_id=request.client_order_id)
            )
            if order is not None:
                if order.request != request:
                    raise field_error(
                        "idempotency_key_reused",
                        "client_order_id",
                        "already names a different order; use a new ID for a different order",
                    )
                self._observe_order("replayed", product)
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
            failure = "persistence_failure"
            self._persist_order(order)
            failure = "internal_error"
            self._emit(
                "order_rejected" if reason is not None else "order_accepted", order=order.to_dict()
            )
            self._observe_order(
                "arrival_rejected" if reason is not None else "accepted", product, reason or ""
            )
            return order
        except Exception as exc:
            if isinstance(exc, OrderError) and failure != "persistence_failure":
                failure = exc.reason
            self._observe_order("submit_errors", product, failure)
            raise

    def _acceptance_problem(self, order: Order, now_ns: int) -> str | None:
        request = order.request
        instrument = self.instruments.get(request.product_id)
        if instrument is None:
            order.reject_details = field_error(
                "unknown_product", "product_id", "must name a configured product"
            ).details
            return "unknown_product"
        try:
            request.check(instrument)
        except OrderError as exc:
            order.reject_details = dict(exc.details)
            return exc.reason
        if self.risk_limits is not None and request.product_id not in self.risk_limits.max_quantity:
            # Disabled products may have no gate/book; fail at the account policy boundary.
            refusal = self.risk_limits.order_refusal(request.product_id, request.quantity, None)
            assert refusal is not None
            order.reject_details = dict(refusal.details)
            return refusal.reason
        decision = self.gate.check(request.product_id, now_ns)
        if not decision.open:
            reason = decision.reason or "market_closed"
            order.reject_details = _gate_details(reason, request.product_id)
            return reason
        if request.type is OrderType.LIMIT:
            assert request.limit_price is not None
            price = request.limit_price
        else:
            best = decision.best_ask if request.side is Side.BUY else decision.best_bid
            if best is None:
                return "no_liquidity"
            price = band_price(best, request.side, instrument)
            self.band_prices[order.order_id] = price
        if self.risk_limits is not None:
            refusal = self.risk_limits.order_refusal(request.product_id, request.quantity, price)
            if refusal is None:
                remaining = sum(
                    (
                        existing.remaining
                        for existing in self.orders.values()
                        if existing.order_id != order.order_id
                        and existing.account_id == order.account_id
                        and existing.product_id == order.product_id
                        and existing.side is order.side
                        and not existing.status.terminal
                    ),
                    Decimal(0),
                )
                refusal = self.risk_limits.position_refusal(
                    order.product_id,
                    order.side.value,
                    self.balance_totals(order.account_id).get(instrument.base, Decimal(0)),
                    remaining,
                    request.quantity,
                )
            if refusal is not None:
                order.reject_details = dict(refusal.details)
                if request.type is OrderType.MARKET and refusal.reason == "notional_limit":
                    order.reject_details = _market_notional_details(
                        order.reject_details, instrument.quote
                    )
                self.band_prices.pop(order.order_id, None)
                return refusal.reason
        asset, amount = required_reservation(
            request.side, request.quantity, price, instrument, self.fees
        )
        try:
            self.accounts.reserve(order.order_id, order.account_id, asset, amount)
        except InsufficientFunds:
            available = self.accounts.balance(order.account_id, asset).available
            order.reject_details = {
                "rule": "quantity <= available base balance"
                if request.side is Side.SELL
                else "limit notional plus maximum rounded-up fee <= available quote balance",
                "product_id": request.product_id,
                "asset": asset,
                "required": str(amount),
                "available": str(available),
            }
            if request.type is OrderType.MARKET and request.side is Side.BUY:
                # Even a fee-inclusive requirement would disclose the live band price.
                order.reject_details.pop("required")
                order.reject_details["rule"] = (
                    f"Reservation must fit available {asset} balance {available} and covers "
                    "band-bounded notional plus fee (the maximum rounded-up fee). "
                    "Reduce quantity or use a limit order to know its notional exactly"
                )
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
        self._require_running(account_id)
        order = self.lookup_order(account_id, order_id=order_id, client_order_id=client_order_id)
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
        self._require_running(order.account_id)
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
        with self._balance_lock:
            self.accounts.settle(fill, instrument)
            if self.ledger is not None:
                try:
                    sequence = self.ledger.record_fill(
                        fill,
                        instrument,
                        order,
                        self.accounts.reservations.get(order.order_id),
                        self.band_prices.get(order.order_id),
                    )
                    if sequence is not None:
                        self.applied_sequences[order.account_id] = sequence
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
        order = self.orders[order_id]
        self._require_running(order.account_id)
        order.activate()
        self._persist_order(order)
        self._emit("order_activated", order_id=order_id, ts_ns=ts_ns)

    def cancel_now(self, order_id: str, ts_ns: int) -> None:
        order = self.orders[order_id]
        self._require_running(order.account_id)
        order.cancel(ts_ns)
        self._close(order)
        self._emit("order_cancelled", order=order.to_dict())

    def expire(self, order_id: str, ts_ns: int) -> None:
        order = self.orders[order_id]
        self._require_running(order.account_id)
        order.expire(ts_ns)
        self._close(order)
        self._emit("order_expired", order=order.to_dict())

    def reject_on_effect(self, order_id: str, reason: str, ts_ns: int) -> None:
        """The gate closed between arrival and taking effect (#9)."""
        order = self.orders[order_id]
        self._require_running(order.account_id)
        order.reject(reason, ts_ns)
        order.reject_details = _gate_details(reason, order.product_id)
        self._close(order)
        self._emit("order_rejected", order=order.to_dict())
        self._observe_order("effect_rejected", order.product_id, reason)

    def _close(self, order: Order) -> None:
        self.accounts.release(order.order_id)
        # Keep the bound in the terminal audit snapshot before discarding runtime state.
        self._persist_order(order)
        self.band_prices.pop(order.order_id, None)

    # Queries -----------------------------------------------------------------------------------

    def open_orders(self, account_id: str) -> list[Order]:
        return [
            o for o in self.orders.values() if o.account_id == account_id and not o.status.terminal
        ]
