"""Orders: requests, the order state machine and fills (ADR 0004, ADR 0014).

Money is Decimal everywhere and serialized as strings. A request's quantity and price must arrive
as strings: a JSON number would already have passed through a float.

States:

    PENDING ──> OPEN ──> PARTIALLY_FILLED ──> FILLED
       │          │            │
       │          └────────────┴──> CANCELLED
       ├──> FILLED, PARTIALLY_FILLED (filled on taking effect)
       ├──> CANCELLED, EXPIRED (a market order's unfilled rest)
       └──> REJECTED

PENDING is an accepted order waiting out the anti-cheat delay (ADR 0005). FILLED, CANCELLED,
EXPIRED and REJECTED are terminal: nothing leaves them, and the filled quantity only grows.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import ROUND_UP, Decimal, InvalidOperation
from enum import StrEnum

from .fees import FeeSchedule, Liquidity
from .instruments import Instrument

_CLIENT_ID = re.compile(r"[A-Za-z0-9._:-]{1,64}")
_ORDER_NAMESPACE = uuid.UUID("5d1e7f0a-3c2b-4e8f-9a61-2f4b8c0d7e13")


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"  # IOC inside the pair's band; never rests (ADR 0014)
    LIMIT = "limit"  # good till cancelled; the crossing part fills as taker, the rest rests


class OrderStatus(StrEnum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"

    @property
    def terminal(self) -> bool:
        return self in TERMINAL


TERMINAL = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.EXPIRED, OrderStatus.REJECTED}
)


class FillCause(StrEnum):
    """Why a fill happened. The honesty report counts maker fills by cause (ADR 0014)."""

    TAKER = "taker"  # against the book when the order took effect
    PRINT = "print"  # a venue trade at the order's price cleared the queue ahead
    TRADE_THROUGH = "trade_through"  # a venue trade beyond the order's price
    BOOK_CROSS = "book_cross"  # the venue's opposite side reached the order's price


class OrderError(ValueError):
    """A request that can't become an order. `reason` is a stable machine-readable code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def order_id_for(account_id: str, client_order_id: str) -> str:
    """Deterministic, so a replay of the same requests produces the same IDs."""
    return uuid.uuid5(_ORDER_NAMESPACE, f"{account_id}\x00{client_order_id}").hex


def _decimal(payload: Mapping, name: str) -> Decimal:
    value = payload.get(name)
    if not isinstance(value, str):
        raise OrderError(
            "bad_decimal", f"{name} must be a decimal string, not {type(value).__name__}"
        )
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation:
        raise OrderError("bad_decimal", f"{name}={value!r} is not a decimal") from None
    if not parsed.is_finite():
        raise OrderError("bad_decimal", f"{name}={value!r} is not finite")
    return parsed


@dataclass(frozen=True)
class OrderRequest:
    client_order_id: str  # the idempotency key, unique per account
    product_id: str
    side: Side
    type: OrderType
    quantity: Decimal  # in the base currency
    limit_price: Decimal | None = None  # limit orders only

    @classmethod
    def parse(cls, payload: Mapping) -> OrderRequest:
        """From a JSON-like mapping. Raises OrderError with a reason code."""
        client_order_id = payload.get("client_order_id")
        if not isinstance(client_order_id, str) or not _CLIENT_ID.fullmatch(client_order_id):
            raise OrderError("bad_client_order_id", "1 to 64 of A-Z a-z 0-9 . _ : -")
        try:
            side = Side(payload.get("side"))
            kind = OrderType(payload.get("type"))
        except ValueError as exc:
            raise OrderError("bad_field", str(exc)) from None
        product = payload.get("product_id")
        if not isinstance(product, str):
            raise OrderError("unknown_product", repr(product))
        price = None
        if kind is OrderType.LIMIT:
            price = _decimal(payload, "limit_price")
        elif payload.get("limit_price") is not None:
            raise OrderError("bad_field", "a market order has no limit_price")
        return cls(client_order_id, product, side, kind, _decimal(payload, "quantity"), price)

    def check(self, instrument: Instrument) -> None:
        """The pair's grids and minimum. Off-grid values are rejected, never rounded."""
        if self.quantity <= 0 or not instrument.on_size_grid(self.quantity):
            raise OrderError(
                "bad_quantity", f"{self.quantity} is not a positive multiple of "
                f"{instrument.size_increment}"
            )  # fmt: skip
        if self.quantity < instrument.min_size:
            raise OrderError("below_min_size", f"{self.quantity} < {instrument.min_size}")
        if self.type is OrderType.LIMIT:
            assert self.limit_price is not None
            if not instrument.on_price_grid(self.limit_price):
                raise OrderError(
                    "bad_price", f"{self.limit_price} is not a positive multiple of "
                    f"{instrument.price_tick}"
                )  # fmt: skip

    def to_dict(self) -> dict:
        return {
            "client_order_id": self.client_order_id,
            "product_id": self.product_id,
            "side": self.side.value,
            "type": self.type.value,
            "quantity": str(self.quantity),
            "limit_price": None if self.limit_price is None else str(self.limit_price),
        }


@dataclass(frozen=True)
class Fill:
    fill_id: str
    order_id: str
    account_id: str
    product_id: str
    side: Side
    price: Decimal
    quantity: Decimal
    liquidity: Liquidity
    cause: FillCause
    fee: Decimal
    fee_asset: str
    ts_ns: int  # market-data clock: received_at_ns of the event that caused it

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity

    def to_dict(self) -> dict:
        return {
            "fill_id": self.fill_id,
            "order_id": self.order_id,
            "account_id": self.account_id,
            "product_id": self.product_id,
            "side": self.side.value,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "liquidity": self.liquidity.value,
            "cause": self.cause.value,
            "fee": str(self.fee),
            "fee_asset": self.fee_asset,
            "ts_ns": self.ts_ns,
        }


class InvalidTransition(RuntimeError):
    pass


@dataclass
class Order:
    order_id: str
    account_id: str
    request: OrderRequest
    created_at_ns: int  # arrival, on the market-data clock
    effective_at_ns: int  # arrival + anti-cheat delay (ADR 0005)
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Decimal = Decimal(0)
    filled_notional: Decimal = Decimal(0)
    maker_notional: Decimal = Decimal(0)
    taker_notional: Decimal = Decimal(0)
    fees: Decimal = Decimal(0)
    fill_count: int = 0
    reject_reason: str | None = None
    cancel_requested_at_ns: int | None = None
    cancel_effective_at_ns: int | None = None
    closed_at_ns: int | None = None
    _fill_ids: list[str] = field(default_factory=list, repr=False)

    @property
    def product_id(self) -> str:
        return self.request.product_id

    @property
    def side(self) -> Side:
        return self.request.side

    @property
    def remaining(self) -> Decimal:
        return self.request.quantity - self.filled_quantity

    @property
    def average_price(self) -> Decimal | None:
        return self.filled_notional / self.filled_quantity if self.filled_quantity else None

    def _require_live(self, action: str) -> None:
        if self.status.terminal:
            raise InvalidTransition(f"cannot {action} order {self.order_id}: {self.status}")

    def activate(self) -> None:
        """PENDING -> OPEN: the order took effect and rests (anything it filled on the way stays
        recorded; an order with fills stays PARTIALLY_FILLED)."""
        if self.status is not OrderStatus.PENDING:
            raise InvalidTransition(f"cannot activate order {self.order_id}: {self.status}")
        if self.request.type is OrderType.MARKET:
            raise InvalidTransition("a market order never rests")
        self.status = OrderStatus.OPEN

    def record_fill(
        self,
        price: Decimal,
        quantity: Decimal,
        liquidity: Liquidity,
        cause: FillCause,
        ts_ns: int,
        fees: FeeSchedule,
        instrument: Instrument,
    ) -> Fill:
        """Apply a fill and return it with its fee. The fee is computed on the order's running
        totals (each fill pays the increase in the rounded-up total), so splitting an order into
        many fills never costs more than one fill would, and never more than was reserved."""
        self._require_live("fill")
        if quantity <= 0 or quantity > self.remaining:
            raise InvalidTransition(f"fill of {quantity} on {self.remaining} remaining")
        if not instrument.on_price_grid(price):
            raise InvalidTransition(f"fill price {price} is off the price grid")
        limit = self.request.limit_price
        if limit is not None and (
            (self.side is Side.BUY and price > limit) or (self.side is Side.SELL and price < limit)
        ):
            raise InvalidTransition(f"fill at {price} is worse than the limit {limit}")
        if (cause is FillCause.TAKER) != (liquidity is Liquidity.TAKER):
            raise InvalidTransition(f"a {cause} fill can't be {liquidity} liquidity")
        notional = price * quantity
        if liquidity is Liquidity.MAKER:
            self.maker_notional += notional
        else:
            self.taker_notional += notional
        total = (
            self.maker_notional * fees.rate(Liquidity.MAKER)
            + self.taker_notional * fees.rate(Liquidity.TAKER)
        ).quantize(fees.fee_increment, rounding=ROUND_UP)
        fee = total - self.fees
        self.fees = total
        self.filled_quantity += quantity
        self.filled_notional += notional
        self.fill_count += 1
        self.status = OrderStatus.FILLED if self.remaining == 0 else OrderStatus.PARTIALLY_FILLED
        if self.status is OrderStatus.FILLED:
            self.closed_at_ns = ts_ns
        fill_id = f"{self.order_id}-{self.fill_count}"
        self._fill_ids.append(fill_id)
        return Fill(
            fill_id=fill_id,
            order_id=self.order_id,
            account_id=self.account_id,
            product_id=self.product_id,
            side=self.side,
            price=price,
            quantity=quantity,
            liquidity=liquidity,
            cause=cause,
            fee=fee,
            fee_asset=instrument.quote,
            ts_ns=ts_ns,
        )

    def cancel(self, ts_ns: int) -> None:
        self._require_live("cancel")
        self.status = OrderStatus.CANCELLED
        self.closed_at_ns = ts_ns

    def expire(self, ts_ns: int) -> None:
        """A market order's unfilled rest (or one that found nothing inside its band)."""
        self._require_live("expire")
        if self.request.type is not OrderType.MARKET:
            raise InvalidTransition("only a market order expires")
        self.status = OrderStatus.EXPIRED
        self.closed_at_ns = ts_ns

    def reject(self, reason: str, ts_ns: int) -> None:
        if self.status is not OrderStatus.PENDING or self.filled_quantity:
            raise InvalidTransition(f"cannot reject order {self.order_id}: {self.status}")
        self.status = OrderStatus.REJECTED
        self.reject_reason = reason
        self.closed_at_ns = ts_ns

    def to_dict(self) -> dict:
        avg = self.average_price
        return {
            "order_id": self.order_id,
            "account_id": self.account_id,
            **self.request.to_dict(),
            "status": self.status.value,
            "filled_quantity": str(self.filled_quantity),
            "remaining_quantity": str(self.remaining),
            "average_price": None if avg is None else str(avg),
            "fees": str(self.fees),
            "reject_reason": self.reject_reason,
            "created_at_ns": self.created_at_ns,
            "effective_at_ns": self.effective_at_ns,
            "cancel_requested_at_ns": self.cancel_requested_at_ns,
            "cancel_effective_at_ns": self.cancel_effective_at_ns,
            "closed_at_ns": self.closed_at_ns,
        }
