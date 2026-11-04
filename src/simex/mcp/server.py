"""Private MCP tools for the simulated exchange (ADR 0018)."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from mcp.server.fastmcp import FastMCP

from simex.book.orderbook import BookNotReady
from simex.exchange import (
    AccountWriter,
    OrderError,
    OrderRequest,
    OrderStatus,
    PairEngine,
    Side,
)
from simex.exchange.writer import band_price
from simex.ledger import LedgerHalted


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


@dataclass(frozen=True)
class RiskLimits:
    max_quantity: Mapping[str, Decimal]
    max_notional: Mapping[str, Decimal]

    def __post_init__(self) -> None:
        if set(self.max_quantity) != set(self.max_notional):
            raise ValueError("risk limits must cover the same products")
        if any(
            not isinstance(value, Decimal) or not value.is_finite() or value <= 0
            for limits in (self.max_quantity, self.max_notional)
            for value in limits.values()
        ):
            raise ValueError("risk limits must be positive finite Decimals")

    def check(self, product_id: str, quantity: Decimal, price: Decimal | None) -> str | None:
        maximum = self.max_quantity.get(product_id)
        if maximum is None:
            return "product_not_allowed"
        if quantity > maximum:
            return "quantity_limit"
        if price is not None and price * quantity > self.max_notional[product_id]:
            return "notional_limit"
        return None


class ExchangeTools:
    """The only stateful boundary exposed to MCP."""

    def __init__(
        self,
        account_id: str,
        writer: AccountWriter,
        engines: Mapping[str, PairEngine],
        limits: RiskLimits,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        if not account_id:
            raise ValueError("account_id must not be empty")
        if not set(limits.max_quantity) <= set(writer.instruments):
            raise ValueError("risk limits contain an unknown product")
        if not set(engines) <= set(writer.instruments):
            raise ValueError("engine map contains an unknown product")
        self.account_id = account_id
        self.writer = writer
        self.engines = dict(engines)
        self.limits = limits
        self.clock_ns = clock_ns

    def quote(self, product_id: str, depth: int = 5) -> dict[str, Any]:
        if product_id not in self.engines:
            return {"ok": False, "reason": "unknown_product"}
        if not isinstance(depth, int) or isinstance(depth, bool) or not 1 <= depth <= 100:
            return {"ok": False, "reason": "bad_depth"}
        engine = self.engines[product_id]
        try:
            bids, asks = engine.book.depth(depth)
            return {
                "ok": True,
                "product_id": product_id,
                "state": str(engine.book.state),
                "last_update_id": engine.book.last_update_id,
                "received_at_ns": engine.book.received_at_ns,
                "bids": _json_value([[price, size] for price, size in bids]),
                "asks": _json_value([[price, size] for price, size in asks]),
            }
        except BookNotReady:
            return {"ok": False, "reason": "book_not_ready"}

    def place_order(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {"ok": False, "reason": "bad_payload"}
        if not isinstance(payload.get("quantity"), str) or (
            payload.get("limit_price") is not None
            and not isinstance(payload.get("limit_price"), str)
        ):
            return {"ok": False, "reason": "bad_decimal_or_product"}
        try:
            order_request = OrderRequest.parse(payload)
            instrument = self.writer.instruments[order_request.product_id]
            order_request.check(instrument)
            quantity = order_request.quantity
            price = order_request.limit_price
            if price is None:
                decision = self.writer.gate.check(order_request.product_id, self.clock_ns())
                if not decision.open:
                    return {"ok": False, "reason": "no_band_price"}
                else:
                    best = (
                        decision.best_ask if order_request.side is Side.BUY else decision.best_bid
                    )
                    if best is None:
                        return {"ok": False, "reason": "no_band_price"}
                    price = band_price(best, order_request.side, instrument)
        except (KeyError, TypeError, ValueError, OrderError):
            return {"ok": False, "reason": "bad_order"}
        if not quantity.is_finite() or (price is not None and not price.is_finite()):
            return {"ok": False, "reason": "bad_decimal"}
        reason = self.limits.check(order_request.product_id, quantity, price)
        if reason is not None:
            return {"ok": False, "reason": reason}
        if self.account_id in self.writer.halted_accounts:
            return {"ok": False, "reason": "account_halted"}
        if self.writer.halted:
            return {"ok": False, "reason": "writer_halted"}
        try:
            order = self.writer.submit(self.account_id, payload, self.clock_ns())
        except LedgerHalted:
            return {"ok": False, "reason": "ledger_halted"}
        except (OrderError, ValueError) as exc:
            return {"ok": False, "reason": getattr(exc, "reason", "invalid_order")}
        if order.status is OrderStatus.REJECTED:
            return {
                "ok": False,
                "reason": order.reject_reason or "order_rejected",
                "order": _json_value(order.to_dict()),
            }
        try:
            self.engines[order.product_id].submit(order)
        except Exception:
            return {
                "ok": False,
                "reason": "engine_submit_failed",
                "order_id": order.order_id,
            }
        return {"ok": True, "order": _json_value(order.to_dict())}

    def portfolio(self) -> dict[str, Any]:
        orders = self.writer.open_orders(self.account_id)
        balances = self.writer.balance_totals(self.account_id)
        positions = {
            instrument.base: balances.get(instrument.base, Decimal(0))
            for instrument in self.writer.instruments.values()
        }
        return {
            "ok": True,
            "account_id": self.account_id,
            "balances": _json_value(balances),
            "positions": _json_value(positions),
            "open_orders": _json_value([order.to_dict() for order in orders]),
        }


def create_server(tools: ExchangeTools, *, port: int = 8765) -> FastMCP:
    server = FastMCP("simex", host="127.0.0.1", port=port)

    @server.tool()
    def quote(product_id: str, depth: int = 5) -> dict[str, Any]:
        return tools.quote(product_id, depth)

    @server.tool()
    def place_order(
        client_order_id: str,
        product_id: str,
        side: str,
        type: str,
        quantity: str,
        limit_price: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": side,
            "type": type,
            "quantity": quantity,
        }
        if limit_price is not None:
            payload["limit_price"] = limit_price
        return tools.place_order(payload)

    @server.tool()
    def portfolio() -> dict[str, Any]:
        return tools.portfolio()

    return server


async def run_server_async(tools: ExchangeTools, *, port: int = 8765) -> None:
    """Run the private HTTP transport until the owning task is cancelled."""
    await create_server(tools, port=port).run_streamable_http_async()


def run_server(tools: ExchangeTools, *, port: int = 8765) -> None:
    """Run the private HTTP transport; the caller owns exchange construction and lifecycle."""
    create_server(tools, port=port).run(transport="streamable-http")


__all__ = ["ExchangeTools", "RiskLimits", "create_server", "run_server"]
