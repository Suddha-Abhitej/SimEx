"""Private MCP tools for the simulated exchange (ADR 0018)."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from collections.abc import Callable, Mapping
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
from simex.exchange.risk import RiskLimits
from simex.exchange.writer import band_price
from simex.ledger import LedgerHalted

from .pnl import position_pnl


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


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
            return _json_value(
                {
                    "ok": True,
                    "product_id": product_id,
                    "state": str(engine.book.state),
                    "last_update_id": engine.book.last_update_id,
                    "received_at_ns": engine.book.received_at_ns,
                    "bids": _json_value([[price, size] for price, size in bids]),
                    "asks": _json_value([[price, size] for price, size in asks]),
                }
            )
        except BookNotReady:
            return {"ok": False, "reason": "book_not_ready"}

    def features(self, product_id: str, depth: int = 5) -> dict[str, Any]:
        if product_id not in self.engines:
            return {"ok": False, "reason": "unknown_product"}
        if not isinstance(depth, int) or isinstance(depth, bool) or not 1 <= depth <= 100:
            return {"ok": False, "reason": "bad_depth"}
        try:
            bids, asks = self.engines[product_id].book.depth(depth)
            best_bid = bids[0][0] if bids else None
            best_ask = asks[0][0] if asks else None
            bid_depth = sum((size for _, size in bids), Decimal(0))
            ask_depth = sum((size for _, size in asks), Decimal(0))
            total_depth = bid_depth + ask_depth
            return _json_value(
                {
                    "ok": True,
                    "product_id": product_id,
                    "depth": depth,
                    "mid": None
                    if best_bid is None or best_ask is None
                    else (best_bid + best_ask) / 2,
                    "spread": None if best_bid is None or best_ask is None else best_ask - best_bid,
                    "depth_imbalance": (
                        None
                        if not total_depth
                        else ((bid_depth - ask_depth) / total_depth).quantize(Decimal("0.000001"))
                    ),
                }
            )
        except BookNotReady:
            return {"ok": False, "reason": "book_not_ready"}

    def place_order(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {"ok": False, "reason": "bad_payload"}
        reasoning = payload.get("reasoning")
        if not isinstance(reasoning, str) or not reasoning.strip():
            return {"ok": False, "reason": "reasoning_required"}
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
            if (
                price is None
                and (self.account_id, order_request.client_order_id) not in self.writer.by_key
            ):
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
        if self.writer.risk_limits is None:
            return {"ok": False, "reason": "risk_limits_missing"}
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
                **order.reject_details,
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
        balances, sequence = self.writer.balance_snapshot(self.account_id)
        positions = {
            instrument.base: balances.get(instrument.base, Decimal(0))
            for instrument in self.writer.instruments.values()
        }
        fills: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        unavailable_reason = None
        if self.writer.ledger is None:
            unavailable_reason = "ledger_unavailable"
        else:
            try:
                rows = self.writer.ledger.portfolio_fills(self.account_id, sequence or 0)
            except Exception:
                # A failed read cannot turn missing lifetime history into zero P&L.
                unavailable_reason = "ledger_unavailable"
            else:
                for row in rows:
                    fills[row["product_id"]].append(row)
        pnl = {}
        for product, instrument in self.writer.instruments.items():
            mark = None
            engine = self.engines.get(product)
            if (
                engine is not None
                and engine.book.venue == "gemini"
                and self.writer.gate.check(product, self.clock_ns()).open
            ):
                try:
                    mark = engine.book.mid()
                except BookNotReady:
                    pass
            pnl[instrument.base] = position_pnl(
                fills[product],
                positions[instrument.base],
                mark,
                instrument.quote,
                unavailable_reason=unavailable_reason,
            )
        return {
            "ok": True,
            "account_id": self.account_id,
            "balances": _json_value(balances),
            "positions": _json_value(positions),
            "open_orders": _json_value([order.to_dict() for order in orders]),
            "cost_basis_method": "weighted_average",
            "ledger_sequence": sequence,
            "pnl": _json_value(pnl),
        }

    def cancel(
        self, *, order_id: str | None = None, client_order_id: str | None = None
    ) -> dict[str, Any]:
        if self.account_id in self.writer.halted_accounts:
            return {"ok": False, "reason": "account_halted"}
        if self.writer.halted:
            return {"ok": False, "reason": "writer_halted"}
        if order_id is None and client_order_id is None:
            return {"ok": False, "reason": "missing_order_reference"}
        resolved_id = order_id or self.writer.by_key.get((self.account_id, client_order_id or ""))
        order = self.writer.orders.get(resolved_id) if resolved_id else None
        if order is None or order.account_id != self.account_id:
            return {"ok": False, "reason": "unknown_order"}
        if order.status.terminal:
            return {"ok": False, "reason": "order_terminal"}
        if order.cancel_requested_at_ns is not None:
            return {"ok": False, "reason": "cancel_already_requested"}
        try:
            order = self.writer.request_cancel(
                self.account_id,
                self.clock_ns(),
                order_id=order_id,
                client_order_id=client_order_id,
            )
        except LedgerHalted:
            return {"ok": False, "reason": "ledger_halted"}
        except (OrderError, RuntimeError):
            return {"ok": False, "reason": "cancel_failed"}
        return {"ok": True, "status": "requested", "order": _json_value(order.to_dict())}

    def open_orders(self) -> dict[str, Any]:
        if self.account_id in self.writer.halted_accounts:
            return {"ok": False, "reason": "account_halted"}
        if self.writer.halted:
            return {"ok": False, "reason": "writer_halted"}
        return {
            "ok": True,
            "orders": _json_value(
                [order.to_dict() for order in self.writer.open_orders(self.account_id)]
            ),
        }

    def history(self, limit: int = 100) -> dict[str, Any]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            return {"ok": False, "reason": "bad_limit"}
        if self.account_id in self.writer.halted_accounts:
            return {"ok": False, "reason": "account_halted"}
        if self.writer.halted:
            return {"ok": False, "reason": "writer_halted"}
        if self.writer.ledger is None:
            return {"ok": False, "reason": "ledger_unavailable"}
        try:
            fills = self.writer.ledger.history(self.account_id, limit)
        except LedgerHalted:
            return {"ok": False, "reason": "ledger_halted"}
        except (RuntimeError, ValueError):
            return {"ok": False, "reason": "history_unavailable"}
        return {"ok": True, "fills": _json_value(fills)}


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
        reasoning: str = "",
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
        payload["reasoning"] = reasoning
        return tools.place_order(payload)

    @server.tool()
    def features(product_id: str, depth: int = 5) -> dict[str, Any]:
        return tools.features(product_id, depth)

    @server.tool()
    def cancel(order_id: str | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        return tools.cancel(order_id=order_id, client_order_id=client_order_id)

    @server.tool()
    def open_orders() -> dict[str, Any]:
        return tools.open_orders()

    @server.tool()
    def history(limit: int = 100) -> dict[str, Any]:
        return tools.history(limit)

    @server.tool()
    def portfolio() -> dict[str, Any]:
        """Balances, positions, open orders and weighted-average P&L by base asset.

        Realised and unrealised P&L are gross; net P&L subtracts the separately reported fees.
        Unavailable marks or accounting values are null, with a reason in each asset's P&L.
        """
        return tools.portfolio()

    return server


async def run_server_async(
    tools: ExchangeTools,
    *,
    port: int = 8765,
    bind_retries: int = 5,
    retry_delay: float = 0.1,
) -> None:
    """Run the private HTTP transport until the owning task is cancelled."""
    for attempt in range(bind_retries):
        try:
            await create_server(tools, port=port).run_streamable_http_async()
            return
        except OSError:
            if attempt + 1 == bind_retries:
                raise
            await asyncio.sleep(retry_delay)


def run_server(tools: ExchangeTools, *, port: int = 8765) -> None:
    """Run the private HTTP transport; the caller owns exchange construction and lifecycle."""
    create_server(tools, port=port).run(transport="streamable-http")


__all__ = ["ExchangeTools", "RiskLimits", "create_server", "run_server"]
