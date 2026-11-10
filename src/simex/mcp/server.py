"""Private MCP transport for the shared exchange tools (ADR 0018)."""

from __future__ import annotations

import asyncio
from typing import Any

from mcp.server.fastmcp import FastMCP

from simex.exchange.tools import ExchangeTools, RiskLimits


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
        time_in_force: str = "GTC",
    ) -> dict[str, Any]:
        payload = {
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": side,
            "type": type,
            "quantity": quantity,
            "time_in_force": time_in_force,
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
