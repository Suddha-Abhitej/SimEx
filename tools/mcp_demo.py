"""Record a private MCP tool transcript from a running simulated exchange.

The caller supplies the live runner's MCP tools; the output belongs under ``data/`` and must not
be committed because it contains Gemini-derived prices.
"""

from __future__ import annotations

import json
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

from simex.ledger import ReconciliationJob
from simex.mcp import ExchangeTools


def record_demo(
    tools: ExchangeTools,
    output: str | Path,
    *,
    product_id: str = "BTC-USD",
    order: dict[str, Any] | None = None,
    reconciliation: ReconciliationJob | None = None,
) -> None:
    """Write quote, order, portfolio, and reconciliation results as one private JSON transcript."""
    quote = tools.quote(product_id)
    if not quote.get("ok") or not quote.get("asks"):
        raise RuntimeError("demo quote has no ask")
    best_ask = Decimal(quote["asks"][0][0])
    if order is None:
        instrument = tools.writer.instruments[product_id]
        available = tools.writer.accounts.balance(tools.account_id, instrument.quote).available
        max_quantity = tools.limits.max_quantity[product_id]
        max_notional = tools.limits.max_notional[product_id]
        quantity = min(
            max_quantity,
            max_notional / best_ask,
            available / (best_ask * (1 + tools.writer.fees.max_rate)),
        )
        quantity = (quantity / instrument.size_increment).to_integral_value(
            rounding=ROUND_DOWN
        ) * instrument.size_increment
        if quantity < instrument.min_size:
            raise RuntimeError("demo balance or risk limits cannot fund one minimum order")
        order = {
            "client_order_id": "demo-1",
            "product_id": product_id,
            "side": "buy",
            "type": "limit",
            "quantity": str(quantity),
            "limit_price": str(best_ask),
        }
    transcript = [
        {"tool": "quote", "result": quote},
        {"tool": "place_order", "result": tools.place_order(order)},
        {"tool": "portfolio", "result": tools.portfolio()},
    ]
    if reconciliation is not None:
        findings = reconciliation.run_once()
        result = {"mismatches": len(findings)}
        if result["mismatches"] != 0:
            raise AssertionError("demo reconciliation reported mismatches")
        transcript.append({"reconciliation": result})
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(transcript, indent=2) + "\n", encoding="utf-8")
