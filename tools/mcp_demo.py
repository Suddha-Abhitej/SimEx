"""Record a private MCP tool transcript from a running simulated exchange.

The caller supplies the live runner's MCP tools; the output belongs under ``data/`` and must not
be committed because it contains Gemini-derived prices.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from simex.mcp import ExchangeTools


def record_demo(
    tools: ExchangeTools,
    output: str | Path,
    *,
    product_id: str = "BTC-USD",
    order: dict[str, Any] | None = None,
    reconciliation: dict[str, Any] | None = None,
) -> None:
    """Write quote, order, portfolio, and reconciliation results as one private JSON transcript."""
    order = order or {
        "client_order_id": "demo-1",
        "product_id": product_id,
        "side": "buy",
        "type": "limit",
        "quantity": "0.00001",
        "limit_price": "1.00",
    }
    transcript = [
        {"tool": "quote", "result": tools.quote(product_id)},
        {"tool": "place_order", "result": tools.place_order(order)},
        {"tool": "portfolio", "result": tools.portfolio()},
    ]
    if reconciliation is not None:
        if reconciliation.get("mismatches") != 0:
            raise AssertionError("demo reconciliation reported mismatches")
        transcript.append({"reconciliation": reconciliation})
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(transcript, indent=2) + "\n", encoding="utf-8")
