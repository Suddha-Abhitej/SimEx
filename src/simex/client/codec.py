"""Wire conversion only: no order, grid, risk or reason policy."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

ORDER_MONEY = frozenset(
    {"quantity", "limit_price", "filled_quantity", "remaining_quantity", "average_price", "fees"}
)
FILL_MONEY = frozenset({"price", "quantity", "fee"})
DETAIL_MONEY = frozenset({"limit", "attempted", "minimum", "maximum"})
PNL_MONEY = frozenset(
    {
        "quantity",
        "mark",
        "average_cost_basis",
        "realised_pnl",
        "unrealised_pnl",
        "total_fees_paid",
        "net_pnl",
    }
)


def money_input(field: str, value: Decimal | str) -> str:
    if not isinstance(value, (Decimal, str)):
        raise TypeError(f'{field} must be Decimal or str; pass Decimal("0.1") or "0.1"')
    return str(value)


def loads(body: str | bytes) -> Any:
    return json.loads(body, parse_float=Decimal)


def money_fields(body: dict, fields: frozenset[str]) -> dict:
    return {
        key: Decimal(value) if key in fields and value is not None else value
        for key, value in body.items()
    }


def order(body: dict) -> dict:
    result = money_fields(body, ORDER_MONEY)
    if "reject_details" in result:
        result["reject_details"] = money_fields(result["reject_details"], DETAIL_MONEY)
    return result


def response(body: dict, operation: str) -> dict:
    result = money_fields(body, DETAIL_MONEY)
    if "order" in result:
        result["order"] = order(result["order"])
    for key in ("orders", "open_orders"):
        if key in result:
            result[key] = [order(item) for item in result[key]]
    if "fills" in result:
        result["fills"] = [money_fields(item, FILL_MONEY) for item in result["fills"]]
    if operation == "quote":
        for side in ("bids", "asks"):
            if side in result:
                result[side] = [[Decimal(price), Decimal(size)] for price, size in result[side]]
    elif operation == "features":
        result = money_fields(result, frozenset({"mid", "spread", "depth_imbalance"}))
    elif operation == "portfolio":
        for key in ("balances", "positions"):
            if key in result:
                result[key] = {asset: Decimal(value) for asset, value in result[key].items()}
        if "pnl" in result:
            result["pnl"] = {
                asset: money_fields(values, PNL_MONEY) for asset, values in result["pnl"].items()
            }
    return result


def frame(body: dict) -> dict:
    if body.get("type") == "fill":
        return {**body, "fill": money_fields(body["fill"], FILL_MONEY)}
    # Unknown frame types and fields are passed through, never treated as protocol errors.
    return body
