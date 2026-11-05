"""Decimal-only weighted-average position accounting (ADR 0021)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any

ZERO = Decimal(0)


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError("P&L monetary inputs must be Decimal")
    return value


def position_pnl(
    fills: Iterable[Mapping[str, Any]],
    quantity: Decimal,
    mark: Decimal | None,
    quote_asset: str,
    *,
    unavailable_reason: str | None = None,
) -> dict[str, Any]:
    """Fold a product's complete, ordered, committed fills; fees stay outside cost basis.

    Unexplained inventory or sales of inventory without a known acquisition cost make the
    asset's basis and lifetime P&L unavailable. Fees remain known from the committed fills.
    """
    quantity = _decimal(quantity)
    if mark is not None:
        mark = _decimal(mark)
    result = {
        "quantity": quantity,
        "quote_asset": quote_asset,
        "mark": mark,
        "average_cost_basis": None,
        "realised_pnl": None,
        "unrealised_pnl": None,
        "total_fees_paid": None,
        "net_pnl": None,
        "reason": unavailable_reason,
    }
    if unavailable_reason is not None:
        return result

    held = cost = realised = fees = ZERO
    known = True
    for fill in fills:
        price = _decimal(fill["price"])
        size = _decimal(fill["quantity"])
        fee = _decimal(fill["fee"])
        if fill["fee_asset"] != quote_asset:
            raise ValueError("P&L fees must be denominated in the product's quote asset")
        fees += fee
        if fill["side"] == "buy":
            held += size
            cost += price * size
        elif fill["side"] == "sell":
            if size > held:
                known = False
            if known:
                # Consume the whole pool on exit, including any division remainder.
                removed_cost = cost if size == held else cost * size / held
                realised += price * size - removed_cost
                cost -= removed_cost
            held -= size
        else:
            raise ValueError(f"unknown fill side {fill['side']!r}")

    result["total_fees_paid"] = fees
    if not known or held != quantity:
        result["reason"] = "unknown_cost_basis"
        return result

    result["average_cost_basis"] = cost / held if held else None
    result["realised_pnl"] = realised
    if mark is None:
        result["reason"] = "mark_unavailable"
    else:
        unrealised = quantity * mark - cost
        result["unrealised_pnl"] = unrealised
        result["net_pnl"] = realised + unrealised - fees
    return result
