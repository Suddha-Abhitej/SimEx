"""Account-level order and signed position limits (ADR 0022)."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType


@dataclass(frozen=True)
class RiskRefusal:
    reason: str
    details: Mapping[str, str]


@dataclass(frozen=True)
class RiskLimits:
    max_quantity: Mapping[str, Decimal]
    max_notional: Mapping[str, Decimal]
    max_position: Mapping[str, Decimal]

    def __post_init__(self) -> None:
        if not set(self.max_quantity) == set(self.max_notional) == set(self.max_position):
            raise ValueError("risk limits must cover the same products")
        for name in ("max_quantity", "max_notional", "max_position"):
            values = dict(getattr(self, name))
            if any(
                not isinstance(v, Decimal) or not v.is_finite() or v <= 0 for v in values.values()
            ):
                raise ValueError("risk limits must be positive finite Decimals")
            object.__setattr__(self, name, MappingProxyType(values))

    @classmethod
    def from_env(cls, products: Sequence[str], env: Mapping[str, str] | None = None) -> RiskLimits:
        env = os.environ if env is None else env

        def read(kind: str, product: str) -> Decimal:
            suffix = product.replace("-", "_")
            name = f"EXCHANGE_MAX_{kind}_{suffix}"
            alias = f"EXCHANGE_MCP_MAX_{kind}_{suffix}" if kind != "POSITION" else None
            value = env.get(name, "").strip()
            legacy = env.get(alias, "").strip() if alias is not None else ""
            if not value and not legacy:
                raise ValueError(f"set {name}: risk limits must be explicit")
            try:
                parsed = Decimal(value or legacy)
                old = Decimal(legacy) if legacy else parsed
            except InvalidOperation:
                raise ValueError(f"{name} must be a positive finite Decimal") from None
            if not parsed.is_finite() or parsed <= 0 or not old.is_finite() or old <= 0:
                raise ValueError(f"{name} must be a positive finite Decimal")
            if value and legacy and parsed != old:
                raise ValueError(f"conflicting risk limits: {name} and {alias}")
            return parsed

        return cls(
            {p: read("QUANTITY", p) for p in products},
            {p: read("NOTIONAL", p) for p in products},
            {p: read("POSITION", p) for p in products},
        )

    def order_refusal(
        self, product: str, quantity: Decimal, price: Decimal | None
    ) -> RiskRefusal | None:
        if product not in self.max_quantity:
            return RiskRefusal(
                "product_not_allowed", {"rule": "allowed_product", "product_id": product}
            )
        if quantity > self.max_quantity[product]:
            return self._refusal(
                "quantity_limit", "max_quantity", product, quantity, self.max_quantity[product]
            )
        if price is not None and price * quantity > self.max_notional[product]:
            return self._refusal(
                "notional_limit",
                "max_notional",
                product,
                price * quantity,
                self.max_notional[product],
            )
        return None

    def check(self, product_id: str, quantity: Decimal, price: Decimal | None) -> str | None:
        refusal = self.order_refusal(product_id, quantity, price)
        return None if refusal is None else refusal.reason

    def position_refusal(
        self,
        product: str,
        side: str,
        position: Decimal,
        same_side_remaining: Decimal,
        quantity: Decimal,
    ) -> RiskRefusal | None:
        limit = self.max_position[product]
        projected = (
            position + same_side_remaining + quantity
            if side == "buy"
            else position - same_side_remaining - quantity
        )
        if (side == "buy" and projected > limit) or (side == "sell" and projected < -limit):
            refusal = self._refusal("position_limit", "max_position", product, projected, limit)
            return RiskRefusal(
                refusal.reason,
                {**refusal.details, "minimum": str(-limit), "maximum": str(limit), "side": side},
            )
        return None

    @staticmethod
    def _refusal(
        reason: str, rule: str, product: str, attempted: Decimal, limit: Decimal
    ) -> RiskRefusal:
        return RiskRefusal(
            reason,
            {"rule": rule, "product_id": product, "limit": str(limit), "attempted": str(attempted)},
        )
