"""Deterministic baseline agents using the same MCP exchange façade as other agents."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any

from simex.mcp import ExchangeTools


@dataclass(frozen=True)
class BaselineSpec:
    kind: str
    account_id: str
    seed: int


class BaselineAgent:
    def __init__(
        self,
        spec: BaselineSpec,
        tools: ExchangeTools,
        *,
        cadence_seconds: float = 60.0,
        dca_quote_amount: Decimal = Decimal("100"),
    ) -> None:
        if cadence_seconds <= 0:
            raise ValueError("agent cadence must be positive")
        if spec.kind not in {"buy_and_hold", "dca", "random", "momentum"}:
            raise ValueError(f"unknown baseline agent {spec.kind!r}")
        self.spec = spec
        self.tools = tools
        self.cadence_seconds = cadence_seconds
        self.dca_quote_amount = dca_quote_amount
        self.random = random.Random(spec.seed)
        self._fired = False
        self._tick_count = 0
        self._previous_mid: Decimal | None = None

    def _order(
        self, side: str, quantity: Decimal, price: Decimal, reasoning: str
    ) -> dict[str, Any]:
        instrument = self.tools.writer.instruments[self._product()]
        quantity = (quantity / instrument.size_increment).to_integral_value(
            rounding=ROUND_DOWN
        ) * instrument.size_increment
        if quantity < instrument.min_size:
            return {"ok": False, "reason": "below_min_size"}
        return self.tools.place_order(
            {
                "client_order_id": f"{self.spec.kind}-{self._tick_count}",
                "product_id": self._product(),
                "side": side,
                "type": "limit",
                "quantity": str(quantity),
                "limit_price": str(price),
                "reasoning": reasoning,
            }
        )

    def _product(self) -> str:
        return next(iter(self.tools.engines))

    def _sized_quantity(self, price: Decimal, quote_amount: Decimal) -> Decimal:
        return min(
            self.tools.limits.max_quantity[self._product()],
            self.tools.limits.max_notional[self._product()] / price,
            quote_amount / price,
        )

    def tick(self) -> dict[str, Any]:
        product = self._product()
        quote = self.tools.quote(product, depth=5)
        if not quote.get("ok") or not quote.get("asks"):
            return {"ok": False, "reason": "book_not_ready"}
        ask = Decimal(quote["asks"][0][0])
        bid = Decimal(quote["bids"][0][0]) if quote.get("bids") else ask
        result: dict[str, Any] = {"ok": True, "action": "none"}
        if self.spec.kind == "buy_and_hold" and not self._fired:
            result = self._order(
                "buy",
                self._sized_quantity(
                    ask,
                    self.tools.writer.accounts.balance(
                        self.spec.account_id, self.tools.writer.instruments[product].quote
                    ).available,
                ),
                ask,
                "buy_and_hold: buy once at the current best ask",
            )
        elif self.spec.kind == "dca":
            result = self._order(
                "buy",
                self._sized_quantity(ask, self.dca_quote_amount),
                ask,
                "dca: buy the configured quote amount at the current best ask",
            )
        elif self.spec.kind == "random":
            side = self.random.choice(("buy", "sell"))
            price = ask if side == "buy" else bid
            result = self._order(
                side,
                self._sized_quantity(
                    price, self.dca_quote_amount * Decimal(str(self.random.random()))
                ),
                price,
                f"random: seed {self.spec.seed} selected {side} and a bounded size",
            )
        elif self.spec.kind == "momentum":
            features = self.tools.features(product, depth=5)
            if features.get("ok"):
                mid = Decimal(features["mid"]) if features.get("mid") is not None else None
                imbalance = (
                    Decimal(features["depth_imbalance"])
                    if features.get("depth_imbalance") is not None
                    else Decimal(0)
                )
                if mid is not None and self._previous_mid is not None:
                    if imbalance >= Decimal("0.25") and mid > self._previous_mid:
                        result = self._order(
                            "buy",
                            self._sized_quantity(ask, self.dca_quote_amount),
                            ask,
                            "momentum: buy because mid rose and depth imbalance is at least 0.25",
                        )
                    elif imbalance <= Decimal("-0.25") and mid < self._previous_mid:
                        result = self._order(
                            "sell",
                            self._sized_quantity(bid, self.dca_quote_amount),
                            bid,
                            "momentum: sell because mid fell and depth imbalance is at most -0.25",
                        )
                self._previous_mid = mid
        self._fired = True
        self._tick_count += 1
        return result

    async def run_forever(self) -> None:
        while True:
            self.tick()
            await asyncio.sleep(self.cadence_seconds)


__all__ = ["BaselineAgent", "BaselineSpec"]
