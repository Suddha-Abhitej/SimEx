"""Maker and taker fees (ADR 0014).

Defaults are Gemini's API / ActiveTrader entry tier (30-day volume >= $0): 60 bps maker, 120 bps
taker, schedule dated 2026-09-01, https://www.gemini.com/fees/api-fee-schedule. A personal
account below the first volume threshold pays exactly this. The tier is configurable
(EXCHANGE_MAKER_FEE_BPS, EXCHANGE_TAKER_FEE_BPS) to study other tiers; it never changes with
simulated volume in v1.

A fee is charged in the quote currency and rounded up to 1e-8 of it, so rounding never favours
the agent. Not to cents: a one-cent minimum would exceed the whole proceeds of a minimum-size sale
(0.00001 BTC at $100 earns $0.001). With 1e-8, the smallest possible notional (BTC's minimum
size at a one-cent price, 1e-7) still pays a fee below its proceeds; settlement checks it.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_UP, Decimal
from enum import StrEnum

from .instruments import BPS

FEE_SCHEDULE_URL = "https://www.gemini.com/fees/api-fee-schedule"
FEE_SCHEDULE_DATE = "2026-09-01"


class Liquidity(StrEnum):
    MAKER = "maker"  # filled while resting on the book
    TAKER = "taker"  # filled on arrival, against the book


@dataclass(frozen=True)
class FeeSchedule:
    maker_bps: Decimal = Decimal("60")
    taker_bps: Decimal = Decimal("120")
    fee_increment: Decimal = Decimal("0.00000001")

    def __post_init__(self) -> None:
        if self.maker_bps < 0 or self.taker_bps < 0:
            raise ValueError("fees can't be negative (no maker rebates in v1)")

    def rate(self, liquidity: Liquidity) -> Decimal:
        bps = self.maker_bps if liquidity is Liquidity.MAKER else self.taker_bps
        return bps / BPS

    def fee(self, notional: Decimal, liquidity: Liquidity) -> Decimal:
        """The fee on a fill of this notional, rounded up to the fee increment."""
        raw = notional * self.rate(liquidity)
        return raw.quantize(self.fee_increment, rounding=ROUND_UP)

    @property
    def max_rate(self) -> Decimal:
        return max(self.maker_bps, self.taker_bps) / BPS

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> FeeSchedule:
        env = os.environ if env is None else env
        d = cls()
        maker = env.get("EXCHANGE_MAKER_FEE_BPS", "").strip()
        taker = env.get("EXCHANGE_TAKER_FEE_BPS", "").strip()
        return cls(
            maker_bps=Decimal(maker) if maker else d.maker_bps,
            taker_bps=Decimal(taker) if taker else d.taker_bps,
        )
