"""Per-pair trading rules: price tick, size increment, minimum size, market-order band (ADR 0014).

An order must sit exactly on these grids; anything else is rejected, never rounded, because
rounding would silently change what the agent asked for (Gemini enforces its increments the same
way). The values mirror Gemini's /v1/symbols/details for each pair; tools/gemini_symbols.py
fetches them and reports any difference from this table.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

BPS = Decimal("10000")


@dataclass(frozen=True)
class Instrument:
    product_id: str  # e.g. "BTC-USD"
    base: str  # e.g. "BTC"
    quote: str  # e.g. "USD"
    price_tick: Decimal  # quote_increment: prices are multiples of this
    size_increment: Decimal  # tick_size: quantities are multiples of this
    min_size: Decimal  # min_order_size
    # A market order walks the book only this far from the best price when it takes effect;
    # whatever doesn't fill inside the band expires (ADR 0014).
    market_band_bps: Decimal = Decimal("50")

    def on_price_grid(self, price: Decimal) -> bool:
        return price > 0 and price % self.price_tick == 0

    def on_size_grid(self, size: Decimal) -> bool:
        return size > 0 and size % self.size_increment == 0


def _instrument(product: str, tick: str, increment: str, minimum: str) -> Instrument:
    base, quote = product.split("-")
    return Instrument(product, base, quote, Decimal(tick), Decimal(increment), Decimal(minimum))


# Gemini's published values for the live pairs (ADR 0010, ADR 0014).
DEFAULT_INSTRUMENTS: dict[str, Instrument] = {
    i.product_id: i
    for i in (
        _instrument("BTC-USD", tick="0.01", increment="0.00000001", minimum="0.00001"),
        _instrument("ETH-USD", tick="0.01", increment="0.000001", minimum="0.001"),
        _instrument("SOL-USD", tick="0.001", increment="0.000001", minimum="0.001"),
    )
}


def instruments_from_env(env: Mapping[str, str] | None = None) -> dict[str, Instrument]:
    """The default table, with the market band overridable per pair:
    EXCHANGE_MARKET_BAND_BPS (all pairs) or EXCHANGE_MARKET_BAND_BPS_BTC_USD (one pair)."""
    env = os.environ if env is None else env
    out = {}
    for product, instrument in DEFAULT_INSTRUMENTS.items():
        band = env.get(f"EXCHANGE_MARKET_BAND_BPS_{product.replace('-', '_')}", "").strip()
        band = band or env.get("EXCHANGE_MARKET_BAND_BPS", "").strip()
        if band:
            value = Decimal(band)
            if value <= 0:
                raise ValueError(f"market band for {product} must be positive, got {band}")
            instrument = Instrument(**{**instrument.__dict__, "market_band_bps": value})
        out[product] = instrument
    return out
