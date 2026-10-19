"""Check simex.exchange's trading rules against Gemini's published symbol details (ADR 0014).

Fetches https://api.gemini.com/v1/symbols/details/<symbol> for each pair and compares the price
tick (quote_increment), size increment (tick_size) and minimum order size with
DEFAULT_INSTRUMENTS. Numbers are parsed as Decimal, never float. Exit code 1 on any difference.

Usage: python tools/gemini_symbols.py
"""

from __future__ import annotations

import json
import sys
import urllib.request
from decimal import Decimal

from simex.exchange.instruments import DEFAULT_INSTRUMENTS

URL = "https://api.gemini.com/v1/symbols/details/{symbol}"


def fetch(symbol: str) -> dict:
    with urllib.request.urlopen(URL.format(symbol=symbol), timeout=10) as response:
        return json.loads(response.read(), parse_float=Decimal, parse_int=Decimal)


def main() -> int:
    differences = 0
    print(f"{'pair':8} {'field':15} {'SimEx':>12} {'Gemini':>12}")
    for product, instrument in DEFAULT_INSTRUMENTS.items():
        details = fetch(product.replace("-", "").lower())
        for field, ours, key in (
            ("price tick", instrument.price_tick, "quote_increment"),
            ("size increment", instrument.size_increment, "tick_size"),
            ("min size", instrument.min_size, "min_order_size"),
        ):
            theirs = Decimal(str(details.get(key)))
            same = theirs == ours
            differences += not same
            mark = "" if same else "  <-- differs"
            print(f"{product:8} {field:15} {ours!s:>12} {theirs!s:>12}{mark}")
    print("OK: matches Gemini" if not differences else f"{differences} difference(s)")
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
