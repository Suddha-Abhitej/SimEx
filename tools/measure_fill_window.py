"""Measure W, the fill engine's shrink/print matching window, from recordings (ADR 0014).

For each pair: the distribution of print time minus the time of the level shrink it caused, and
for candidate windows, the share of print quantity matched and the share of shrink quantity left
unmatched (what the engine would count as cancels). Prints counts and milliseconds only.

Usage:
    python tools/measure_fill_window.py --start 2026-10-16T00:00 --hours 24
    python tools/measure_fill_window.py --root data/recordings --start ... --hours 6
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

from simex.exchange.window import format_report, measure
from simex.recorder.replay import read


def parse_time(text: str) -> datetime:
    when = datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default="data/recordings")
    parser.add_argument("--start", required=True, help="window start, ISO time (UTC by default)")
    parser.add_argument("--hours", type=float, default=1.0)
    args = parser.parse_args()
    start = parse_time(args.start)
    end = start + timedelta(hours=args.hours)
    records = read(args.root, start, end, channels=["level2", "market_trades"])
    pairs = measure(rec.value for rec in records)
    if not pairs:
        print("no recordings in that window")
        return 1
    print(f"{start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M} UTC")
    print(format_report(pairs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
