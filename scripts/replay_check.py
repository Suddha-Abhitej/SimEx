"""Replay recorded market data through the order book and check it (ADR 0013).

Reads data/recordings for a window, rebuilds every product's book with the same decoder and
OrderBook the live service uses, and checks: books SYNCED after their first snapshot (outside
resync windows), every recorded aligned audit still matches, recorded and replayed audit counts
agree, and the window's size on disk as GB/day. Prints counts only, no prices (ADR 0010).

Usage:
    python scripts/replay_check.py --start 2026-10-16T17:00 --hours 1
    python scripts/replay_check.py --last-hours 1          # the last complete hour(s)
Exit code 0 on PASS, 1 on FAIL.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

from simex.recorder.check import check, format_report


def parse_time(text: str) -> datetime:
    when = datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default="data/recordings")
    parser.add_argument("--start", help="window start, ISO time (UTC unless an offset is given)")
    parser.add_argument("--hours", type=float, default=1.0, help="window length (default 1)")
    parser.add_argument(
        "--last-hours", type=int, help="the last N complete UTC hours (instead of --start)"
    )
    parser.add_argument("--venue", default="gemini")
    args = parser.parse_args()
    if args.last_hours:
        end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(hours=args.last_hours)
    elif args.start:
        start = parse_time(args.start)
        end = start + timedelta(hours=args.hours)
    else:
        parser.error("give --start or --last-hours")
    report = check(
        args.root, int(start.timestamp() * 1e9), int(end.timestamp() * 1e9), venue=args.venue
    )
    print(f"{start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M} UTC")
    print(format_report(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
