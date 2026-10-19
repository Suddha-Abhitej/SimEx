"""Check that each Kafka partition in the recordings is in receive order (ADR 0015).

The live exchange runner's merge assumes that, within a partition of md.level2 or md.trades, a
later offset was never received earlier. This reads the recordings in Kafka offset order per
partition and reports every record whose received_at_ns fell below the one before it: how many,
of which type (data, audit, resync), in which UTC hour, and how far below. Records more than the
hold-back below are the ones the runner can see late. Counts and milliseconds only.

Usage:
    python tools/partition_order_check.py --start 2026-10-19T17:00 --hours 3
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

from simex.recorder.replay import read

MS = 1_000_000
HOUR_NS = 3600 * 1_000_000_000


def order_breaks(records, thresholds_ms=(100, 210)):
    """records: anything with topic, partition, offset, received_at_ns and type. Returns
    (total records, Counter of breaks by (type, hour start), Counter of breaks over each
    threshold, the largest backstep in ms)."""
    by_partition = defaultdict(list)
    total = 0
    for r in records:
        by_partition[(r.topic, r.partition)].append((r.offset, r.received_at_ns, r.type))
        total += 1
    breaks: Counter = Counter()
    over: Counter = Counter()
    largest = 0.0
    for rows in by_partition.values():
        rows.sort()
        last = None
        for _, received, kind in rows:
            if last is not None and received < last:
                back_ms = (last - received) / MS
                breaks[(kind, received // HOUR_NS * HOUR_NS)] += 1
                largest = max(largest, back_ms)
                for t in thresholds_ms:
                    if back_ms > t:
                        over[t] += 1
            last = received if last is None else max(last, received)
    return total, breaks, over, largest


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
    total, breaks, over, largest = order_breaks(records)
    if not total:
        print("no recordings in that window")
        return 1
    print(f"{start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M} UTC, {total} records")
    print(f"out of receive order: {sum(breaks.values())}; largest step back {largest:.0f} ms")
    for t, n in sorted(over.items()):
        print(f"  more than {t} ms back: {n}")
    for (kind, hour), n in sorted(breaks.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        print(f"  {datetime.fromtimestamp(hour / 1e9, UTC):%Y-%m-%d %H:00} {kind}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
