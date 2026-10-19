"""Check the live runner's merge order against the recordings (ADR 0015).

The runner writes, per pair and UTC hour, how many md.level2 and md.trades records it released
and a SHA-256 over their (topic, partition, offset) in release order. This recomputes both from
the recordings, read in replay order, and compares every complete hour. A match means the live
engine saw exactly the sequence a replay of that hour feeds it. Prints counts only.

Usage:
    python tools/merge_replay_check.py
    python tools/merge_replay_check.py --digests data/exchange/merge-digests.jsonl \
        --root data/recordings
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from simex.exchange.runner import HOUR_NS, replay_digests
from simex.recorder.replay import read


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--digests", default="data/exchange/merge-digests.jsonl")
    parser.add_argument("--root", default="data/recordings")
    args = parser.parse_args()
    live: dict[tuple[str, int], dict] = {}
    partial = 0
    with open(args.digests, encoding="utf-8") as f:
        for line in f:
            entry = json.loads(line)
            if not entry["complete"]:
                partial += 1
                continue
            live[(entry["product_id"], entry["hour_start_ns"])] = entry  # the latest run wins
    if not live:
        print(f"no complete hours in {args.digests} ({partial} partial)")
        return 1
    failures = 0
    for (product, start), entry in sorted(live.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        recorded = read(
            args.root,
            start,
            start + HOUR_NS,
            products=[product],
            channels=["level2", "market_trades"],
        )
        expected = replay_digests(recorded, [product]).get((product, start), (0, None))
        hour = datetime.fromtimestamp(start / 1e9, UTC).strftime("%Y-%m-%d %H:00")
        ok = expected == (entry["records"], entry["sha256"])
        failures += not ok
        verdict = "match" if ok else "MISMATCH"
        print(
            f"{hour} {product}: live {entry['records']} records ({entry['late']} late), "
            f"recorded {expected[0]}: {verdict}"
        )
    print(
        f"{len(live)} complete hours, {partial} partial skipped: "
        + ("PASS" if not failures else f"FAIL ({failures} mismatched)")
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
