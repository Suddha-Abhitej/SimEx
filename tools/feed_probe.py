"""Measure the Coinbase market-data feed before building on it.

Connects to the public Coinbase Advanced Trade WebSocket (no account or key), subscribes to
level2, market_trades and heartbeats for the SimEx pairs, and reports after N seconds:

- messages and individual events per second, per channel
- raw bytes per second and an extrapolated GB/day (raw JSON and gzip-compressed)
- sequence gaps (dropped messages)
- feed lag: exchange timestamp vs local receive time (needs an NTP-synced clock)

Usage:
    pip install websockets
    python tools/feed_probe.py --seconds 300

Run it a few times (quiet hours, busy hours) to size Kafka retention and recordings.
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import statistics
import time
from collections import defaultdict
from datetime import UTC, datetime

import websockets

URL = "wss://advanced-trade-ws.coinbase.com"
DEFAULT_PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD"]
CHANNELS = ["level2", "market_trades", "heartbeats"]


def parse_ts(value: str) -> float:
    """Parse Coinbase ISO timestamps (nanosecond precision, Z suffix) to epoch seconds."""
    value = value.rstrip("Z")
    if "." in value:
        head, frac = value.split(".", 1)
        value = f"{head}.{frac[:6]}"
    return datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp()


def count_events(msg: dict) -> int:
    """Count the individual updates or trades inside one message."""
    total = 0
    for event in msg.get("events", []):
        if "updates" in event:
            total += len(event["updates"])
        elif "trades" in event:
            total += len(event["trades"])
        else:
            total += 1
    return total


def is_snapshot(msg: dict) -> bool:
    return any(e.get("type") == "snapshot" for e in msg.get("events", []))


async def probe(pairs: list[str], seconds: int) -> None:
    msgs: dict[str, int] = defaultdict(int)
    events: dict[str, int] = defaultdict(int)
    raw_bytes = 0
    snapshot_bytes = 0
    sample = bytearray()
    lags: list[float] = []
    gaps = 0
    last_seq: int | None = None

    async with websockets.connect(URL, max_size=None, open_timeout=15) as ws:
        for channel in CHANNELS:
            subscribe = {"type": "subscribe", "product_ids": pairs, "channel": channel}
            await ws.send(json.dumps(subscribe))
        print(f"Connected. Sampling {seconds}s of {', '.join(pairs)} ...")

        deadline = time.monotonic() + seconds
        while (remaining := deadline - time.monotonic()) > 0:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                break
            received = time.time()
            msg = json.loads(raw)
            size = len(raw if isinstance(raw, bytes) else raw.encode())

            seq = msg.get("sequence_num")
            if isinstance(seq, int):
                if last_seq is not None and seq != last_seq + 1:
                    gaps += 1
                last_seq = seq

            channel = msg.get("channel", "unknown")
            if is_snapshot(msg):
                snapshot_bytes += size
                continue

            msgs[channel] += 1
            events[channel] += count_events(msg)
            raw_bytes += size
            if len(sample) < 5_000_000:
                sample += raw if isinstance(raw, bytes) else raw.encode()
            if "timestamp" in msg and channel != "subscriptions":
                lags.append(received - parse_ts(msg["timestamp"]))

    elapsed = float(seconds)
    ratio = len(gzip.compress(bytes(sample))) / len(sample) if sample else 1.0
    raw_gb_day = raw_bytes / elapsed * 86_400 / 1e9

    print("\nPer channel (snapshots excluded):")
    for channel in sorted(msgs):
        print(
            f"  {channel:<14} {msgs[channel] / elapsed:8.1f} msg/s"
            f"  {events[channel] / elapsed:9.1f} events/s"
        )
    print(f"\nRaw throughput:    {raw_bytes / elapsed / 1024:8.1f} KB/s")
    print(f"Raw JSON per day:  {raw_gb_day:8.2f} GB/day")
    print(f"gzip ratio:        {ratio:8.2f}  (zstd Parquet is usually smaller still)")
    print(f"Compressed/day:    {raw_gb_day * ratio:8.2f} GB/day")
    print(f"Initial snapshots: {snapshot_bytes / 1e6:8.2f} MB")
    print(f"Sequence gaps:     {gaps:8d}")
    if lags:
        lags.sort()
        p50 = statistics.median(lags)
        p99 = lags[min(len(lags) - 1, int(len(lags) * 0.99))]
        print(f"Feed lag p50/p99:  {p50 * 1000:8.0f} / {p99 * 1000:.0f} ms  (clock must be synced)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=int, default=300, help="sampling time (default 300)")
    parser.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS, help="product ids")
    args = parser.parse_args()
    asyncio.run(probe(args.pairs, args.seconds))


if __name__ == "__main__":
    main()
