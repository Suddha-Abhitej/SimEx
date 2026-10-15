"""Watch the book service's Valkey records for N seconds and summarize them (ADR 0012).

Polls simex:book:<venue>:<product> every --interval seconds and reports, per product: how often
the record was usable by a reader (exists, SYNCED, not catching up, fresh), every state change
with how long the book spent outside SYNCED, level counts and the age of the last update. Then it
prints the book service's counters from /metrics. It prints no prices or sizes, so the output can
be pasted anywhere (ADR 0010).

Usage: python scripts/book_report.py --seconds 120 [--interval 0.25]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import urllib.request
from collections import defaultdict

import valkey.asyncio as valkey

from simex.book.record import book_key, usable


def scrape(url: str) -> list[str]:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            text = response.read().decode()
    except OSError:
        return [f"{url} not reachable (is the book service running?)"]
    wanted = (
        "simex_book_state{",
        "simex_book_levels",
        "simex_book_crossed_total",
        "simex_book_resyncs_total",
        "simex_book_updates_total",
        "simex_book_seconds_since_snapshot",
        "simex_book_catching_up",
        "simex_book_valkey_errors_total",
        "simex_book_replayed_from_log_start_total",
        "simex_book_audits_total",
        "simex_book_resync_requests_total",
        "simex_book_apply_seconds_count",
        "simex_book_apply_seconds_sum",
    )
    return [line for line in text.splitlines() if line.startswith(wanted)]


async def watch(url: str, venue: str, products: list[str], seconds: float, interval: float):
    client = valkey.from_url(url)
    samples: dict[str, int] = defaultdict(int)
    ok: dict[str, int] = defaultdict(int)
    missing: dict[str, int] = defaultdict(int)
    last_state: dict[str, str] = {}
    left_synced_at: dict[str, float] = {}
    transitions: dict[str, list[str]] = defaultdict(list)
    outside: dict[str, list[float]] = defaultdict(list)
    latest: dict[str, dict] = {}
    start = time.monotonic()
    try:
        while time.monotonic() - start < seconds:
            now = time.monotonic()
            for product in products:
                raw = await client.get(book_key(venue, product))
                record = json.loads(raw) if raw else None
                samples[product] += 1
                if record is None:
                    missing[product] += 1
                    state = "MISSING"
                else:
                    latest[product] = record
                    state = record["state"] + (" (catching up)" if record["catching_up"] else "")
                if usable(record, time.time_ns()):
                    ok[product] += 1
                previous = last_state.get(product)
                if previous is not None and state != previous:
                    transitions[product].append(f"{now - start:7.2f}s {previous} -> {state}")
                    if previous == "SYNCED":
                        left_synced_at[product] = now
                    elif state == "SYNCED" and product in left_synced_at:
                        outside[product].append(now - left_synced_at.pop(product))
                last_state[product] = state
            await asyncio.sleep(interval)
    finally:
        await client.aclose()

    print(f"Watched {seconds:.0f} s, every {interval} s\n")
    for product in products:
        n = samples[product]
        share = 100 * ok[product] / max(n, 1)
        print(f"{product}: usable {ok[product]}/{n} ({share:.1f}%), key missing {missing[product]}")
        record = latest.get(product)
        if record:
            age = (time.time_ns() - (record["received_at_ns"] or 0)) / 1e9
            print(
                f"  now {record['state']}, levels {record['levels']}, "
                f"last update received {age:.2f} s ago, update id {record['last_update_id']}"
            )
        for line in transitions[product]:
            print(f"  {line}")
        if outside[product]:
            spans = outside[product]
            print(
                f"  outside SYNCED {len(spans)}x: max {max(spans) * 1000:.0f} ms, "
                f"mean {sum(spans) / len(spans) * 1000:.0f} ms "
                f"(resolution {interval * 1000:.0f} ms)"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, default=120)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--venue", default="gemini")
    parser.add_argument("--pairs", default=os.environ.get("SIMEX_PAIRS", "BTC-USD,ETH-USD,SOL-USD"))
    port = os.environ.get("VALKEY_PORT", "6379")
    default_url = os.environ.get("VALKEY_URL", f"redis://127.0.0.1:{port}/0")
    parser.add_argument("--valkey-url", default=default_url)
    metrics_port = os.environ.get("BOOK_METRICS_PORT", "9102")
    parser.add_argument("--metrics-url", default=f"http://127.0.0.1:{metrics_port}/metrics")
    args = parser.parse_args()
    products = [p.strip().upper() for p in args.pairs.split(",") if p.strip()]
    asyncio.run(watch(args.valkey_url, args.venue, products, args.seconds, args.interval))
    print("\nBook service counters:")
    for line in scrape(args.metrics_url):
        print(f"  {line}")


if __name__ == "__main__":
    main()
