"""Summarize what the ingestor published over the last N seconds, read back from Kafka.

Reports messages per second per channel, feed lag p50/p99 (from the envelopes, exact), the
ingestor's sequence-gap counters (from its /metrics endpoint), and one sample md.trades message.

Usage: python scripts/ingest_report.py --seconds 120 [--metrics-url http://127.0.0.1:9101/metrics]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import urllib.request
from collections import defaultdict

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

TOPICS = ("md.level2", "md.trades", "md.heartbeats")


def percentile(sorted_values: list[float], fraction: float) -> float:
    index = min(len(sorted_values) - 1, int(len(sorted_values) * fraction))
    return sorted_values[index]


async def list_partitions(bootstrap: str) -> list[TopicPartition]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        described = await admin.describe_topics(list(TOPICS))
    finally:
        await admin.close()
    return [
        TopicPartition(topic["topic"], partition["partition"])
        for topic in described
        if topic["error_code"] == 0
        for partition in topic["partitions"]
    ]


async def read_window(bootstrap: str, start_ms: int, end_ms: int) -> dict[str, list[dict]]:
    partitions = await list_partitions(bootstrap)
    if not partitions:
        raise SystemExit(f"none of {', '.join(TOPICS)} exist yet; has the ingestor run?")
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    await consumer.start()
    try:
        consumer.assign(partitions)
        starts = await consumer.offsets_for_times({tp: start_ms for tp in partitions})
        ends = await consumer.end_offsets(partitions)
        todo = {}
        for tp in partitions:
            if starts.get(tp) is None or starts[tp].offset >= ends[tp]:
                continue
            consumer.seek(tp, starts[tp].offset)
            todo[tp] = ends[tp]
        consumer.pause(*[tp for tp in partitions if tp not in todo])

        by_topic: dict[str, list[dict]] = defaultdict(list)
        while todo:
            batch = await consumer.getmany(timeout_ms=2000, max_records=5000)
            if not batch:
                break
            for tp, records in batch.items():
                for record in records:
                    if record.offset >= todo.get(tp, 0):
                        continue
                    if record.timestamp <= end_ms:
                        by_topic[tp.topic].append(json.loads(record.value))
                if records and records[-1].offset + 1 >= todo.get(tp, 0):
                    todo.pop(tp, None)
                    consumer.pause(tp)
        return by_topic
    finally:
        await consumer.stop()


def scrape_gap_metrics(url: str) -> dict[str, float] | None:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            text = response.read().decode()
    except OSError:
        return None
    wanted = (
        "simex_ingest_sequence_gaps_total",
        "simex_ingest_missed_messages_total",
        "simex_ingest_reconnects_total",
    )
    values: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith(wanted):
            name, _, value = line.rpartition(" ")
            values[name] = float(value)
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, default=120, help="window length (default 120)")
    parser.add_argument(
        "--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    )
    port = os.environ.get("INGEST_METRICS_PORT", "9101")
    parser.add_argument("--metrics-url", default=f"http://127.0.0.1:{port}/metrics")
    args = parser.parse_args()

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - int(args.seconds * 1000)
    by_topic = asyncio.run(read_window(args.bootstrap, start_ms, end_ms))

    print(f"Window: last {args.seconds:.0f}s, read back from Kafka at {args.bootstrap}\n")
    print(f"{'channel':<14} {'messages':>9} {'msg/s':>8} {'lag p50':>10} {'lag p99':>10}")
    for topic in TOPICS:
        envelopes = by_topic.get(topic, [])
        channel = envelopes[0]["channel"] if envelopes else topic
        lags = sorted(
            (e["received_at_ns"] - e["exchange_ts_ns"]) / 1e6
            for e in envelopes
            if e.get("exchange_ts_ns") is not None
        )
        p50 = f"{percentile(lags, 0.50):.1f} ms" if lags else "-"
        p99 = f"{percentile(lags, 0.99):.1f} ms" if lags else "-"
        rate = len(envelopes) / args.seconds
        print(f"{channel:<14} {len(envelopes):>9} {rate:>8.1f} {p50:>10} {p99:>10}")
    print("(lag = local receive time minus Coinbase timestamp; needs an NTP-synced clock)")

    gaps = scrape_gap_metrics(args.metrics_url)
    print("\nIngestor counters since it started:")
    if gaps is None:
        print(f"  {args.metrics_url} not reachable (is the ingestor still running?)")
    elif not gaps:
        print("  no sequence gaps and no reconnects")
    for name, value in sorted((gaps or {}).items()):
        print(f"  {name} = {value:.0f}")

    trades = by_topic.get("md.trades", [])
    if trades:
        print("\nSample md.trades message:")
        print(json.dumps(trades[-1], indent=2))


if __name__ == "__main__":
    main()
