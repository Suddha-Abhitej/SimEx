"""Summarize what the ingestor published over the last N seconds, read back from Kafka.

Reports messages per second per channel, feed lag p50/p99 (from the envelopes, exact), every
gap recorded on md.gaps, how each resync recovered (from the md.level2 markers and snapshots),
the ingestor's gap and resync counters (from its /metrics endpoint), and one md.trades message.

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

TOPICS = ("md.level2", "md.trades", "md.heartbeats", "md.gaps")
DATA_TOPICS = TOPICS[:3]


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
        raise SystemExit(f"none of {', '.join(DATA_TOPICS)} exist yet; has the ingestor run?")
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
        "simex_ingest_gaps_total",
        "simex_ingest_missed_messages_total",
        "simex_ingest_reconnects_total",
        "simex_ingest_resyncs_total",
        "simex_ingest_resyncs_suppressed_total",
        "simex_ingest_resync_duration_seconds_count",
        "simex_ingest_resync_duration_seconds_sum",
        "simex_ingest_feed_gaps_last_minute",
        "simex_ingest_fault_injected_drops_total",
        "simex_ingest_time_since_last_snapshot_seconds",
    )
    values: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith(wanted):
            name, _, value = line.rpartition(" ")
            values[name] = float(value)
    return values


def is_data(envelope: dict) -> bool:
    return envelope.get("type", "data") == "data"  # v1 envelopes had no type


def is_snapshot(envelope: dict) -> bool:
    events = (envelope.get("payload") or {}).get("events") or []
    return any(e.get("type") == "snapshot" for e in events if isinstance(e, dict))


def resync_outcomes(level2: list[dict], gaps: list[dict]) -> list[str]:
    """For each resync: which method recovered it and how long until every book had a snapshot."""
    level2 = sorted(level2, key=lambda e: e["received_at_ns"])
    markers: dict[str, list[dict]] = defaultdict(list)
    for envelope in level2:
        if envelope.get("type") == "resync":
            markers[envelope["resync_id"]].append(envelope)
    reconnected = {g["resync_id"] for g in gaps if g.get("reason") == "disconnect"}
    lines = []
    for resync_id, marks in markers.items():
        start = min(m["received_at_ns"] for m in marks)
        pending = {m["product_id"] for m in marks}
        products = sorted(pending)
        # Markers can be re-sent while a resync is in flight; recovery is the last snapshot
        # after the last marker.
        last_marker = max(m["received_at_ns"] for m in marks)
        end = None
        for envelope in level2:
            if envelope["received_at_ns"] < last_marker or not is_data(envelope):
                continue
            if is_snapshot(envelope):
                pending.discard(envelope["product_id"])
                if not pending:
                    end = envelope["received_at_ns"]
                    break
        method = "reconnect" if resync_id in reconnected else "resubscribe"
        if end is None:
            lines.append(
                f"  {resync_id[:8]}  NOT recovered in window: "
                f"no snapshot yet for {', '.join(sorted(pending))}"
            )
        else:
            lines.append(
                f"  {resync_id[:8]}  resync via {method}: snapshot received for "
                f"{', '.join(products)} in {(end - start) / 1e6:.0f} ms"
            )
    return lines


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
    for topic in DATA_TOPICS:
        envelopes = [e for e in by_topic.get(topic, []) if is_data(e)]
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

    level2 = by_topic.get("md.level2", [])
    gaps = by_topic.get("md.gaps", [])
    flagged = sum(1 for e in level2 if is_data(e) and e.get("awaiting_snapshot"))
    print(f"\nGaps recorded on md.gaps: {len(gaps)}")
    for gap in gaps:
        missing = gap["missing_messages"]
        print(
            f"  {gap['reason']:<13} missing={'?' if missing is None else missing:<4} "
            f"action={gap['resync_action']:<27} injected={gap['fault_injected']}"
        )
    outcomes = resync_outcomes(level2, gaps)
    print(f"\nResyncs ({len(outcomes)}), level2 updates flagged awaiting_snapshot: {flagged}")
    for line in outcomes:
        print(line)

    counters = scrape_gap_metrics(args.metrics_url)
    print("\nIngestor counters since it started:")
    if counters is None:
        print(f"  {args.metrics_url} not reachable (is the ingestor still running?)")
    for name, value in sorted((counters or {}).items()):
        print(f"  {name} = {value:g}")

    trades = by_topic.get("md.trades", [])
    if trades:
        print("\nSample md.trades message:")
        print(json.dumps(trades[-1], indent=2))


if __name__ == "__main__":
    main()
