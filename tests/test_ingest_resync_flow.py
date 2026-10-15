"""Gap -> marker -> resubscribe -> snapshot, against the scripted fake Coinbase server."""

import asyncio
import json
import logging

import websockets
from fake_coinbase import PAIRS, FakeCoinbase, wait_until
from test_ingest_end_to_end import FakeProducer, sample

from simex.ingest.client import CoinbaseIngestor
from simex.ingest.config import Settings
from simex.ingest.metrics import IngestMetrics


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def records(producer: FakeProducer, topic: str) -> list[dict]:
    return [json.loads(value) for t, _, value in producer.records if t == topic]


async def run_against(fake: FakeCoinbase, script, **settings):
    """Start the fake server and an ingestor, run `script(ingestor, producer, metrics)`."""
    producer, metrics = FakeProducer(), IngestMetrics()
    async with websockets.serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        config = Settings(ws_url=f"ws://127.0.0.1:{port}", stale_after_seconds=5, **settings)
        ingestor = CoinbaseIngestor(config, producer, metrics, sleep=no_sleep)
        task = asyncio.create_task(ingestor.run())
        try:
            await wait_until(lambda: fake.ready_connections >= 1 and not ingestor.awaiting)
            await script(ingestor, producer, metrics)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    return ingestor, producer, metrics


def test_gap_publishes_markers_resubscribes_and_recovers(caplog):
    fake = FakeCoinbase()
    seen = {}

    async def script(ingestor, producer, metrics):
        await fake.update("BTC-USD")
        fake.skip(2)
        fake.snapshot_on_resubscribe = True
        await fake.update("ETH-USD")  # reveals the gap
        await wait_until(lambda: len(fake.requests_of("unsubscribe", "level2")) == 1)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)
        seen["awaiting_gauges"] = [
            sample(metrics, "simex_ingest_awaiting_snapshot", product_id=p) for p in PAIRS
        ]

    with caplog.at_level(logging.INFO, logger="simex.ingest"):
        ingestor, producer, metrics = asyncio.run(run_against(fake, script))

    level2 = records(producer, "research.coinbase.md.level2")
    markers = [r for r in level2 if r["type"] == "resync"]
    assert [m["product_id"] for m in markers] == list(PAIRS)
    assert len({m["resync_id"] for m in markers}) == 1
    assert all(m["reason"] == "sequence_gap" and m["v"] == 3 for m in markers)

    # Order on md.level2: snapshots, BTC update, markers, ETH update flagged, fresh snapshots.
    after_markers = level2[level2.index(markers[-1]) + 1 :]
    eth_update = after_markers[0]
    assert eth_update["product_id"] == "ETH-USD" and eth_update["awaiting_snapshot"] is True
    fresh = [r for r in after_markers if r["payload"]["events"][0]["type"] == "snapshot"]
    assert [r["product_id"] for r in fresh] == list(PAIRS)
    assert all(r["awaiting_snapshot"] is False for r in fresh)

    [gap] = records(producer, "research.coinbase.md.gaps")
    assert gap["reason"] == "sequence_gap"
    assert gap["missing_messages"] == 2
    assert gap["resync_action"] == "resubscribe"
    assert gap["resync_id"] == markers[0]["resync_id"]
    assert gap["fault_injected"] is False

    # Unsubscribe and subscribe level2 on the same connection; no reconnect.
    assert fake.connections == 1
    resync_requests = [r for _, _, r in fake.requests[3:]]
    assert [(r["type"], r["channel"]) for r in resync_requests] == [
        ("unsubscribe", "level2"),
        ("subscribe", "level2"),
    ]
    assert resync_requests[1]["product_ids"] == list(PAIRS)

    assert sample(metrics, "simex_ingest_resyncs_total", method="resubscribe", trigger="gap") == 1
    assert sample(metrics, "simex_ingest_resync_duration_seconds_count", method="resubscribe") == 1
    assert seen["awaiting_gauges"] == [0, 0, 0]
    assert any(
        "resync via resubscribe: snapshot received for BTC-USD, ETH-USD, SOL-USD in" in m
        for m in caplog.messages
    )
    assert all(
        ingestor._seconds_since_snapshot(p) < 5 for p in PAIRS
    )  # time_since_last_snapshot is fresh


def test_resubscribe_without_snapshot_falls_back_to_reconnect(caplog):
    fake = FakeCoinbase(snapshot_on_resubscribe=False)

    async def script(ingestor, producer, metrics):
        fake.skip(1)
        await fake.update("SOL-USD")
        await wait_until(lambda: fake.connections == 2 and ingestor.resync is None, timeout=5)

    with caplog.at_level(logging.INFO, logger="simex.ingest"):
        _, producer, metrics = asyncio.run(run_against(fake, script, resync_timeout_seconds=0.3))

    assert sample(metrics, "simex_ingest_reconnects_total", reason="resync_timeout") == 1
    assert sample(metrics, "simex_ingest_resync_duration_seconds_count", method="reconnect") == 1
    assert any(
        "resync via reconnect (fallback after resubscribe timed out): snapshot received" in m
        for m in caplog.messages
    )
    gaps = records(producer, "research.coinbase.md.gaps")
    assert [g["reason"] for g in gaps] == ["sequence_gap", "disconnect"]
    assert gaps[1]["resync_id"] == gaps[0]["resync_id"]
    assert gaps[1]["end_received_at_ns"] >= gaps[1]["start_received_at_ns"]


def test_resyncs_are_rate_limited():
    fake = FakeCoinbase()

    async def script(ingestor, producer, metrics):
        fake.skip(1)
        await fake.update("BTC-USD")
        await wait_until(lambda: len(fake.requests_of("unsubscribe", "level2")) == 1)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)
        fake.skip(1)
        await fake.update("ETH-USD")  # second gap, after recovery but inside the 0.5s window
        await wait_until(lambda: len(fake.requests_of("unsubscribe", "level2")) == 2, timeout=3)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)

    _, producer, metrics = asyncio.run(run_against(fake, script, resync_min_interval_seconds=0.5))

    first, second = fake.requests_of("unsubscribe", "level2")
    assert second[1] - first[1] >= 0.45
    assert sample(metrics, "simex_ingest_resyncs_suppressed_total") == 1
    assert [g["resync_action"] for g in records(producer, "research.coinbase.md.gaps")] == [
        "resubscribe",
        "deferred",
    ]
    assert sample(metrics, "simex_ingest_resyncs_total", method="resubscribe", trigger="gap") == 2


def test_fault_injector_forces_a_gap_that_resyncs(caplog):
    fake = FakeCoinbase()

    async def script(ingestor, producer, metrics):
        for _ in range(8):
            await fake.trade("BTC-USD")
        await wait_until(lambda: ingestor.injector.dropped >= 1)
        await fake.trade("BTC-USD")
        await wait_until(lambda: records(producer, "research.coinbase.md.gaps"))
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)

    with caplog.at_level(logging.WARNING, logger="simex.ingest"):
        _, producer, metrics = asyncio.run(run_against(fake, script, drop_every=10))

    assert any("FAULT INJECTOR ON" in m for m in caplog.messages)
    assert sample(metrics, "simex_ingest_fault_injected_drops_total") >= 1
    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="injected") >= 1
    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="feed") == 0
    assert records(producer, "research.coinbase.md.gaps")[0]["fault_injected"] is True
    assert sample(metrics, "simex_ingest_feed_gaps_last_minute") == 0
