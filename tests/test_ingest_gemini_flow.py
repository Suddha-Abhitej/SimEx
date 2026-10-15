"""The ingestor on Gemini, end to end against the scripted fake server (ADR 0011)."""

import asyncio
import json
import logging
from decimal import Decimal

import websockets
from fake_gemini import PAIRS, FakeGemini
from test_ingest_end_to_end import FakeProducer, wait_until

from simex.ingest.client import GeminiIngestor
from simex.ingest.config import Settings
from simex.ingest.metrics import IngestMetrics
from simex.marketdata import BookUpdate, ResyncMarker, Trade, decode


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def sample(metrics: IngestMetrics, name: str, **labels) -> float:
    return metrics.registry.get_sample_value(name, {"venue": "gemini", **labels}) or 0.0


def records(producer: FakeProducer, topic: str) -> list[dict]:
    return [json.loads(value) for t, _, value in producer.records if t == topic]


def values(producer: FakeProducer, topic: str) -> list[bytes]:
    return [value for t, _, value in producer.records if t == topic]


async def run_against(fake: FakeGemini, script, **settings):
    producer, metrics = FakeProducer(), IngestMetrics()
    async with websockets.serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        settings.setdefault("stale_after_seconds", 5)
        config = Settings(ws_url=f"ws://127.0.0.1:{port}/?snapshot=-1", **settings)
        ingestor = GeminiIngestor(config, producer, metrics, sleep=no_sleep)
        task = asyncio.create_task(ingestor.run())
        try:
            await wait_until(lambda: fake.connections >= 1 and not ingestor.awaiting)
            await script(ingestor, producer, metrics)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    return ingestor, producer, metrics


def test_snapshots_updates_and_trades_reach_md_topics():
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        await fake.update("BTC-USD")
        await fake.update("BTC-USD")
        await fake.trade("ETH-USD")
        await wait_until(lambda: len(records(producer, "md.trades")) == 1)
        await wait_until(lambda: len(records(producer, "md.level2")) == 5)
        assert sample(metrics, "simex_ingest_connected") == 1

    ingestor, producer, metrics = asyncio.run(run_against(fake, script))

    # One connection, one SUBSCRIBE for every stream, with the snapshot query.
    assert fake.connections == 1 and fake.snapshot_query is True
    [subscribe] = fake.requests_of("SUBSCRIBE")
    assert len(subscribe[2]["params"]) == 6

    level2 = records(producer, "md.level2")
    snapshots = [r for r in level2 if r["snapshot"]]
    assert sorted(r["product_id"] for r in snapshots) == sorted(PAIRS)
    for record in level2:
        assert record["v"] == 3 and record["venue"] == "gemini" and record["type"] == "data"
        assert record["sequence_num"] is None
        first, last = record["update_ids"]
        assert first <= last and record["awaiting_snapshot"] is False
    updates = [r for r in level2 if not r["snapshot"]]
    assert [r["product_id"] for r in updates] == ["BTC-USD", "BTC-USD"]
    assert updates[1]["update_ids"][0] == updates[0]["update_ids"][1]  # U == previous u

    [trade] = records(producer, "md.trades")
    assert (trade["channel"], trade["product_id"]) == ("market_trades", "ETH-USD")
    assert "snapshot" not in trade and trade["exchange_ts_ns"] is not None

    # Through the shared decoder: Decimal prices and sizes, venue-independent shapes.
    [book] = decode(values(producer, "md.level2")[0])
    assert isinstance(book, BookUpdate) and book.snapshot and book.venue == "gemini"
    assert all(isinstance(p, Decimal) and isinstance(q, Decimal) for p, q in book.bids)
    [decoded_trade] = decode(values(producer, "md.trades")[0])
    assert isinstance(decoded_trade, Trade)
    assert decoded_trade.price == Decimal("100.00") and decoded_trade.aggressor == "sell"

    assert sample(metrics, "simex_ingest_connected") == 0  # cleared once the task stops
    assert sample(metrics, "simex_ingest_messages_total", channel="level2") == 5
    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="feed") == 0
    assert records(producer, "md.gaps") == []
    assert not any(t.startswith("research.") for t, _, _ in producer.records)


def test_gap_resyncs_only_that_product(caplog):
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        await fake.update("ETH-USD")
        await fake.update("ETH-USD", skip=True)  # the diff before this one was lost
        await fake.update("BTC-USD")
        await wait_until(lambda: len(fake.requests_of("UNSUBSCRIBE")) == 1)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)

    with caplog.at_level(logging.INFO, logger="simex.ingest"):
        ingestor, producer, metrics = asyncio.run(run_against(fake, script))

    level2 = records(producer, "md.level2")
    markers = [r for r in level2 if r["type"] == "resync"]
    assert [(m["product_id"], m["reason"], m["venue"]) for m in markers] == [
        ("ETH-USD", "update_id_gap", "gemini")
    ]
    # The revealing ETH diff is flagged; BTC, published after the marker, is not.
    after = level2[level2.index(markers[0]) + 1 :]
    assert (after[0]["product_id"], after[0]["awaiting_snapshot"]) == ("ETH-USD", True)
    btc = next(r for r in after if r["product_id"] == "BTC-USD")
    assert btc["awaiting_snapshot"] is False
    fresh = next(r for r in after if r["product_id"] == "ETH-USD" and r["snapshot"])
    assert fresh["awaiting_snapshot"] is False

    [gap] = records(producer, "md.gaps")
    assert gap["venue"] == "gemini" and gap["products"] == ["ETH-USD"]
    assert gap["channels"] == ["level2"]
    assert gap["missing_messages"] is None  # exchange-wide IDs: not a count
    assert gap["id_jump"] == gap["received_sequence"] - gap["expected_sequence"] > 0
    assert gap["resync_action"] == "resubscribe" and gap["fault_injected"] is False

    # Only ETH's depth stream was resubscribed, on the same connection.
    [(_, _, unsubscribe)] = fake.requests_of("UNSUBSCRIBE")
    assert unsubscribe["params"] == ["ethusd@depth@100ms"]
    assert fake.requests_of("SUBSCRIBE")[-1][2]["params"] == ["ethusd@depth@100ms"]
    assert fake.connections == 1

    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="feed") == 1
    assert sample(metrics, "simex_ingest_missed_messages_total") == 0
    assert sample(metrics, "simex_ingest_resyncs_total", method="resubscribe", trigger="gap") == 1
    assert sample(metrics, "simex_ingest_resync_duration_seconds_count", method="resubscribe") == 1
    assert any(
        "resync via resubscribe: snapshot received for ETH-USD" in m for m in caplog.messages
    )

    # Decoded, the marker tells the book to drop ETH and wait.
    marker = next(v for v in values(producer, "md.level2") if b'"type":"resync"' in v)
    [event] = decode(marker)
    assert isinstance(event, ResyncMarker) and event.product_id == "ETH-USD"


def test_resubscribe_without_snapshot_falls_back_to_reconnect():
    fake = FakeGemini(snapshot_on_resubscribe=False)

    async def script(ingestor, producer, metrics):
        await fake.update("SOL-USD")
        await fake.update("SOL-USD", skip=True)
        await wait_until(lambda: fake.connections == 2 and ingestor.resync is None, timeout=5)

    _, producer, metrics = asyncio.run(run_against(fake, script, resync_timeout_seconds=0.3))

    assert sample(metrics, "simex_ingest_reconnects_total", reason="resync_timeout") == 1
    assert sample(metrics, "simex_ingest_resync_duration_seconds_count", method="reconnect") == 1
    gaps = records(producer, "md.gaps")
    assert [g["reason"] for g in gaps] == ["update_id_gap", "disconnect"]
    assert gaps[1]["products"] == list(PAIRS)


def test_fault_injector_forces_a_gap_that_resyncs():
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        for _ in range(12):
            await fake.update("BTC-USD")
        await wait_until(lambda: records(producer, "md.gaps"))
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)

    _, producer, metrics = asyncio.run(run_against(fake, script, drop_every=10))

    assert sample(metrics, "simex_ingest_fault_injected_drops_total") >= 1
    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="injected") >= 1
    assert sample(metrics, "simex_ingest_gaps_total", kind="gap", cause="feed") == 0
    assert records(producer, "md.gaps")[0]["fault_injected"] is True
    assert sample(metrics, "simex_ingest_feed_gaps_last_minute") == 0


def test_ping_keeps_the_connection_live_and_measures_round_trip():
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        await wait_until(lambda: len(fake.requests_of("ping")) >= 2, timeout=3)
        await wait_until(lambda: sample(metrics, "simex_ingest_ping_rtt_seconds") > 0)

    # No market data after the snapshots: only the pings keep the 0.5 s stale timer quiet.
    ingestor, _, metrics = asyncio.run(
        run_against(fake, script, ping_interval_seconds=0.1, stale_after_seconds=0.5)
    )
    assert fake.connections == 1
    assert sample(metrics, "simex_ingest_reconnects_total", reason="stale") == 0
    assert ingestor._seconds_since_message(("", "ping")) < 1


def test_snapshot_refresh_resubscribes_one_product_at_a_time():
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        await wait_until(lambda: len(fake.requests_of("UNSUBSCRIBE")) >= 3, timeout=5)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)

    _, producer, metrics = asyncio.run(
        run_against(fake, script, snapshot_refresh_seconds=0.6, resync_min_interval_seconds=0)
    )
    unsubscribes = [r[2]["params"] for r in fake.requests_of("UNSUBSCRIBE")[:3]]
    assert unsubscribes == [["btcusd@depth@100ms"], ["ethusd@depth@100ms"], ["solusd@depth@100ms"]]
    refresh_markers = [r for r in records(producer, "md.level2") if r["type"] == "resync"][:3]
    assert [m["reason"] for m in refresh_markers] == ["refresh"] * 3
    for product in PAIRS:
        completed = sample(
            metrics,
            "simex_ingest_snapshot_refreshes_total",
            product_id=product,
            outcome="completed",
        )
        assert completed >= 1
    assert records(producer, "md.gaps") == []


def test_a_lost_snapshot_on_connect_forces_a_reconnect():
    fake = FakeGemini()
    original = fake.snapshot

    async def lose_first_sol_snapshot(symbol):
        if symbol == "SOLUSD" and fake.connections == 1:
            return  # nothing breaks a sequence: only the deadline can notice
        await original(symbol)

    fake.snapshot = lose_first_sol_snapshot
    producer, metrics = FakeProducer(), IngestMetrics()

    async def scenario():
        async with websockets.serve(fake.handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = Settings(
                ws_url=f"ws://127.0.0.1:{port}/?snapshot=-1",
                stale_after_seconds=5,
                resync_timeout_seconds=0.3,
            )
            ingestor = GeminiIngestor(config, producer, metrics, sleep=no_sleep)
            task = asyncio.create_task(ingestor.run())
            try:
                await wait_until(lambda: fake.connections == 2 and not ingestor.awaiting)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert sample(metrics, "simex_ingest_reconnects_total", reason="resync_timeout") == 1
    snapshots = [r["product_id"] for r in records(producer, "md.level2") if r.get("snapshot")]
    assert snapshots.count("SOL-USD") == 1 and snapshots.count("BTC-USD") == 2


def test_trade_silence_never_makes_the_connection_stale():
    # Gemini pairs can go minutes without a trade (212 s measured): only level2 and pings count.
    fake = FakeGemini()

    async def script(ingestor, producer, metrics):
        for _ in range(12):  # 1.2 s of depth diffs, no trades at all, stale window 0.4 s
            await fake.update("BTC-USD")
            await asyncio.sleep(0.1)
        assert ingestor._seconds_since_message(("BTC-USD", "market_trades")) > 0.4

    _, _, metrics = asyncio.run(
        run_against(fake, script, ping_interval_seconds=0, stale_after_seconds=0.4)
    )
    assert fake.connections == 1
    assert sample(metrics, "simex_ingest_reconnects_total", reason="stale") == 0


def test_trades_alone_do_not_keep_a_dead_depth_stream_alive():
    fake = FakeGemini()

    reconnected_while_trading = []

    async def script(ingestor, producer, metrics):
        # No depth diffs and no pings, while a trade arrives every 0.1 s (well inside the 0.4 s
        # stale window): the trades must not hide the dead depth stream.
        for _ in range(30):
            await fake.trade("ETH-USD")
            await asyncio.sleep(0.1)
            if fake.connections >= 2:
                reconnected_while_trading.append(True)
                break

    _, producer, metrics = asyncio.run(
        run_against(fake, script, ping_interval_seconds=0, stale_after_seconds=0.4)
    )
    assert reconnected_while_trading
    assert sample(metrics, "simex_ingest_reconnects_total", reason="stale") >= 1
    assert records(producer, "md.trades")  # the trades themselves were still published
