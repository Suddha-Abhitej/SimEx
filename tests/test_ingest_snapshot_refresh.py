"""Scheduled per-product snapshot refresh (ADR 0009)."""

import asyncio
import json

import pytest
from coinbase_samples import L2_SNAPSHOT
from fake_coinbase import PAIRS, FakeCoinbase, wait_until
from test_ingest_end_to_end import FakeProducer, sample
from test_ingest_resync_flow import records, run_against

from simex.ingest.client import CoinbaseIngestor, ResyncTimeout
from simex.ingest.config import Settings
from simex.ingest.kafka import level2_retention_problem, level2_topic_configs
from simex.ingest.metrics import IngestMetrics
from simex.ingest.venue import Anomaly

T0 = 1000.0


class FakeClock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self) -> float:
        return self.now


class FakeWs:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, text: str) -> None:
        self.sent.append(json.loads(text))


def snapshot_message(product: str) -> str:
    # No sequence_num, so these unit tests never trip gap detection.
    message = {k: v for k, v in L2_SNAPSHOT.items() if k != "sequence_num"}
    message["events"] = [{**L2_SNAPSHOT["events"][0], "product_id": product}]
    return json.dumps(message)


def synced_ingestor(**settings):
    """An ingestor on a live connection whose books all got a snapshot at T0."""
    clock, producer, metrics = FakeClock(), FakeProducer(), IngestMetrics()
    config = Settings(snapshot_refresh_seconds=1800, resync_min_interval_seconds=5, **settings)
    ingestor = CoinbaseIngestor(config, producer, metrics, monotonic=clock)
    for product in PAIRS:
        asyncio.run(ingestor.handle(snapshot_message(product)))
    assert not ingestor.awaiting
    ingestor.next_refresh_at = T0 + config.refresh_stagger_seconds
    return ingestor, clock, producer, metrics


def refreshes(metrics: IngestMetrics, product: str, outcome: str) -> float:
    return sample(
        metrics, "simex_ingest_snapshot_refreshes_total", product_id=product, outcome=outcome
    )


def markers(producer: FakeProducer) -> list[dict]:
    return [r for r in records(producer, "research.coinbase.md.level2") if r["type"] == "resync"]


def test_refreshes_one_product_at_a_time_staggered_over_the_interval():
    ingestor, clock, producer, metrics = synced_ingestor()
    ws = FakeWs()
    assert ingestor.settings.refresh_stagger_seconds == 600

    clock.now = T0 + 599
    asyncio.run(ingestor._drive_refresh(ws))
    assert ws.sent == [] and ingestor.resync is None

    refreshed = []
    for slot in range(1, 7):
        clock.now = T0 + 600 * slot
        asyncio.run(ingestor._drive_refresh(ws))
        [product] = ingestor.resync.products
        refreshed.append(product)
        assert ingestor.awaiting == {product}  # only this book is resyncing
        clock.now += 0.75
        asyncio.run(ingestor.handle(snapshot_message(product)))
        assert ingestor.resync is None and not ingestor.awaiting

    # Oldest snapshot first, so each product is refreshed once per 30 minutes, 10 minutes apart.
    assert refreshed == [*PAIRS, *PAIRS]
    # Per-product unsubscribe and subscribe, nothing for the other products.
    assert ws.sent[:2] == [
        {"type": "unsubscribe", "product_ids": ["BTC-USD"], "channel": "level2"},
        {"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"},
    ]
    assert all(len(m["product_ids"]) == 1 for m in ws.sent)
    # One marker per refresh, only for that product, with reason "refresh".
    assert [m["product_id"] for m in markers(producer)] == refreshed
    assert all(m["reason"] == "refresh" for m in markers(producer))
    for product in PAIRS:
        assert refreshes(metrics, product, "completed") == 2
        last = "simex_ingest_last_snapshot_refresh_duration_seconds"
        assert sample(metrics, last, product_id=product) == pytest.approx(0.75)
        count = "simex_ingest_snapshot_refresh_duration_seconds_count"
        assert sample(metrics, count, product_id=product) == 2
    started = sample(metrics, "simex_ingest_resyncs_total", method="resubscribe", trigger="refresh")
    assert started == 6
    # A refresh is not a gap resync.
    assert sample(metrics, "simex_ingest_resync_duration_seconds_count", method="resubscribe") == 0


def test_updates_for_the_refreshing_product_are_flagged_and_others_are_not():
    ingestor, clock, producer, _ = synced_ingestor()
    clock.now = T0 + 600
    asyncio.run(ingestor._drive_refresh(FakeWs()))
    update = json.loads(snapshot_message("BTC-USD"))
    update["events"][0]["type"] = "update"
    asyncio.run(ingestor.handle(json.dumps(update)))
    update["events"][0]["product_id"] = "ETH-USD"
    asyncio.run(ingestor.handle(json.dumps(update)))
    btc, eth = records(producer, "research.coinbase.md.level2")[-2:]
    assert (btc["product_id"], btc["awaiting_snapshot"]) == ("BTC-USD", True)
    assert (eth["product_id"], eth["awaiting_snapshot"]) == ("ETH-USD", False)


def test_refresh_shares_the_rate_limiter_with_gap_resyncs():
    ingestor, clock, _, _ = synced_ingestor()
    clock.now = T0 + 600
    ingestor.limiter.try_acquire()  # a gap resubscribe just happened
    ws = FakeWs()
    asyncio.run(ingestor._drive_refresh(ws))
    assert ws.sent == [] and ingestor.resync is None
    assert ingestor._next_timeout(clock.now) == pytest.approx(5.0)  # wakes when the limit allows
    clock.now += 5
    asyncio.run(ingestor._drive_refresh(ws))
    assert ingestor.resync.trigger == "refresh"


def test_refresh_waits_for_a_resync_in_flight_and_skips_fresh_books():
    ingestor, clock, _, _ = synced_ingestor()
    ingestor._set_awaiting(set(PAIRS))  # a gap resync is recovering every book
    clock.now = T0 + 600
    ws = FakeWs()
    asyncio.run(ingestor._drive_refresh(ws))
    assert ws.sent == []
    for product in PAIRS:
        asyncio.run(ingestor.handle(snapshot_message(product)))
    # Every snapshot is seconds old now, so this slot has nothing to refresh.
    asyncio.run(ingestor._drive_refresh(ws))
    assert ws.sent == [] and ingestor.next_refresh_at == clock.now + 600


def test_gap_during_a_refresh_takes_over_with_a_full_resync():
    ingestor, clock, producer, metrics = synced_ingestor()
    clock.now = T0 + 600
    asyncio.run(ingestor._drive_refresh(FakeWs()))
    refresh_id = ingestor.resync.id

    gap = Anomaly("gap", PAIRS, "sequence_gap", expected=10, received=12, missing=2)
    asyncio.run(ingestor._on_anomaly(gap, (None, 1)))

    assert ingestor.resync.trigger == "gap" and ingestor.resync.id != refresh_id
    assert ingestor.resubscribe_wanted and ingestor.awaiting == set(PAIRS)
    gap_markers = [m for m in markers(producer) if m["reason"] == "sequence_gap"]
    assert [m["product_id"] for m in gap_markers] == list(PAIRS)
    assert refreshes(metrics, "BTC-USD", "superseded") == 1


def test_refresh_that_gets_no_snapshot_falls_back_to_reconnect():
    ingestor, clock, _, metrics = synced_ingestor()
    clock.now = T0 + 600
    ws = FakeWs()
    asyncio.run(ingestor._drive_refresh(ws))
    clock.now += ingestor.settings.resync_timeout_seconds
    with pytest.raises(ResyncTimeout):
        asyncio.run(ingestor._drive_resync(ws))
    ingestor._messages_on_connection = 1
    asyncio.run(ingestor._on_disconnect(fallback=True))
    assert ingestor.resync.trigger == "disconnect"
    assert refreshes(metrics, "BTC-USD", "timeout") == 1


def test_refresh_off():
    ingestor, clock, _, _ = synced_ingestor()
    ingestor.next_refresh_at = None  # what a session does with INGEST_SNAPSHOT_REFRESH_SECONDS=0
    clock.now = T0 + 10_000
    ws = FakeWs()
    asyncio.run(ingestor._drive_refresh(ws))
    assert ws.sent == []


def test_settings_and_retention():
    settings = Settings.from_env(
        {
            "INGEST_SNAPSHOT_REFRESH_SECONDS": "900",
            "INGEST_LEVEL2_RETENTION_HOURS": "12",
            "INGEST_LEVEL2_RETENTION_BYTES": "-1",
        }
    )
    assert settings.snapshot_refresh_seconds == 900
    assert settings.refresh_stagger_seconds == 300
    assert settings.level2_retention_ms == 12 * 3_600_000
    assert level2_topic_configs(settings.level2_retention_ms, settings.level2_retention_bytes) == {
        "max.message.bytes": str(16 * 1024 * 1024),
        "retention.ms": "43200000",
        "retention.bytes": "-1",
    }
    defaults = Settings()
    assert level2_retention_problem(defaults.level2_retention_ms, 1800) is None
    assert level2_retention_problem(-1, 1800) is None  # keep forever
    assert level2_retention_problem(3_600_000, 1800) is None  # exactly 2x
    assert "less than 2x" in level2_retention_problem(3_599_999, 1800)
    assert "refresh is off" in level2_retention_problem(defaults.level2_retention_ms, 0)


# Against the scripted Coinbase server -----------------------------------------------------------


def test_staggered_refreshes_against_the_fake_server():
    fake = FakeCoinbase()

    async def script(ingestor, producer, metrics):
        await wait_until(lambda: len(fake.requests_of("unsubscribe", "level2")) >= 3)
        await wait_until(lambda: ingestor.resync is None and not ingestor.awaiting)
        await fake.update("ETH-USD")
        await wait_until(
            lambda: (
                records(producer, "research.coinbase.md.level2")[-1]["payload"]["events"][0]["type"]
                == "update"
            )
        )

    ingestor, producer, metrics = asyncio.run(
        run_against(fake, script, snapshot_refresh_seconds=0.6, resync_min_interval_seconds=0)
    )
    unsubscribes = fake.requests_of("unsubscribe", "level2")[:3]
    resubscribes = fake.requests_of("subscribe", "level2")[1:4]  # the first is the initial one
    assert [r[2]["product_ids"] for r in unsubscribes] == [[p] for p in PAIRS]
    assert [r[2]["product_ids"] for r in resubscribes] == [[p] for p in PAIRS]
    assert fake.connections == 1  # no reconnect

    level2 = records(producer, "research.coinbase.md.level2")
    refresh_markers = [r for r in level2 if r["type"] == "resync"][:3]
    assert [m["product_id"] for m in refresh_markers] == list(PAIRS)
    # After each marker, that product's next envelope is its fresh snapshot.
    for marker in refresh_markers:
        after = level2[level2.index(marker) + 1 :]
        mine = next(r for r in after if r["product_id"] == marker["product_id"])
        assert mine["payload"]["events"][0]["type"] == "snapshot"
        assert mine["awaiting_snapshot"] is False
    assert level2[-1]["product_id"] == "ETH-USD" and level2[-1]["awaiting_snapshot"] is False
    assert records(producer, "research.coinbase.md.gaps") == []
