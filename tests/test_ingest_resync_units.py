import json
import logging

import pytest
from coinbase_samples import L2_SNAPSHOT, TRADES, raw

from simex.ingest.client import CoinbaseIngestor
from simex.ingest.config import Settings
from simex.ingest.envelope import (
    build_envelopes,
    build_gap_record,
    build_resync_marker,
    snapshot_products,
)
from simex.ingest.metrics import IngestMetrics
from simex.ingest.resync import FaultInjector, GapRateMonitor, RateLimiter

RECEIVED = 1_792_026_000_250_000_000


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


# Envelope v2 shapes ---------------------------------------------------------------------------


def test_data_envelopes_are_version_2_with_a_type():
    [env] = build_envelopes(raw(TRADES), TRADES, RECEIVED)
    decoded = json.loads(env.value)
    assert decoded["v"] == 2 and decoded["type"] == "data"
    assert "awaiting_snapshot" not in decoded  # level2 only


def test_level2_envelope_says_whether_its_product_awaits_a_snapshot():
    [fresh] = build_envelopes(raw(L2_SNAPSHOT), L2_SNAPSHOT, RECEIVED)
    [stale] = build_envelopes(raw(L2_SNAPSHOT), L2_SNAPSHOT, RECEIVED, {"BTC-USD"})
    assert json.loads(fresh.value)["awaiting_snapshot"] is False
    assert json.loads(stale.value)["awaiting_snapshot"] is True


def test_resync_marker_shape():
    marker = build_resync_marker("ETH-USD", "abc123", "sequence_gap", RECEIVED)
    assert (marker.topic, marker.key) == ("md.level2", b"ETH-USD")
    assert json.loads(marker.value) == {
        "v": 2,
        "type": "resync",
        "received_at_ns": RECEIVED,
        "exchange_ts_ns": None,
        "channel": "level2",
        "product_id": "ETH-USD",
        "sequence_num": None,
        "resync_id": "abc123",
        "reason": "sequence_gap",
    }


def test_gap_record_shape():
    record = json.loads(
        build_gap_record(
            gap_id="g1",
            reason="sequence_gap",
            products=("BTC-USD", "ETH-USD"),
            channels=("level2", "market_trades", "heartbeats"),
            expected_sequence=101,
            received_sequence=104,
            start=(1, 2),
            end=(3, 4),
            detected_at_ns=5,
            resync_id="r1",
            resync_action="resubscribe",
            fault_injected=False,
        )
    )
    assert record == {
        "v": 1,
        "type": "gap",
        "gap_id": "g1",
        "reason": "sequence_gap",
        "products": ["BTC-USD", "ETH-USD"],
        "channels": ["level2", "market_trades", "heartbeats"],
        "expected_sequence": 101,
        "received_sequence": 104,
        "missing_messages": 3,
        "start_exchange_ts_ns": 1,
        "start_received_at_ns": 2,
        "end_exchange_ts_ns": 3,
        "end_received_at_ns": 4,
        "detected_at_ns": 5,
        "resync_id": "r1",
        "resync_action": "resubscribe",
        "fault_injected": False,
    }


def test_snapshot_products():
    assert snapshot_products(L2_SNAPSHOT) == ["BTC-USD"]
    update = {**L2_SNAPSHOT, "events": [{**L2_SNAPSHOT["events"][0], "type": "update"}]}
    assert snapshot_products(update) == []
    assert snapshot_products(TRADES) == []


# Rate limiting ---------------------------------------------------------------------------------


def test_rate_limiter_allows_one_per_interval():
    clock = FakeClock()
    limiter = RateLimiter(5.0, clock)
    assert limiter.try_acquire()
    clock.now += 4.9
    assert not limiter.ready() and not limiter.try_acquire()
    clock.now += 0.1
    assert limiter.try_acquire()
    assert limiter.ready_at() == clock.now + 5.0


# Fault injector --------------------------------------------------------------------------------


def test_fault_injector_is_off_by_default(caplog):
    assert Settings().drop_every == 0
    assert Settings.from_env({}).drop_every == 0
    injector = FaultInjector()
    assert not injector.enabled
    assert not any(injector.should_drop() for _ in range(10_000))
    with caplog.at_level(logging.WARNING):
        CoinbaseIngestor(Settings(), producer=None, metrics=IngestMetrics())
    assert not any("FAULT INJECTOR" in m for m in caplog.messages)


def test_fault_injector_drops_every_nth_and_warns_loudly(caplog):
    injector = FaultInjector(3)
    assert [injector.should_drop() for _ in range(7)] == [False, False, True] * 2 + [False]
    injector.record_dropped(42)
    assert injector.caused(40, 43) and not injector.caused(43, 50)
    with caplog.at_level(logging.WARNING):
        CoinbaseIngestor(Settings(drop_every=500), producer=None, metrics=IngestMetrics())
    assert any("FAULT INJECTOR ON: dropping every 500th message" in m for m in caplog.messages)


@pytest.mark.parametrize("value", [-1, 1])
def test_fault_injector_rejects_nonsense(value):
    with pytest.raises(ValueError):
        FaultInjector(value)


def test_settings_read_resync_variables():
    settings = Settings.from_env(
        {
            "SIMEX_INGEST_DROP_EVERY": "500",
            "INGEST_RESYNC_MIN_INTERVAL_SECONDS": "2",
            "INGEST_RESYNC_TIMEOUT_SECONDS": "7",
            "INGEST_GAP_ALERT_PER_MINUTE": "3",
        }
    )
    assert settings.drop_every == 500
    assert settings.resync_min_interval_seconds == 2.0
    assert settings.resync_timeout_seconds == 7.0
    assert settings.gap_alert_per_minute == 3


# Gap-rate alarm --------------------------------------------------------------------------------


def test_gap_rate_monitor_counts_the_last_minute():
    clock = FakeClock()
    monitor = GapRateMonitor(clock)
    for _ in range(3):
        monitor.record()
    clock.now += 61
    assert monitor.record() == 1


def test_more_than_ten_real_gaps_a_minute_logs_an_error(caplog):
    clock = FakeClock()
    ingestor = CoinbaseIngestor(Settings(), producer=None, metrics=IngestMetrics(), monotonic=clock)
    with caplog.at_level(logging.ERROR):
        for _ in range(10):
            ingestor._check_gap_rate()
        assert not caplog.records
        ingestor._check_gap_rate()
        ingestor._check_gap_rate()  # only one error per minute
    [error] = caplog.records
    assert "11 sequence gaps in the last minute" in error.message
    assert "sequence_num semantics may have changed" in error.message


def test_gap_during_resubscribe_in_flight_does_not_resubscribe_again():
    import asyncio

    from test_ingest_end_to_end import FakeProducer

    from simex.ingest.client import Resync
    from simex.ingest.sequence import SequenceAnomaly

    producer = FakeProducer()
    ingestor = CoinbaseIngestor(Settings(), producer=producer, metrics=IngestMetrics())
    ingestor.resync = Resync("r1", "gap", 0.0, "resubscribe", ingestor.settings.pairs, 0.0)
    ingestor.limiter.try_acquire()  # the in-flight resubscribe used the limiter

    asyncio.run(ingestor._on_anomaly(SequenceAnomaly("gap", 10, 12), "l2_data", (None, 1)))

    assert ingestor.resubscribe_wanted is False
    [gap] = [json.loads(v) for t, _, v in producer.records if t == "md.gaps"]
    assert gap["resync_action"] == "covered_by_resync_in_flight"
    assert gap["resync_id"] == "r1"
    assert ingestor.metrics.registry.get_sample_value("simex_ingest_resyncs_suppressed_total") == 0
