"""Lag coverage and real gate decisions on hand-written level2 events (#113)."""

from dataclasses import replace
from decimal import Decimal

import pytest

from simex.book import BookState
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    PairEngine,
    StaleDataGate,
)
from simex.exchange.engine import LAG_WINDOW_NS, EngineSettings
from simex.marketdata import BookUpdate, ResyncMarker

P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
MS = 1_000_000
SECOND = 1_000_000_000
WARMUP = 600 * SECOND
INTERVALS = (1_010_000_007, 997_300_013, 1_023_456_791)


def engine_and_gate(settings=None):
    engines = {}
    gate = StaleDataGate(engines)
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate, 200 * MS)
    engine = PairEngine(P, writer, settings)
    engines[P] = engine
    return engine, gate


def sample(t, lag=100 * MS, **changes):
    event = BookUpdate(
        "gemini",
        P,
        True,
        ((Decimal("100"), Decimal("2")),),
        ((Decimal("101"), Decimal("2")),),
        (1, 1),
        False,
        t - lag,
        t,
    )
    return replace(event, **changes)


def warm(engine):
    now = T0
    engine.on_event(sample(now))
    for i in range(700):
        now += INTERVALS[i % len(INTERVALS)]
        engine.on_event(sample(now))
    return now


def test_irregular_coverage_establishes_and_stays_warm_after_window_eviction():
    engine, gate = engine_and_gate()
    now = T0
    engine.on_event(sample(now))
    for i in range(700):
        now += INTERVALS[i % len(INTERVALS)]
        engine.on_event(sample(now))
        assert engine.status.baseline_established == (now - T0 >= WARMUP)
    assert now - T0 > WARMUP
    assert engine._lag_samples[0][0] > T0
    assert engine._lag_samples[-1][0] - engine._lag_samples[0][0] < LAG_WINDOW_NS
    assert engine.status.excess_lag_ns == 0
    assert gate.check(P, now).open


def test_nine_minutes_of_coverage_do_not_establish():
    engine, _ = engine_and_gate()
    for elapsed in (0, 123 * SECOND + 17, 9 * 60 * SECOND):
        engine.on_event(sample(T0 + elapsed))
    assert not engine.status.baseline_established
    assert engine.status.excess_lag_ns is None


@pytest.mark.parametrize("minutes", [3, 10, 12])
@pytest.mark.parametrize("offset, established", [(-1, False), (0, True)])
def test_configured_coverage_threshold_is_inclusive(minutes, offset, established):
    engine, _ = engine_and_gate(EngineSettings(lag_baseline_minutes=minutes))
    end = T0 + minutes * 60 * SECOND + offset
    for t in (T0, end - 2 * SECOND, end - SECOND, end):
        engine.on_event(sample(t))
    assert engine.status.baseline_established is established


def test_missing_exchange_timestamps_do_not_start_coverage():
    engine, _ = engine_and_gate()
    engine.on_event(sample(T0, exchange_ts_ns=None))
    first_usable = T0 + 2 * WARMUP
    for elapsed in (0, SECOND, 2 * SECOND, WARMUP - 1):
        engine.on_event(sample(first_usable + elapsed))
        assert not engine.status.baseline_established
    engine.on_event(sample(first_usable + WARMUP))
    assert engine.status.baseline_established


@pytest.mark.parametrize("excess_ms", [1999, 2000, 2001])
def test_warm_engine_enforces_excess_with_exact_reason_and_strict_limit(excess_ms):
    engine, gate = engine_and_gate()
    now = warm(engine) + SECOND
    engine.on_event(sample(now, (100 + excess_ms) * MS))
    assert engine.status.baseline_established
    assert engine.status.excess_lag_ns == excess_ms * MS
    # Fresh, synchronized and below the absolute limit: only rule 3 can close this gate.
    assert engine.status.book_state is BookState.SYNCED
    assert not engine.status.catching_up
    assert now - engine.status.last_level2_received_ns == 0
    assert abs(engine.status.lag_ns) < gate.max_absolute_lag_ns
    decision = gate.check(P, now)
    assert decision.open is (excess_ms <= 2000)
    assert decision.reason == (
        f"feed_lagging:excess_ns={excess_ms * MS}" if excess_ms > 2000 else None
    )


def test_resync_and_awaiting_snapshot_preserve_transport_coverage():
    engine, gate = engine_and_gate()
    now = warm(engine)
    engine.on_event(ResyncMarker("gemini", P, "synthetic", "update_id_gap", now + MS))
    assert engine.status.baseline_established
    assert gate.check(P, now + MS).reason == "catching_up"
    engine.on_event(sample(now + 2 * MS, snapshot=False, awaiting_snapshot=True))
    assert engine.status.baseline_established
    assert engine.status.excess_lag_ns == 0
    assert gate.check(P, now + 2 * MS).reason == "catching_up"
    engine.on_event(sample(now + 3 * MS))
    assert engine.status.baseline_established
    assert gate.check(P, now + 3 * MS).open
    engine.on_event(sample(now + 4 * MS, 2201 * MS))
    assert gate.check(P, now + 4 * MS).reason == "feed_lagging:excess_ns=2101000000"


@pytest.mark.parametrize(
    "second_lag_ms, third_lag_ms, excess_ms",
    [(100, 4500, 4400), (4500, 4400, 0)],
)
def test_sparse_window_requires_three_samples_without_restarting_coverage(
    second_lag_ms, third_lag_ms, excess_ms
):
    engine, gate = engine_and_gate()
    now = warm(engine) + LAG_WINDOW_NS + SECOND
    engine.on_event(sample(now))
    assert len(engine._lag_samples) == 1
    assert not engine.status.baseline_established
    assert engine.status.excess_lag_ns is None
    assert gate.check(P, now).open
    engine.on_event(sample(now + 100 * MS, second_lag_ms * MS))
    assert len(engine._lag_samples) == 2
    assert not engine.status.baseline_established
    assert engine.status.excess_lag_ns is None
    # At n=2, 100/4500 ms would otherwise spuriously close on 2200 ms excess.
    assert gate.check(P, now + 100 * MS).open
    engine.on_event(sample(now + 200 * MS, third_lag_ms * MS))
    assert len(engine._lag_samples) == 3
    assert engine.status.baseline_established
    assert engine.status.excess_lag_ns == excess_ms * MS
    assert gate.check(P, now + 200 * MS).open is (excess_ms <= 2000)


def test_expired_lags_do_not_contribute_to_the_current_median():
    engine, gate = engine_and_gate()
    now = warm(engine) + LAG_WINDOW_NS + SECOND
    for i, lag_ms in enumerate((10_000, 11_000, 12_000)):
        engine.on_event(sample(now + i * MS, lag_ms * MS))
    assert len(engine._lag_samples) == 3
    assert engine.status.baseline_established
    assert engine.status.excess_lag_ns == 1000 * MS
    assert engine.status.p99_lag_ns == 12_000 * MS
    assert gate.check(P, now + 2 * MS).open


def test_new_engine_restarts_coverage_instead_of_inheriting_warmup():
    old_engine, _ = engine_and_gate()
    now = warm(old_engine) + SECOND
    engine, _ = engine_and_gate()
    for elapsed in (0, SECOND, 2 * SECOND, WARMUP - 1):
        engine.on_event(sample(now + elapsed))
        assert not engine.status.baseline_established
    assert old_engine.status.baseline_established
    engine.on_event(sample(now + WARMUP))
    assert engine.status.baseline_established
