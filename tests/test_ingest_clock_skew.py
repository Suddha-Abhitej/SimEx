"""Clock diagnostics through the ingestor with hand-written messages and controlled clocks."""

import asyncio
import json
import logging
from datetime import UTC, datetime

import pytest
from test_ingest_end_to_end import FakeProducer

from simex.ingest.client import CoinbaseIngestor, GeminiIngestor
from simex.ingest.config import Settings
from simex.ingest.metrics import IngestMetrics
from simex.ingest.venue import LEVEL2, Item, Parsed

METRIC = "simex_ingest_clock_skew_suspected"
EPOCH_NS = 1_800_000_000_000_000_000


class Feed:
    def __init__(self, venue="gemini", stale=5):
        self.now = 0.0
        self.wall_shift_ns = 0
        self.ids = {}
        self.producer = FakeProducer()
        self.metrics = IngestMetrics()
        cls = GeminiIngestor if venue == "gemini" else CoinbaseIngestor

        def no_connection(*args, **kwargs):
            pytest.fail("clock diagnostics must not make a network request")

        self.ingestor = cls(
            Settings(stale_after_seconds=stale),
            self.producer,
            self.metrics,
            monotonic=lambda: self.now,
            clock_ns=lambda: self.wall_ns,
            connect=no_connection,
        )
        self.ingestor.adapter.connect_frames()

    @property
    def wall_ns(self):
        return EPOCH_NS + round(self.now * 1_000_000_000) + self.wall_shift_ns

    def level2(self, now, lag=-700_000_000, product="BTC-USD"):
        self.now = now
        previous = self.ids.get(product)
        last = 100 if previous is None else previous + 1
        self.ids[product] = last
        if self.ingestor.venue == "gemini":
            message = {
                "s": product.replace("-", ""),
                "U": last if previous is None else previous,
                "u": last,
                "b": [["100.00", "1"]],
                "a": [["101.00", "1"]],
            }
            if lag is not None:
                message["E"] = self.wall_ns - lag
        else:
            message = {
                "channel": "l2_data",
                "events": [
                    {
                        "type": "snapshot" if previous is None else "update",
                        "product_id": product,
                        "updates": [],
                    }
                ],
            }
            if lag is not None:
                seconds, nanos = divmod(self.wall_ns - lag, 1_000_000_000)
                message["timestamp"] = (
                    datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
                    + f".{nanos:09d}Z"
                )
        return asyncio.run(self.ingestor.handle(json.dumps(message)))

    def negatives(self, start=0, end=60, **kwargs):
        for now in range(start, end + 1):
            self.level2(now, **kwargs)

    def suspected(self, product="BTC-USD"):
        value = self.metrics.registry.get_sample_value(
            METRIC, {"venue": self.ingestor.venue, "product_id": product}
        )
        assert value is not None, "every configured product must export the diagnostic from startup"
        return value


def warnings(caplog):
    return [r for r in caplog.records if r.message.startswith("suspected clock skew:")]


def test_consecutive_negatives_warn_at_exactly_sixty_seconds_once(caplog):
    feed = Feed()
    assert all(feed.suspected(p) == 0 for p in feed.ingestor.settings.pairs)
    feed.negatives(end=59)
    feed.level2(59.999)
    assert feed.suspected() == 0 and warnings(caplog) == []
    feed.now = 60  # elapsed time without another observation cannot confirm the condition
    assert feed.suspected() == 0
    feed.level2(60)
    assert feed.suspected() == 1
    feed.negatives(start=61, end=120)
    [warning] = warnings(caplog)
    assert warning.levelno == logging.WARNING
    assert "venue=gemini product_id=BTC-USD" in warning.message
    assert "60.000s" in warning.message and "62 observations" in warning.message
    assert "lag_ns=-700000000" in warning.message
    assert "host clock synchronization" in warning.message
    assert "absolute lag" in warning.message


@pytest.mark.parametrize("lag, expected", [(-1, 1), (0, 0), (1, 0)])
def test_lag_threshold_is_strictly_negative_without_an_offset(lag, expected, caplog):
    feed = Feed()
    feed.negatives(lag=lag)
    assert feed.suspected() == expected
    assert len(warnings(caplog)) == expected


def test_single_sample_and_intermittent_jitter_do_not_confirm(caplog):
    feed = Feed()
    feed.level2(0)
    feed.now = 60
    assert feed.suspected() == 0
    for now in range(61, 182):
        feed.level2(now, lag=-1 if now % 2 else 0)
    assert feed.suspected() == 0 and warnings(caplog) == []


def test_minimum_three_distinct_observations_even_with_long_staleness(caplog):
    feed = Feed(stale=61)
    feed.level2(0)
    feed.level2(60)
    assert feed.suspected() == 0 and warnings(caplog) == []
    feed.level2(60.1)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


def test_duplicate_product_items_in_one_message_count_once(monkeypatch, caplog):
    feed = Feed(stale=61)
    monkeypatch.setattr(
        feed.ingestor.adapter,
        "parse",
        lambda message: Parsed(
            exchange_ts_ns=feed.wall_ns + 1,
            items=[Item(LEVEL2, "BTC-USD")] * 3,
        ),
    )
    for now in (0, 60):
        feed.now = now
        asyncio.run(feed.ingestor.handle("{}"))
    assert feed.suspected() == 0 and warnings(caplog) == []
    feed.now = 60.1
    asyncio.run(feed.ingestor.handle("{}"))
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


@pytest.mark.parametrize("recovery_lag", [0, 1])
def test_nonnegative_observation_clears_and_rearms_episode(recovery_lag, caplog):
    feed = Feed()
    feed.negatives()
    feed.level2(61, lag=recovery_lag)
    assert feed.suspected() == 0
    feed.negatives(start=62, end=121)
    assert feed.suspected() == 0 and len(warnings(caplog)) == 1
    feed.level2(122)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 2


def test_missing_timestamp_breaks_coverage_but_does_not_rearm_warning(caplog):
    feed = Feed()
    feed.negatives(end=29)
    feed.level2(30, lag=None)
    feed.negatives(start=31, end=90)
    assert feed.suspected() == 0 and warnings(caplog) == []
    feed.level2(91)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1
    feed.level2(92, lag=None)
    assert feed.suspected() == 0
    feed.negatives(start=93, end=153)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


def test_outage_requires_new_coverage_and_scrape_expiry_is_pure(caplog):
    feed = Feed()
    feed.negatives(end=30)
    feed.negatives(start=100, end=159)
    assert feed.suspected() == 0 and warnings(caplog) == []
    feed.level2(160)
    assert feed.suspected() == 1
    feed.now = 165
    assert feed.suspected() == 1
    feed.now = 165.000001
    for _ in range(3):
        assert feed.suspected() == 0  # no incoming frame or polling task is needed to expire it
    feed.negatives(start=166, end=225)
    assert feed.suspected() == 0
    feed.level2(226)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


def test_fast_disconnect_restarts_coverage_without_rewarning(caplog):
    feed = Feed()
    feed.negatives()
    feed.now = 60.1
    asyncio.run(feed.ingestor._on_disconnect(fallback=False))
    assert feed.suspected() == 0
    feed.ingestor.adapter.reset()
    feed.ingestor.adapter.connect_frames()
    feed.ids.clear()
    feed.negatives(start=61, end=120)
    assert feed.suspected() == 0
    feed.level2(121)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


def test_resync_and_awaiting_snapshot_do_not_reset_transport_observations(caplog):
    feed = Feed()
    feed.negatives(end=30)
    asyncio.run(feed.ingestor._start_resync(("BTC-USD",), "gap", "synthetic_gap", feed.wall_ns))
    feed.negatives(start=31, end=60)
    assert "BTC-USD" in feed.ingestor.awaiting
    [envelope] = feed.level2(61)
    assert json.loads(envelope.value)["awaiting_snapshot"] is True
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


@pytest.mark.parametrize("kind", ["trade", "control", "ping", "heartbeat"])
def test_other_channels_neither_reset_nor_refresh_level2_observations(kind, caplog):
    feed = Feed(venue="coinbase" if kind == "heartbeat" else "gemini")
    feed.negatives(end=59)

    def noise(now):
        feed.now = now
        if kind == "trade":
            message = {"s": "BTCUSD", "t": 1, "p": "100.00", "q": "1", "E": feed.wall_ns - 1}
        elif kind == "ping":
            request = json.loads(feed.ingestor.adapter.ping_frame())
            message = {"id": request["id"], "status": 200}
        elif kind == "heartbeat":
            message = {"channel": "heartbeats", "events": []}
        else:
            message = {"id": 999, "status": 200}
        asyncio.run(feed.ingestor.handle(json.dumps(message)))

    noise(59.5)
    feed.level2(60)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1
    noise(66)
    assert feed.suspected() == 0
    feed.negatives(start=67, end=127)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1


@pytest.mark.parametrize("venue", ["gemini", "coinbase"])
def test_diagnostic_is_per_venue_and_product_and_process_restart_starts_fresh(venue, caplog):
    feed = Feed(venue=venue)
    for now in range(61):
        feed.level2(now)
        feed.level2(now, lag=1, product="ETH-USD")
    assert feed.suspected() == 1
    assert feed.suspected("ETH-USD") == feed.suspected("SOL-USD") == 0
    [warning] = warnings(caplog)
    assert f"venue={venue}" in warning.message
    other = "coinbase" if venue == "gemini" else "gemini"
    assert (
        feed.metrics.registry.get_sample_value(METRIC, {"venue": other, "product_id": "BTC-USD"})
        is None
    )
    restarted = Feed(venue=venue)
    restarted.level2(61)
    assert restarted.suspected() == 0
    restarted.negatives(start=62, end=121)
    assert restarted.suspected() == 1 and len(warnings(caplog)) == 2


def test_diagnostic_leaves_timestamps_payload_and_raw_lag_metrics_unchanged(caplog):
    feed = Feed()
    feed.negatives(end=30)
    feed.wall_shift_ns = -10_000_000_000  # wall-clock movement must not time the debounce
    feed.negatives(start=31, end=60)
    assert feed.suspected() == 1 and len(warnings(caplog)) == 1
    envelopes = [
        json.loads(value) for topic, _, value in feed.producer.records if topic == "md.level2"
    ]
    assert len(envelopes) == 61
    for now, envelope in enumerate(envelopes):
        expected_receive = EPOCH_NS + now * 1_000_000_000 - (10_000_000_000 if now > 30 else 0)
        assert envelope["received_at_ns"] == expected_receive
        assert (
            envelope["exchange_ts_ns"] == envelope["payload"]["E"] == expected_receive + 700_000_000
        )
        assert envelope["payload"]["b"] == [["100.00", "1"]]
    labels = {"venue": "gemini", "channel": "level2"}
    assert (
        feed.metrics.registry.get_sample_value("simex_ingest_last_feed_lag_seconds", labels) == -0.7
    )
    assert (
        feed.metrics.registry.get_sample_value("simex_ingest_feed_lag_seconds_count", labels) == 61
    )
    assert feed.metrics.registry.get_sample_value(
        "simex_ingest_feed_lag_seconds_sum", labels
    ) == pytest.approx(-0.7 * 61)
