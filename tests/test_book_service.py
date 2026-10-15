"""The book service with an in-memory md.level2 and Valkey (ADR 0012); synthetic data only."""

import asyncio
import json
import logging

import pytest

from simex.book.config import BookSettings
from simex.book.metrics import BookMetrics
from simex.book.orderbook import BookState
from simex.book.record import book_key, usable
from simex.book.service import BookService
from simex.book.source import Record, plan_start_offsets
from simex.ingest.envelope import build_envelope, build_resync_marker, topics_for
from simex.ingest.venue import LEVEL2, Item

TOPICS = topics_for("gemini")
NOW_NS = 1_792_036_800_000_000_000
NOW_MS = NOW_NS // 1_000_000
SETTINGS = BookSettings(snapshot_refresh_seconds=60)  # lookback window: 120 s
_ids = iter(range(1_000, 10**9, 10))


def depth(product, bids=(), asks=(), snapshot=False, awaiting=(), age_s=1.0) -> bytes:
    """A Gemini-shaped md.level2 envelope, received age_s seconds before NOW."""
    u = next(_ids)
    first = u if snapshot else u - 10
    symbol = product.replace("-", "")
    payload = {"e": "depthUpdate", "E": NOW_NS, "s": symbol, "U": first, "u": u,
               "b": [list(b) for b in bids], "a": [list(a) for a in asks]}  # fmt: skip
    item = Item(LEVEL2, product, update_ids=(first, u), snapshot=snapshot)
    received = NOW_NS - int(age_s * 1e9)
    return build_envelope(
        json.dumps(payload),
        item,
        venue="gemini",
        topics=TOPICS,
        received_at_ns=received,
        exchange_ts_ns=received,
        awaiting_snapshot=awaiting,
    ).value


def marker(product, age_s=1.0) -> bytes:
    received = NOW_NS - int(age_s * 1e9)
    return build_resync_marker(
        product, "r1", "update_id_gap", received, venue="gemini", topics=TOPICS
    ).value


class FakeSource:
    """md.level2 in memory: partition -> [(key, value, timestamp_ms)], optionally with older
    records already deleted (log_start)."""

    def __init__(self, partitions, log_start=None, retention_ms=None, batch=3):
        self.data = partitions
        self.batch = batch
        self.log_start = log_start or {p: 0 for p in partitions}
        self.positions = {}
        self.seeks = []
        self._retention = retention_ms
        self.stopped = False

    async def start(self):
        return sorted(self.data)

    async def offsets_for_time(self, timestamp_ms):
        result = {}
        for p, records in self.data.items():
            hits = [i for i, (_, _, ts) in enumerate(records) if ts >= timestamp_ms]
            result[p] = max(hits[0], self.log_start[p]) if hits else None
        return result

    async def beginning_offsets(self):
        return dict(self.log_start)

    async def end_offsets(self):
        return {p: len(records) for p, records in self.data.items()}

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))
        self.positions[partition] = offset

    async def fetch(self, timeout_ms):
        out = []
        for p, records in self.data.items():
            start = self.positions.get(p, self.log_start[p])
            stop = min(start + self.batch, len(records))
            for offset in range(start, stop):
                key, value, _ = records[offset]
                out.append(Record(p, offset, key.encode(), value))
            self.positions[p] = stop
        return out

    async def retention_ms(self):
        return self._retention

    async def stop(self):
        self.stopped = True


class FakeStore:
    def __init__(self, fail=False):
        self.values, self.ttls, self.fail = {}, {}, fail

    async def set(self, name, value, px):
        if self.fail:
            raise ConnectionError("valkey down")
        self.values[name], self.ttls[name] = value, px


def rec(product, value, age_s):
    return (product, value, NOW_MS - int(age_s * 1000))


def make_service(source, store=None, settings=SETTINGS):
    metrics = BookMetrics()
    service = BookService(settings, source, store or FakeStore(), metrics, clock_ns=lambda: NOW_NS)
    return service, metrics


async def drain(service):
    """Read everything there is, as dispatch_forever would, and apply it."""
    for _ in range(100):
        records = await service.source.fetch(timeout_ms=0)
        service.dispatch(records)
        for product, queue in service.queues.items():
            while not queue.empty():
                service.apply(product, queue.get_nowait())
        service.maybe_reread_from_start()
        if not records and all(q.empty() for q in service.queues.values()):
            if service.reread_done:
                return


def sample(metrics, name, **labels):
    return metrics.registry.get_sample_value(name, labels) or 0.0


# Cold start ----------------------------------------------------------------------------------


def test_plan_start_offsets():
    plan = plan_start_offsets(
        [0, 1, 2],
        by_time={0: 5, 1: None, 2: 1},
        beginnings={0: 0, 1: 0, 2: 3},
        ends={0: 9, 1: 4, 2: 7},
    )
    assert plan == {0: 5, 1: 4, 2: 3}  # in the window; all older -> end; clamped to log start


def test_cold_start_seeks_into_the_window_and_syncs_from_its_snapshot():
    old = [
        rec("BTC-USD", depth("BTC-USD", [("1", "1")], [("2", "1")], snapshot=True, age_s=500), 500),
        rec("BTC-USD", depth("BTC-USD", bids=[("1.5", "9")], age_s=400), 400),
    ]
    window = [
        rec("BTC-USD", depth("BTC-USD", bids=[("1.5", "3")], age_s=100), 100),  # EMPTY: skipped
        rec("BTC-USD", depth("BTC-USD", [("10", "1")], [("11", "2")], snapshot=True, age_s=90), 90),
        rec("BTC-USD", depth("BTC-USD", bids=[("10.5", "4")], age_s=1), 1),
    ]
    source = FakeSource({0: old + window})
    settings = BookSettings(pairs=("BTC-USD",), snapshot_refresh_seconds=60)
    service, metrics = make_service(source, settings=settings)

    async def scenario():
        await service.start()
        await drain(service)

    asyncio.run(scenario())
    assert source.seeks[0] == (0, 2)  # the first record inside the 120 s window
    book = service.books["BTC-USD"]
    assert book.state is BookState.SYNCED
    assert [(str(p), str(q)) for p, q in book.depth(5)[0]] == [("10.5", "4"), ("10", "1")]
    assert not service.catching_up("BTC-USD")
    labels = {"venue": "gemini", "product_id": "BTC-USD"}
    assert sample(metrics, "simex_book_updates_total", outcome="skipped_not_synced", **labels) == 1
    assert sample(metrics, "simex_book_replayed_from_log_start_total") == 0


def test_products_on_one_partition_each_get_their_own_book():
    records = [
        rec("BTC-USD", depth("BTC-USD", [("100", "1")], [("101", "1")], snapshot=True), 5),
        rec("ETH-USD", depth("ETH-USD", [("20", "1")], [("21", "1")], snapshot=True), 5),
        rec("ETH-USD", depth("ETH-USD", asks=[("20.5", "2")]), 4),
        rec("BTC-USD", depth("BTC-USD", bids=[("100", "0"), ("99", "5")]), 4),
        rec("DOGE-USD", depth("DOGE-USD", [("1", "1")], snapshot=True), 3),  # not a pair: ignored
    ]
    service, _ = make_service(FakeSource({0: records, 1: [], 2: []}))
    asyncio.run(_start_and_drain(service))
    btc, eth, sol = (service.books[p] for p in ("BTC-USD", "ETH-USD", "SOL-USD"))
    assert [str(p) for p, _ in btc.depth(5)[0]] == ["99"]
    assert [str(p) for p, _ in eth.depth(5)[1]] == ["20.5", "21"]
    assert sol.state is BookState.EMPTY


async def _start_and_drain(service):
    await service.start()
    await drain(service)


@pytest.mark.parametrize("batch", [1, 3, 50])
def test_reread_from_log_start_when_the_window_has_no_snapshot(caplog, batch):
    # SOL's only snapshot is older than the window; BTC has one inside it.
    records = [
        rec("SOL-USD", depth("SOL-USD", [("3", "1")], [("4", "1")], snapshot=True, age_s=600), 600),
        rec("BTC-USD", depth("BTC-USD", [("1", "1")], [("2", "1")], snapshot=True, age_s=550), 550),
        rec("SOL-USD", depth("SOL-USD", bids=[("3.5", "2")], age_s=60), 60),
        rec("BTC-USD", depth("BTC-USD", [("50", "1")], [("60", "1")], snapshot=True, age_s=30), 30),
        rec("BTC-USD", depth("BTC-USD", bids=[("55", "7")], age_s=10), 10),
    ]
    source = FakeSource({0: records}, batch=batch)
    settings = BookSettings(pairs=("BTC-USD", "SOL-USD"), snapshot_refresh_seconds=60)
    service, metrics = make_service(source, settings=settings)
    with caplog.at_level(logging.WARNING, logger="simex.book"):
        asyncio.run(_start_and_drain(service))

    sol, btc = service.books["SOL-USD"], service.books["BTC-USD"]
    assert sol.state is BookState.SYNCED  # rebuilt from the old snapshot and every diff after it
    assert [str(p) for p, _ in sol.depth(5)[0]] == ["3.5", "3"]
    # BTC was already SYNCED: the re-read must not replay its old snapshot over the new book.
    assert [str(p) for p, _ in btc.depth(5)[0]] == ["55", "50"]
    assert sample(metrics, "simex_book_replayed_from_log_start_total") == 1
    assert any("re-reading md.level2 from the log start" in m for m in caplog.messages)
    assert service.reread_done and not service.catching_up("SOL-USD")


def test_retention_shorter_than_the_window_logs_an_error(caplog):
    source = FakeSource({0: []}, retention_ms=60_000)  # 1 min < 2 x 60 s refresh
    service, _ = make_service(source)
    with caplog.at_level(logging.ERROR, logger="simex.book"):
        asyncio.run(service.start())
    assert any("less than 2x the snapshot refresh" in m for m in caplog.messages)


# Markers, crossed books, publishing ----------------------------------------------------------


def test_marker_and_crossed_message_drop_the_book_and_publish_not_ready():
    records = [
        rec("BTC-USD", depth("BTC-USD", [("100", "1")], [("101", "1")], snapshot=True), 5),
        rec("BTC-USD", marker("BTC-USD"), 4),
        rec("BTC-USD", depth("BTC-USD", bids=[("100", "2")], awaiting={"BTC-USD"}), 4),
        rec("BTC-USD", depth("BTC-USD", [("100", "1")], [("101", "1")], snapshot=True), 3),
        rec("BTC-USD", depth("BTC-USD", bids=[("102", "1")]), 2),  # crosses the 101 ask
    ]
    store = FakeStore()
    service, metrics = make_service(FakeSource({0: records}), store)

    async def scenario():
        await service.start()
        await drain(service)
        await service.publish_once()

    asyncio.run(scenario())
    labels = {"venue": "gemini", "product_id": "BTC-USD"}
    assert service.books["BTC-USD"].state is BookState.RESYNCING
    assert sample(metrics, "simex_book_resyncs_total", reason="marker", **labels) == 1
    assert sample(metrics, "simex_book_resyncs_total", reason="crossed", **labels) == 1
    assert sample(metrics, "simex_book_crossed_total", **labels) == 1
    assert sample(metrics, "simex_book_updates_total", outcome="skipped_awaiting", **labels) == 1
    assert sample(metrics, "simex_book_state", state="RESYNCING", **labels) == 1
    assert sample(metrics, "simex_book_state", state="SYNCED", **labels) == 0

    record = json.loads(store.values[book_key("gemini", "BTC-USD")])
    assert record["state"] == "RESYNCING"
    assert record["best_bid"] is None and record["bids"] == [] and record["mid"] is None
    assert not usable(record, NOW_NS)


def test_published_record_for_a_synced_book():
    bids = [(str(100 - i), "1.50") for i in range(15)]
    asks = [(str(101 + i), "0.25") for i in range(15)]
    records = [rec("ETH-USD", depth("ETH-USD", bids, asks, snapshot=True, age_s=0.5), 1)]
    store = FakeStore()
    settings = BookSettings(snapshot_refresh_seconds=60, publish_depth=3)
    service, metrics = make_service(FakeSource({0: records}), store, settings)

    async def scenario():
        await service.start()
        await drain(service)
        await service.publish_once()

    asyncio.run(scenario())
    key = "simex:book:gemini:ETH-USD"
    assert store.ttls[key] == 2000
    record = json.loads(store.values[key])
    assert record["v"] == 1 and record["state"] == "SYNCED" and record["catching_up"] is False
    assert record["best_bid"] == ["100", "1.50"] and record["best_ask"] == ["101", "0.25"]
    assert record["mid"] == "100.5" and record["spread"] == "1"
    assert record["bids"] == [["100", "1.50"], ["99", "1.50"], ["98", "1.50"]]
    assert len(record["asks"]) == 3 and record["levels"] == {"bids": 15, "asks": 15}
    assert record["last_update_id"] is not None and record["published_at_ns"] == NOW_NS
    assert usable(record, NOW_NS)
    assert not usable(record, NOW_NS + 6 * 10**9)  # nothing received for over 5 s: stale
    assert not usable({**record, "catching_up": True}, NOW_NS)
    assert not usable(None, NOW_NS)  # key missing: service down or evicted
    labels = {"venue": "gemini", "product_id": "ETH-USD"}
    assert sample(metrics, "simex_book_levels", side="bid", **labels) == 15
    assert sample(metrics, "simex_book_seconds_since_synced", **labels) == 0


def test_valkey_down_is_counted_not_fatal():
    service, metrics = make_service(FakeSource({0: []}), FakeStore(fail=True))
    asyncio.run(service.publish_once())
    assert sample(metrics, "simex_book_valkey_errors_total") == len(SETTINGS.pairs)


def test_run_end_to_end_with_tasks():
    records = [
        rec("SOL-USD", depth("SOL-USD", [("3", "1")], [("3.1", "1")], snapshot=True), 2),
        rec("SOL-USD", depth("SOL-USD", asks=[("3.05", "8")]), 1),
    ]
    store = FakeStore()
    settings = BookSettings(snapshot_refresh_seconds=60, publish_interval_seconds=0.01)
    service, _ = make_service(FakeSource({0: records, 1: []}), store, settings)

    async def scenario():
        task = asyncio.create_task(service.run())
        key = book_key("gemini", "SOL-USD")
        for _ in range(300):
            await asyncio.sleep(0.01)
            if key in store.values and json.loads(store.values[key])["best_ask"] == ["3.05", "8"]:
                break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return json.loads(store.values[key])

    record = asyncio.run(scenario())
    assert record["state"] == "SYNCED" and record["best_ask"] == ["3.05", "8"]
    assert record["catching_up"] is False and service.source.stopped


def test_settings_from_env():
    settings = BookSettings.from_env(
        {
            "BOOK_PUBLISH_DEPTH": "20",
            "VALKEY_PORT": "6380",
            "INGEST_SNAPSHOT_REFRESH_SECONDS": "900",
            "SIMEX_PAIRS": "btc-usd",
        }
    )
    assert settings.publish_depth == 20 and settings.valkey_url == "redis://127.0.0.1:6380/0"
    assert settings.lookback_seconds == 1800 and settings.pairs == ("BTC-USD",)
    assert BookSettings.from_env({}).publish_depth == 10


def test_catching_up_latches_off_and_never_flaps_in_steady_state():
    """A book leaves catch-up once; queued-but-not-yet-applied records or a quiet spell later
    must never flip it back (live run: SOL flapped 20 times in 30 min)."""
    records = [
        rec("BTC-USD", depth("BTC-USD", [("100", "1")], [("101", "1")], snapshot=True), 5),
        rec("SOL-USD", depth("SOL-USD", [("3", "1")], [("3.1", "1")], snapshot=True), 5),
        rec("BTC-USD", depth("BTC-USD", bids=[("99", "1")]), 4),
    ]
    source = FakeSource({0: records, 1: []})  # BTC and SOL share partition 0; p1 is empty
    settings = BookSettings(pairs=("BTC-USD", "SOL-USD"), snapshot_refresh_seconds=60)
    service, metrics = make_service(source, settings=settings)

    async def scenario():
        await service.start()
        # Before catch-up both books are catching up.
        assert service.catching_up("BTC-USD") and service.catching_up("SOL-USD")
        await drain(service)
        assert not service.catching_up("BTC-USD") and not service.catching_up("SOL-USD")

        # Steady state: new records arrive and are queued but not applied yet (the window in
        # which the publisher used to see a non-empty queue). SOL stays quiet throughout.
        for _ in range(20):
            source.data[0].append(rec("BTC-USD", depth("BTC-USD", bids=[("99", "2")]), 0))
            source.data[0].append(rec("SOL-USD", depth("SOL-USD", asks=[("3.2", "1")]), 0))
            service.dispatch(await source.fetch(timeout_ms=0))
            assert not service.queues["SOL-USD"].empty()  # queued, not yet applied
            assert not service.catching_up("SOL-USD") and not service.catching_up("BTC-USD")
            await service.publish_once()
            for product, queue in service.queues.items():
                while not queue.empty():
                    service.apply(product, queue.get_nowait())
            service.maybe_reread_from_start()

    asyncio.run(scenario())
    record = json.loads(service.store.values[book_key("gemini", "SOL-USD")])
    assert record["catching_up"] is False and usable(record, NOW_NS)
    labels = {"venue": "gemini", "product_id": "SOL-USD"}
    assert sample(metrics, "simex_book_catching_up", **labels) == 0
