"""The live runner (ADR 0015) on a simulated Kafka: same fills as replay, digests that match."""

import asyncio
import bisect
import json
import random
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from test_exchange_engine_properties import outcome

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule
from simex.exchange.engine import EngineSettings, PairEngine
from simex.exchange.merge import Arrival
from simex.exchange.runner import (
    HOUR_NS,
    BookReadyGate,
    ExchangeRunner,
    HourDigests,
    replay_digests,
)
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.exchange.source import Fetched, Record
from simex.ingest.envelope import build_envelope, topics_for
from simex.ingest.venue import LEVEL2, TRADES, Item
from simex.marketdata import decode

TOPICS = topics_for("gemini")
L2, TR = TOPICS.level2, TOPICS.trades
MS = 1_000_000
T0 = 1_792_036_800_000_000_000
PAIRS = ("BTC-USD", "ETH-USD")
PARTITION = {(L2, "BTC-USD"): 0, (L2, "ETH-USD"): 0, (TR, "BTC-USD"): 0, (TR, "ETH-USD"): 1}
SIZE = {"BTC-USD": "0.001", "ETH-USD": "0.01"}
_update_ids = iter(range(10, 10**9, 10))
_trade_ids = iter(range(1, 10**9))


def level2(product, t, bids=(), asks=(), snapshot=False):
    u = next(_update_ids)
    payload = {"e": "depthUpdate", "E": t, "s": product.replace("-", ""),
               "U": u if snapshot else u - 9, "u": u,
               "b": [list(b) for b in bids], "a": [list(a) for a in asks]}  # fmt: skip
    item = Item(LEVEL2, product, update_ids=(payload["U"], u), snapshot=snapshot)
    env = build_envelope(json.dumps(payload), item, venue="gemini", topics=TOPICS,
                         received_at_ns=t, exchange_ts_ns=t)  # fmt: skip
    return env.value


def trade(product, t, price, size, maker_buyer):
    payload = {"E": t, "m": maker_buyer, "p": price, "q": size, "s": product.replace("-", ""),
               "t": next(_trade_ids)}  # fmt: skip
    env = build_envelope(json.dumps(payload), Item(TRADES, product), venue="gemini",
                         topics=TOPICS, received_at_ns=t, exchange_ts_ns=t)  # fmt: skip
    return env.value


def market(seed, n):
    """Records in receive order: a snapshot per pair, then diffs near the touch and prints at
    the touch, with ties in received time. The books never cross."""
    rng = random.Random(seed)
    bids = [f"{99.99 - i / 100:.2f}" for i in range(6)]
    asks = [f"{100.01 + i / 100:.2f}" for i in range(6)]
    out = []
    for p in PAIRS:
        snap = level2(p, T0, [(b, "5") for b in bids], [(a, "5") for a in asks], snapshot=True)
        out.append((L2, p, T0, snap))
    t = T0 + 500 * MS
    for _ in range(n):
        if rng.random() > 0.2:
            t += rng.randint(0, 30) * MS
        p = rng.choice(PAIRS)
        if rng.random() < 0.7:
            side = rng.choice(["bid", "ask"])
            price = rng.choice(bids if side == "bid" else asks)
            level = [(price, str(rng.choice([0, 1, 2, 5, 8])))]
            value = level2(p, t, *((level, ()) if side == "bid" else ((), level)))
            out.append((L2, p, t, value))
        else:
            hit_bid = rng.random() < 0.5
            price = rng.choice(bids[:2] if hit_bid else asks[:2])
            out.append((TR, p, t, trade(p, t, price, rng.choice(["0.5", "2", "6"]), hit_bid)))
    return out


def orders(seed, end_ns):
    """Limit orders at the touch and one step inside, submitted after the books are SYNCED."""
    rng = random.Random(seed + 1)
    t, out = T0 + 1000 * MS, []
    while t < end_ns:
        p = rng.choice(PAIRS)
        side = rng.choice(["buy", "sell"])
        price = rng.choice(["99.99", "99.98"] if side == "buy" else ["100.01", "100.02"])
        out.append((t, p, side, price, f"o{len(out)}"))
        t += rng.randint(50, 400) * MS
    return out


def fund(writer):
    for asset, amount in (("USD", "1000000"), ("BTC", "100"), ("ETH", "1000")):
        writer.accounts.deposit("alice", asset, D(amount))


def payload(product, side, price, cid):
    return {"client_order_id": cid, "product_id": product, "side": side, "type": "limit",
            "quantity": SIZE[product], "limit_price": price}  # fmt: skip


def late_count(runner):
    samples = runner.metrics.late.collect()[0].samples
    return sum(s.value for s in samples if s.name.endswith("_total"))


class FakeKafka:
    """Two topics: each record becomes visible some delay after it was received, never before
    the one before it in its partition. A fetch returns some of what is visible at the wall
    time, and reports the partitions read to the end."""

    def __init__(self, records, rng, max_delay_ms):
        self.rng = rng
        self.logs = {tp: [] for tp in {(t, p) for (t, _), p in PARTITION.items()}}
        last = {}
        for topic, product, received, value in records:
            tp = (topic, PARTITION[(topic, product)])
            visible = max(last.get(tp, 0), received + rng.randint(0, max_delay_ms) * MS)
            last[tp] = visible
            offset = len(self.logs[tp])
            self.logs[tp].append((visible, Record(topic, tp[1], offset, product.encode(), value)))
        self.position = dict.fromkeys(self.logs, 0)
        self.visible = {tp: [v for v, _ in log] for tp, log in self.logs.items()}
        self.wall = T0

    async def start(self):
        out = {}
        for topic, p in self.logs:
            out.setdefault(topic, []).append(p)
        return {t: sorted(ps) for t, ps in out.items()}

    async def offsets_for_time(self, timestamp_ms):
        return dict.fromkeys(self.logs, 0)

    async def beginning_offsets(self):
        return dict.fromkeys(self.logs, 0)

    async def end_offsets(self):
        return {tp: len(log) for tp, log in self.logs.items()}

    def seek(self, topic, partition, offset):
        self.position[(topic, partition)] = offset

    async def fetch(self, timeout_ms):
        records, caught_up = [], []
        for tp, log in self.logs.items():
            pos = self.position[tp]
            visible = bisect.bisect_right(self.visible[tp], self.wall) - pos  # monotone
            take = self.rng.randint(0, visible) if visible else 0
            records += [r for _, r in log[pos : pos + take]]
            self.position[tp] = pos + take
            if self.position[tp] == len(log) or log[self.position[tp]][0] > self.wall:
                caught_up.append(tp)
        return Fetched(records, caught_up, self.wall)

    async def stop(self):
        pass


def run_live(records, order_list, seed, max_delay_ms, holdback_ms=100):
    rng = random.Random(seed)
    kafka = FakeKafka(records, rng, max_delay_ms)
    runner_settings = RunnerSettings(pairs=PAIRS, holdback_ms=holdback_ms, digest_path="")
    runner = ExchangeRunner(
        runner_settings, kafka, RunnerMetrics(), wall_ns=lambda: kafka.wall,
        engine_settings=EngineSettings(),
    )  # fmt: skip
    fund(runner.writer)
    pending = list(order_list)
    end = max(r[2] for r in records) + 2000 * MS

    async def go():
        await runner.start()
        while kafka.wall < end:
            kafka.wall += rng.randint(1, 30) * MS
            while pending and pending[0][0] <= kafka.wall:
                t, p, side, price, cid = pending.pop(0)
                order = runner.writer.submit("alice", payload(p, side, price, cid), t)
                runner.engines[p].submit(order)
            runner.handle(await kafka.fetch(0))
        for engine in runner.engines.values():
            engine.advance(end + 10_000 * MS)

    asyncio.run(go())
    return runner


def run_replay(records, order_list):
    """The engines fed in replay order: (received, topic, partition, offset)."""
    engines = {}
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), BookReadyGate(engines), 200 * MS)
    fund(writer)
    for p in PAIRS:
        engines[p] = PairEngine(p, writer, EngineSettings())
    offsets, keyed = {}, []
    for topic, product, received, value in records:
        tp = (topic, PARTITION[(topic, product)])
        offsets[tp] = offsets.get(tp, -1) + 1
        keyed.append(((received, topic, tp[1], offsets[tp]), product, value))
    keyed.sort(key=lambda k: k[0])
    pending = list(order_list)
    for key, product, value in keyed:
        while pending and pending[0][0] < key[0]:
            t, p, side, price, cid = pending.pop(0)
            engines[p].submit(writer.submit("alice", payload(p, side, price, cid), t))
        for event in decode(value):
            engines[product].on_event(event)
    end = max(r[2] for r in records) + 2000 * MS
    for t, p, side, price, cid in pending:
        engines[p].submit(writer.submit("alice", payload(p, side, price, cid), t))
    for engine in engines.values():
        engine.advance(end + 10_000 * MS)
    return writer, keyed


@settings(max_examples=40, deadline=None)
@given(st.integers(0, 10**9), st.integers(50, 400), st.integers(0, 80))
def test_live_fills_and_digests_equal_replay_within_the_holdback(seed, n, max_delay_ms):
    records = market(seed, n)
    order_list = orders(seed, max(r[2] for r in records))
    live = run_live(records, order_list, seed, max_delay_ms)
    writer, keyed = run_replay(records, order_list)
    assert late_count(live) == 0
    assert outcome(live.writer) == outcome(writer)
    live.digests.close_all()
    expected = replay_digests(
        (SimpleNamespace(product_id=p, received_at_ns=k[0], topic=k[1], partition=k[2],
                         offset=k[3]) for k, p, _ in keyed),
        PAIRS,
    )  # fmt: skip
    got = {(d["product_id"], d["hour_start_ns"]): (d["records"], d["sha256"])
           for d in live.digests.lines}  # fmt: skip
    assert got == expected


def test_the_scenario_fills_orders():
    """Guard against a vacuous equality: the generated orders do fill, by prints."""
    records = market(7, 400)
    live = run_live(records, orders(7, max(r[2] for r in records)), 7, 40)
    causes = [e["fill"]["cause"] for e in live.writer.events if e["type"] == "fill"]
    assert len(causes) >= 5 and set(causes) & {"print", "trade_through"}


def test_beyond_the_holdback_late_records_are_applied_and_counted():
    records = market(3, 300)
    live = run_live(records, [], 3, max_delay_ms=400, holdback_ms=20)
    assert late_count(live) > 0
    released = sum(
        s.value for s in live.metrics.events.collect()[0].samples if s.name.endswith("_total")
    )
    assert released == len(records)  # every record applied once, late or not


def test_cold_start_seeks_both_topics_by_time():
    class Seeks(FakeKafka):
        async def offsets_for_time(self, timestamp_ms):
            self.asked = timestamp_ms
            return {tp: (2 if tp[0] == L2 else None) for tp in self.logs}

    kafka = Seeks(market(1, 20), random.Random(1), 0)
    runner = ExchangeRunner(
        RunnerSettings(pairs=PAIRS, digest_path=""), kafka, RunnerMetrics(),
        wall_ns=lambda: T0, engine_settings=EngineSettings(),
    )  # fmt: skip
    asyncio.run(runner.start())
    assert kafka.asked == T0 // MS - 3600 * 1000  # 2 x the 30-minute refresh
    for tp, log in kafka.logs.items():
        assert kafka.position[tp] == (min(2, len(log)) if tp[0] == L2 else len(log))


def test_cold_start_reads_the_ends_before_the_time_lookup():
    """A partition empty at the time lookup whose first record lands before the end is read
    would be skipped if the end were read second."""

    class Race(FakeKafka):
        calls = []

        async def end_offsets(self):
            self.calls.append("ends")
            return await super().end_offsets()

        async def offsets_for_time(self, timestamp_ms):
            self.calls.append("time")
            return dict.fromkeys(self.logs)  # every partition: nothing at or after the time

    kafka = Race(market(1, 20), random.Random(1), 0)
    runner = ExchangeRunner(
        RunnerSettings(pairs=PAIRS, digest_path=""), kafka, RunnerMetrics(),
        wall_ns=lambda: T0, engine_settings=EngineSettings(),
    )  # fmt: skip
    asyncio.run(runner.start())
    assert kafka.calls == ["ends", "time"]


def test_hours_are_complete_only_when_the_runner_saw_both_sides():
    digests = HourDigests(None)
    h = T0 // HOUR_NS * HOUR_NS
    for i, t in enumerate([h + 10, h + HOUR_NS + 5, h + HOUR_NS + 7, h + 2 * HOUR_NS + 1]):
        digests.add(Arrival(L2, 0, i, t, "BTC-USD", b""), late=False)
    digests.close_all()
    assert [(d["records"], d["complete"]) for d in digests.lines] == [
        (1, False),  # the hour the runner started in
        (2, True),
        (1, False),  # open at shutdown
    ]


def test_the_holdback_setting():
    with pytest.raises(ValueError):
        RunnerSettings(holdback_ms=-1)
    assert RunnerSettings.from_env({"EXCHANGE_MERGE_HOLDBACK_MS": "250"}).holdback_ms == 250
    assert RunnerSettings.from_env({"EXCHANGE_ORDER_DELAY_MS": "125"}).order_delay_ms == 125
    with pytest.raises(ValueError):
        RunnerSettings.from_env({"EXCHANGE_ORDER_DELAY_MS": "-1"})


def runner_on(kafka, **settings):
    return ExchangeRunner(
        RunnerSettings(pairs=PAIRS, digest_path="", **settings), kafka, RunnerMetrics(),
        wall_ns=lambda: kafka.wall, engine_settings=EngineSettings(),
    )  # fmt: skip


def histogram_count(runner):
    samples = runner.metrics.delivery_seconds.collect()[0].samples
    return sum(s.value for s in samples if s.name.endswith("_count"))


def test_delivery_leaves_out_the_catch_up():
    """An hour-old backlog read at start: its delay is the backlog's age, not delivery."""
    records = market(4, 200)
    kafka = FakeKafka(records, random.Random(4), 0)
    kafka.wall = max(r[2] for r in records) + 3600 * 10**9
    runner = runner_on(kafka)

    async def go():
        await runner.start()
        for _ in range(50):
            runner.handle(await kafka.fetch(0))

    asyncio.run(go())
    assert histogram_count(runner) == 0

    class ReadsAll(random.Random):
        def randint(self, a, b):
            return b

    # In steady state it counts: a new record, consumed with partitions already caught up
    # (and read whole, as Kafka returns what is there).
    kafka.rng = ReadsAll()
    t = kafka.wall - 5 * MS
    value = trade("BTC-USD", t, "99.99", "1", True)
    log = kafka.logs[(TR, PARTITION[(TR, "BTC-USD")])]
    log.append((t, Record(TR, PARTITION[(TR, "BTC-USD")], len(log), b"BTC-USD", value)))
    kafka.visible[(TR, PARTITION[(TR, "BTC-USD")])].append(t)
    for _ in range(20):
        runner.handle(asyncio.run(kafka.fetch(0)))
    assert histogram_count(runner) == 1


def test_out_of_order_and_late_records_are_logged_with_their_type(caplog):
    # Two level2 records in one partition, the second received 300 ms before the first: the
    # ingestor's held audits looked like this.
    first = level2("BTC-USD", T0 + 1002 * MS, [("99.99", "1")])
    early = level2("BTC-USD", T0 + 700 * MS, [("99.98", "1")])
    kafka = FakeKafka(market(5, 0), random.Random(5), 0)
    runner = runner_on(kafka, holdback_ms=0)

    async def go():
        await runner.start()
        kafka.wall = T0 + 1001 * MS
        runner.handle(await kafka.fetch(0))  # the snapshots
        records = [Record(L2, 0, 10, b"BTC-USD", first)]
        runner.handle(Fetched(records, list(kafka.logs), kafka.wall))
        records = [Record(L2, 0, 11, b"BTC-USD", early)]
        runner.handle(Fetched(records, list(kafka.logs), kafka.wall + MS))

    with caplog.at_level("WARNING", logger="simex.exchange.runner"):
        asyncio.run(go())
    assert runner.bounds.out_of_order == 1 and late_count(runner) == 1
    text = "\n".join(caplog.messages)
    assert "offset 11 (BTC-USD, data) was received 302 ms before" in text
    assert "late: md.level2 p0 offset 11 (BTC-USD, data)" in text
