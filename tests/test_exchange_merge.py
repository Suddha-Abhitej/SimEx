"""The live merge and watermark (ADR 0015) against replay's order, on simulated Kafka timing."""

import random

from hypothesis import given, settings
from hypothesis import strategies as st

from simex.exchange.merge import NEVER, Arrival, Bounds, PairMerge

MS = 1_000_000
T0 = 1_792_036_800_000_000_000
L2, TR = "md.level2", "md.trades"
PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
# As live: two products can share a partition, and the topics map them differently.
PARTITION = {
    (L2, "BTC-USD"): 0,
    (L2, "ETH-USD"): 1,
    (L2, "SOL-USD"): 1,
    (TR, "BTC-USD"): 0,
    (TR, "ETH-USD"): 0,
    (TR, "SOL-USD"): 1,
}


def kafka_log(seed, n, max_delay_ms, tie_rate=0.2):
    """Records in receive order, each visible in Kafka some delay after it was received;
    within a partition, a record is never visible before the one before it (log order)."""
    rng = random.Random(seed)
    t, offsets, visible_at, records = T0, {}, {}, []
    for _ in range(n):
        if rng.random() > tie_rate:
            t += rng.randint(0, 40) * MS
        topic = rng.choice([L2, L2, L2, TR])
        product = rng.choice(PAIRS)
        tp = (topic, PARTITION[(topic, product)])
        offset = offsets.get(tp, 0)
        offsets[tp] = offset + 1
        visible = max(visible_at.get(tp, 0), t + rng.randint(0, max_delay_ms) * MS)
        visible_at[tp] = visible
        records.append((visible, Arrival(topic, tp[1], offset, t, product, b"")))
    return records, rng


def run_live(records, rng, holdback_ms):
    """Consume as the runner does: fetches at advancing wall times return some of what is
    visible per partition, then caught-up partitions raise their bound."""
    bounds = Bounds(holdback_ms * MS)
    for topic in (L2, TR):
        bounds.add_partitions(topic, {p for (t, _), p in PARTITION.items() if t == topic})
    merges = {p: PairMerge(p, (L2, TR)) for p in PAIRS}
    by_tp = {(t, p): [] for (t, _), p in PARTITION.items()}  # every partition, empty or not
    for visible, arrival in records:
        by_tp.setdefault((arrival.topic, arrival.partition), []).append((visible, arrival))
    position = dict.fromkeys(by_tp, 0)
    out = {p: [] for p in PAIRS}
    late = 0
    wall = T0
    end = max(v for v, _ in records) + 1000 * MS
    while wall <= end:
        wall += rng.randint(1, 30) * MS
        for tp, log in by_tp.items():
            available = sum(1 for v, _ in log[position[tp] :] if v <= wall)
            take = rng.randint(0, available) if available else 0
            for _, arrival in log[position[tp] : position[tp] + take]:
                bounds.consumed(arrival)
                if not merges[arrival.product].add(arrival):
                    late += 1
                    out[arrival.product].append(arrival)
            position[tp] += take
            if position[tp] == len(log) or log[position[tp]][0] > wall:
                bounds.caught_up(*tp, wall)
        for product, merge in merges.items():
            out[product] += merge.release(merge.watermark(bounds))
    return out, late, merges


@settings(max_examples=200, deadline=None)
@given(st.integers(0, 10**9), st.integers(1, 300), st.integers(0, 80))
def test_within_the_holdback_live_release_is_replay_order(seed, n, max_delay_ms):
    records, rng = kafka_log(seed, n, max_delay_ms)
    out, late, merges = run_live(records, rng, holdback_ms=100)
    assert late == 0
    for product in PAIRS:
        expected = sorted((a for _, a in records if a.product == product), key=lambda a: a.key)
        assert out[product] == expected
        assert merges[product].held == 0


@settings(max_examples=200, deadline=None)
@given(st.integers(0, 10**9), st.integers(1, 300))
def test_beyond_the_holdback_every_record_is_applied_once_and_lateness_is_seen(seed, n):
    records, rng = kafka_log(seed, n, max_delay_ms=400)
    out, late, _ = run_live(records, rng, holdback_ms=20)
    for product in PAIRS:
        expected = sorted((a for _, a in records if a.product == product), key=lambda a: a.key)
        assert sorted(out[product], key=lambda a: a.key) == expected  # each once
        if out[product] != expected:
            assert late > 0  # a wrong order is never silent


def test_nothing_is_released_until_every_topic_has_a_bound():
    bounds = Bounds(100 * MS)
    bounds.add_partitions(L2, [0])
    bounds.add_partitions(TR, [0])
    merge = PairMerge("BTC-USD", (L2, TR))
    first = Arrival(L2, 0, 0, T0, "BTC-USD", b"")
    bounds.consumed(first)
    merge.add(first)
    assert merge.watermark(bounds) == NEVER  # md.trades says nothing yet
    assert merge.release(merge.watermark(bounds)) == []
    bounds.caught_up(TR, 0, T0 + 50 * MS)  # trades caught up: bound T0 - 50 ms
    assert merge.release(merge.watermark(bounds)) == []
    bounds.caught_up(TR, 0, T0 + 150 * MS)
    bounds.caught_up(L2, 0, T0 + 150 * MS)
    assert merge.release(merge.watermark(bounds)) == [first]


def test_a_record_at_the_watermark_waits_for_a_tie():
    bounds = Bounds(0)
    bounds.add_partitions(L2, [0])
    bounds.add_partitions(TR, [0])
    merge = PairMerge("BTC-USD", (L2, TR))
    trade = Arrival(TR, 0, 0, T0, "BTC-USD", b"")
    bounds.consumed(trade)
    merge.add(trade)
    bounds.caught_up(L2, 0, T0)  # level2: nothing before T0, but one at T0 could still come
    assert merge.release(merge.watermark(bounds)) == []
    book = Arrival(L2, 0, 0, T0, "BTC-USD", b"")
    bounds.consumed(book)
    assert merge.add(book)
    bounds.caught_up(L2, 0, T0 + MS)
    bounds.caught_up(TR, 0, T0 + MS)
    assert merge.release(merge.watermark(bounds)) == [book, trade]  # md.level2 sorts first


def test_a_record_below_the_cleared_time_is_late():
    bounds = Bounds(0)
    bounds.add_partitions(L2, [0])
    bounds.add_partitions(TR, [0])
    merge = PairMerge("BTC-USD", (L2, TR))
    bounds.caught_up(L2, 0, T0 + 10 * MS)
    bounds.caught_up(TR, 0, T0 + 10 * MS)
    merge.release(merge.watermark(bounds))  # cleared up to T0 + 10 ms with nothing held
    assert not merge.add(Arrival(TR, 0, 0, T0 + 5 * MS, "BTC-USD", b""))
    assert merge.add(Arrival(TR, 0, 1, T0 + 10 * MS, "BTC-USD", b""))
