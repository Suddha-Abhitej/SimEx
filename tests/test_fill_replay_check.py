"""The fill replay check's markout test (#8, ADR 0014 §B), on hand-made markouts."""

import importlib.util
from decimal import Decimal as D
from pathlib import Path

import pytest
from test_mcp import OpenGate, order_payload

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, FillCause, Liquidity
from simex.marketdata import BookUpdate, Trade

SCRIPT = Path(__file__).resolve().parent.parent / "tools" / "fill_replay_check.py"
spec = importlib.util.spec_from_file_location("fill_replay_check", SCRIPT)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)

P = "BTC-USD"


def test_markouts_are_positive_when_the_mid_moves_the_makers_way():
    assert check.markout_bps("buy", D("100"), D("100.01")) == D("1")
    assert check.markout_bps("sell", D("100"), D("100.01")) == D("-1")


def markouts(kind, values_by_price, horizons=check.MARKOUT_S):
    m = check.Markouts()
    for price, values in values_by_price.items():
        for h in horizons:
            m.values[((kind, P, "buy", D(price)), h)] = [D(v) for v in values]
    return m


class FakeRun:
    name = "proportional/print"

    def __init__(self, markouts):
        self.markouts = markouts


def test_maker_fills_better_than_real_prints_at_the_same_price_fail():
    real = markouts("real", {"100.00": ["-2", "0"], "99.99": ["5"]})
    # At 100.00 the real makers averaged -1 bp; ours averaged +0.5 bp: 1.5 bp better.
    ours = markouts("sim", {"100.00": ["0.5"] * 25})
    lines, problems = check.compare_markouts(FakeRun(ours), real, P)
    assert len(problems) == len(check.MARKOUT_S)
    assert "+1.50 bp over 25" in lines[0]


def test_close_to_real_passes_and_too_few_or_unmatched_fills_are_not_judged():
    real = markouts("real", {"100.00": ["-1"]})
    ours = markouts("sim", {"100.00": ["-0.5"] * 30, "99.98": ["9"] * 5})  # 0.5 bp better
    lines, problems = check.compare_markouts(FakeRun(ours), real, P)
    assert problems == [] and "(5 unmatched)" in lines[0]
    few = markouts("sim", {"100.00": ["9"] * 3})
    lines, problems = check.compare_markouts(FakeRun(few), real, P)
    assert problems == [] and "too few to judge" in lines[0]


def collection_run(size=3):
    run = check.Run(*check.RUNS[0], [], check.SECOND * 60, check.SECOND * 300)
    run.writer = AccountWriter(
        DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), 0, event_buffer_size=size
    )
    run.writer.deposit("probe", "USD", D("10000"), "synthetic-seed")
    return run


def test_collect_fills_tracks_sequence_across_wraps_without_duplicates():
    run = collection_run()
    makers, takers = {}, set()
    for index in range(20):
        order = run.writer.submit("probe", order_payload(str(index)), index * check.SECOND)
        run.passive[order.order_id] = P
        run._collect_fills()
        run.writer.activate(order.order_id, order.effective_at_ns)
        run._collect_fills()
        liquidity = Liquidity.MAKER if index % 2 else Liquidity.TAKER
        fill = run.writer.record_fill(
            order.order_id,
            D("100"),
            D("0.1"),
            liquidity,
            FillCause.PRINT if index % 2 else FillCause.TAKER,
            order.effective_at_ns,
        )
        if liquidity is Liquidity.MAKER:
            makers[order.order_id] = fill.ts_ns
        else:
            takers.add(order.order_id)
        run._collect_fills()
        run._collect_fills()
    assert len(run.writer.events) == 3
    assert run.seen_seq == run.writer.event_sequence == 60
    assert run.first_maker_fill == makers
    assert run.taker_on_arrival == takers
    for horizon in check.MARKOUT_S:
        due = run.markouts.due[(P, horizon)]
        assert [entry[0] for entry in due] == [
            ts + horizon * check.SECOND for ts in makers.values()
        ]
        assert all(entry[1:] == (("sim", P, "buy", D("100")), "buy", D("100")) for entry in due)


def test_collect_fills_aborts_on_eviction_before_advancing_or_using_partial_history():
    run = collection_run()
    run.writer._emit("order_accepted", order={})
    run._collect_fills()
    for index in range(5):
        run.writer._emit(
            "fill",
            fill={
                "order_id": str(index),
                "product_id": P,
                "side": "buy",
                "price": "100",
                "ts_ns": index,
                "liquidity": "maker",
            },
        )
        run.passive[str(index)] = P
    with pytest.raises(RuntimeError, match=r"2 writer events evicted.*accuracy results invalid"):
        run._collect_fills()
    assert run.seen_seq == 1
    assert run.first_maker_fill == {}
    assert not run.markouts.due


def test_replay_event_path_collects_actual_synthetic_engine_fills():
    run = check.Run(*check.RUNS[0], [P], 60 * check.SECOND, 300 * check.SECOND)
    run.on_event(
        BookUpdate(
            "gemini",
            P,
            True,
            ((D("99"), D("10")),),
            ((D("101"), D("10")),),
            (1, 1),
            False,
            check.SECOND,
            check.SECOND,
        )
    )
    run.on_event(
        Trade(
            "gemini",
            P,
            "synthetic-print",
            D("98"),
            D("1"),
            "sell",
            2 * check.SECOND,
            2 * check.SECOND,
        )
    )
    assert run.first_maker_fill
    assert any(run.markouts.due.values())
    assert run.seen_seq == run.writer.event_sequence
