"""The fill replay check's markout test (#8, ADR 0014 §B), on hand-made markouts."""

import importlib.util
from decimal import Decimal as D
from pathlib import Path

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
