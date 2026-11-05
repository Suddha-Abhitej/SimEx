from decimal import Decimal as D

from simex.evaluator import (
    EvaluationConfig,
    FillRecord,
    PriceMark,
    PriceSeries,
    evaluate,
)

NS = 1_000_000_000


def fill(account, side, price, quantity, fee, ts, product="BTC-USD"):
    return FillRecord(account, product, side, D(price), D(quantity), D(fee), "USD", ts)


def prices(*values):
    return PriceSeries(
        [PriceMark("BTC-USD", index * 60 * NS, D(value)) for index, value in enumerate(values)]
    )


def test_evaluator_replays_exact_decimal_metrics():
    result = evaluate(
        "agent",
        [
            fill("agent", "buy", "100", "1", "1", 10 * NS),
            fill("agent", "sell", "120", "0.5", "0.6", 120 * NS),
        ],
        prices("100", "110", "120"),
        {"USD": D("1000")},
        start_ns=0,
        end_ns=180 * NS,
    )

    metrics = result.account
    assert [point.equity for point in metrics.equity_curve] == [D("1000"), D("1009"), D("1018.4")]
    assert metrics.return_value == D("0.0184")
    assert metrics.max_drawdown == D("0")
    assert metrics.turnover == D("160")
    assert metrics.fees == D("1.6")
    assert metrics.fee_drag == D("0.0016")
    assert metrics.computable
    assert result.to_dict()["account"]["final_equity"] == "1018.4"


def test_incomplete_marks_decline_return_statistics_without_interpolation():
    result = evaluate(
        "agent",
        [fill("agent", "buy", "100", "1", "1", 10 * NS)],
        prices("100"),
        {"USD": D("1000")},
        start_ns=0,
        end_ns=180 * NS,
    )

    assert not result.account.computable
    assert result.account.return_value is None
    assert result.account.sharpe is None
    assert "incomplete" in result.report


def test_too_few_periods_declines_to_call_a_winner():
    result = evaluate(
        "agent",
        [],
        prices("100", "100"),
        {"USD": D("1000")},
        start_ns=0,
        end_ns=120 * NS,
        baselines={"baseline": ([], {"USD": D("1000")})},
        config=EvaluationConfig(
            minimum_compute_samples=3,
            minimum_conclusion_samples=3,
            bootstrap_replicates=100,
        ),
    )

    comparison = result.comparisons[0]
    assert comparison.sample_size == 1
    assert comparison.confidence_interval is None
    assert "insufficient data" in comparison.conclusion


def test_baseline_beating_agent_is_reported_unflatteringly():
    result = evaluate(
        "agent",
        [],
        prices("100", "105", "110", "115"),
        {"USD": D("1000")},
        start_ns=0,
        end_ns=240 * NS,
        baselines={
            "buy-and-hold": (
                [fill("buy-and-hold", "buy", "100", "1", "1", 1 * NS)],
                {"USD": D("1000")},
            )
        },
        config=EvaluationConfig(
            minimum_compute_samples=2,
            minimum_conclusion_samples=2,
            bootstrap_replicates=100,
            block_size=2,
        ),
    )

    comparison = result.comparisons[0]
    assert comparison.estimate is not None and comparison.estimate < 0
    assert comparison.confidence_interval is not None
    assert "baseline" in comparison.conclusion
    assert "n=3" in result.report
