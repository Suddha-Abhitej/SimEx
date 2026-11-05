"""Private, Decimal-first evaluation of simulated accounts."""

from __future__ import annotations

import random
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

NANOSECONDS_PER_SECOND = 1_000_000_000
SECONDS_PER_YEAR = Decimal(365 * 24 * 60 * 60)


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        raise TypeError("evaluator inputs must not contain floats")
    return Decimal(str(value))


@dataclass(frozen=True)
class FillRecord:
    account_id: str
    product_id: str
    side: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    fee_asset: str
    ts_ns: int

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> FillRecord:
        return cls(
            account_id=str(row["account_id"]),
            product_id=str(row["product_id"]),
            side=str(row["side"]).lower(),
            price=_decimal(row["price"]),
            quantity=_decimal(row["quantity"]),
            fee=_decimal(row["fee"]),
            fee_asset=str(row["fee_asset"]),
            ts_ns=int(row["ts_ns"]),
        )


@dataclass(frozen=True)
class PriceMark:
    product_id: str
    ts_ns: int
    price: Decimal
    venue: str = "gemini"


class PriceSeries:
    """Gemini marks with last-observation-at-or-before lookup.

    Marks are never interpolated. A missing lookup is reported by the evaluator and makes
    return-series statistics ineligible.
    """

    def __init__(self, marks: list[PriceMark] | tuple[PriceMark, ...]) -> None:
        by_product: dict[str, list[PriceMark]] = defaultdict(list)
        for mark in marks:
            if mark.venue != "gemini":
                raise ValueError("only Gemini marks may feed scoring")
            by_product[mark.product_id].append(mark)
        self._marks = {
            product: tuple(sorted(values, key=lambda mark: mark.ts_ns))
            for product, values in by_product.items()
        }

    def mark_at(
        self, product_id: str, ts_ns: int, *, max_age_ns: int | None = None
    ) -> Decimal | None:
        result: Decimal | None = None
        result_ts: int | None = None
        for mark in self._marks.get(product_id, ()):
            if mark.ts_ns > ts_ns:
                break
            result = mark.price
            result_ts = mark.ts_ns
        if result is None or (
            max_age_ns is not None and result_ts is not None and ts_ns - result_ts > max_age_ns
        ):
            return None
        return result


@dataclass(frozen=True)
class EquityPoint:
    ts_ns: int
    equity: Decimal


@dataclass(frozen=True)
class Comparison:
    baseline: str
    sample_size: int
    estimate: Decimal | None
    confidence_interval: tuple[Decimal, Decimal] | None
    conclusion: str
    test: str
    assumptions: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline": self.baseline,
            "sample_size": self.sample_size,
            "estimate": None if self.estimate is None else str(self.estimate),
            "confidence_interval": (
                None
                if self.confidence_interval is None
                else [str(value) for value in self.confidence_interval]
            ),
            "conclusion": self.conclusion,
            "test": self.test,
            "assumptions": self.assumptions,
        }


@dataclass(frozen=True)
class AccountMetrics:
    account_id: str
    period_seconds: int
    initial_equity: Decimal | None
    final_equity: Decimal | None
    return_value: Decimal | None
    sharpe: Decimal | None
    max_drawdown: Decimal | None
    turnover: Decimal
    turnover_ratio: Decimal | None
    fees: Decimal
    fee_drag: Decimal | None
    equity_curve: tuple[EquityPoint, ...]
    returns: tuple[Decimal, ...]
    missing_marks: tuple[tuple[int, str], ...]
    computable: bool

    def to_dict(self) -> dict[str, Any]:
        decimal_fields = {
            "initial_equity": self.initial_equity,
            "final_equity": self.final_equity,
            "return": self.return_value,
            "sharpe": self.sharpe,
            "max_drawdown": self.max_drawdown,
            "turnover": self.turnover,
            "turnover_ratio": self.turnover_ratio,
            "fees": self.fees,
            "fee_drag": self.fee_drag,
        }
        return {
            "account_id": self.account_id,
            "period_seconds": self.period_seconds,
            **{key: None if value is None else str(value) for key, value in decimal_fields.items()},
            "equity_curve": [
                {"ts_ns": point.ts_ns, "equity": str(point.equity)} for point in self.equity_curve
            ],
            "returns": [str(value) for value in self.returns],
            "missing_marks": [
                {"ts_ns": ts_ns, "asset": asset} for ts_ns, asset in self.missing_marks
            ],
            "computable": self.computable,
        }


@dataclass(frozen=True)
class EvaluationResult:
    account: AccountMetrics
    comparisons: tuple[Comparison, ...]
    report: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "account": self.account.to_dict(),
            "comparisons": [comparison.to_dict() for comparison in self.comparisons],
            "report": self.report,
        }


@dataclass(frozen=True)
class EvaluationConfig:
    period_seconds: int = 60
    minimum_compute_samples: int = 30
    minimum_conclusion_samples: int = 300
    max_mark_age_periods: int = 1
    bootstrap_replicates: int = 2_000
    block_size: int = 5
    seed: int = 21

    def __post_init__(self) -> None:
        if (
            self.period_seconds <= 0
            or self.minimum_compute_samples < 2
            or self.max_mark_age_periods < 0
        ):
            raise ValueError("period and minimum sample settings must be positive")
        if self.minimum_conclusion_samples < self.minimum_compute_samples:
            raise ValueError("conclusion sample minimum cannot be below compute minimum")
        if self.bootstrap_replicates < 100 or self.block_size < 1:
            raise ValueError("bootstrap settings are too small")


def _asset_for_product(product_id: str) -> tuple[str, str]:
    if "-" not in product_id:
        raise ValueError(f"product must be normalized as BASE-QUOTE: {product_id!r}")
    base, quote = product_id.split("-", 1)
    return base, quote


def _apply_fill(
    balances: dict[str, Decimal], fill: FillRecord, turnover: list[Decimal], fees: list[Decimal]
) -> None:
    base, quote = _asset_for_product(fill.product_id)
    notional = fill.price * fill.quantity
    turnover[0] += notional
    fees[0] += fill.fee
    balances.setdefault(base, Decimal(0))
    balances.setdefault(quote, Decimal(0))
    if fill.side == "buy":
        balances[base] += fill.quantity
        balances[quote] -= notional
    elif fill.side == "sell":
        balances[base] -= fill.quantity
        balances[quote] += notional
    else:
        raise ValueError(f"unknown fill side {fill.side!r}")
    balances[fill.fee_asset] = balances.get(fill.fee_asset, Decimal(0)) - fill.fee


def _equity(
    balances: Mapping[str, Decimal],
    prices: PriceSeries,
    ts_ns: int,
    max_age_ns: int,
) -> Decimal | None:
    total = balances.get("USD", Decimal(0))
    for asset, amount in balances.items():
        if asset == "USD" or amount == 0:
            continue
        price = prices.mark_at(f"{asset}-USD", ts_ns, max_age_ns=max_age_ns)
        if price is None:
            return None
        total += amount * price
    return total


def _metrics(
    account_id: str,
    fills: list[FillRecord],
    prices: PriceSeries,
    initial_balances: Mapping[str, Decimal],
    start_ns: int,
    end_ns: int,
    config: EvaluationConfig,
) -> AccountMetrics:
    fills = sorted(
        (fill for fill in fills if start_ns <= fill.ts_ns < end_ns),
        key=lambda f: f.ts_ns,
    )
    balances = {asset: _decimal(amount) for asset, amount in initial_balances.items()}
    cursor = 0
    turnover = [Decimal(0)]
    fees = [Decimal(0)]
    curve: list[EquityPoint] = []
    missing: list[tuple[int, str]] = []
    periods = range(start_ns, end_ns, config.period_seconds * NANOSECONDS_PER_SECOND)
    for ts_ns in periods:
        while cursor < len(fills) and fills[cursor].ts_ns <= ts_ns:
            _apply_fill(balances, fills[cursor], turnover, fees)
            cursor += 1
        value = _equity(
            balances,
            prices,
            ts_ns,
            config.period_seconds * NANOSECONDS_PER_SECOND * config.max_mark_age_periods,
        )
        if value is None:
            for asset, amount in balances.items():
                if (
                    asset != "USD"
                    and amount != 0
                    and prices.mark_at(
                        f"{asset}-USD",
                        ts_ns,
                        max_age_ns=(
                            config.period_seconds
                            * NANOSECONDS_PER_SECOND
                            * config.max_mark_age_periods
                        ),
                    )
                    is None
                ):
                    missing.append((ts_ns, asset))
        else:
            curve.append(EquityPoint(ts_ns, value))
    initial = curve[0].equity if curve else None
    final = curve[-1].equity if curve else None
    returns = (
        tuple(
            (current.equity - previous.equity) / previous.equity
            for previous, current in zip(curve, curve[1:], strict=False)
            if previous.equity != 0
        )
        if not missing
        else ()
    )
    return_value = (
        None
        if missing or initial in (None, Decimal(0)) or final is None
        else (final - initial) / initial
    )
    peak: Decimal | None = None
    drawdown = Decimal(0)
    for point in curve:
        peak = point.equity if peak is None or point.equity > peak else peak
        if peak:
            drawdown = max(drawdown, (peak - point.equity) / peak)
    sharpe: Decimal | None = None
    if len(returns) >= 2:
        mean = sum(returns, Decimal(0)) / Decimal(len(returns))
        variance = sum((item - mean) ** 2 for item in returns) / Decimal(len(returns) - 1)
        if variance:
            sharpe = (
                mean / variance.sqrt() * (SECONDS_PER_YEAR / Decimal(config.period_seconds)).sqrt()
            )
    return AccountMetrics(
        account_id,
        config.period_seconds,
        initial,
        final,
        return_value,
        sharpe,
        drawdown if curve else None,
        turnover[0],
        None if initial in (None, Decimal(0)) else turnover[0] / initial,
        fees[0],
        None if initial in (None, Decimal(0)) else fees[0] / initial,
        tuple(curve),
        returns,
        tuple(missing),
        bool(curve) and not missing,
    )


def _moving_block_ci(
    values: tuple[Decimal, ...], config: EvaluationConfig
) -> tuple[Decimal, Decimal]:
    """Return a 95% moving-block bootstrap CI; float conversion is statistical-boundary only."""
    data = [float(value) for value in values]
    rng = random.Random(config.seed)
    n = len(data)
    means: list[float] = []
    for _ in range(config.bootstrap_replicates):
        sample: list[float] = []
        while len(sample) < n:
            start = rng.randrange(n)
            sample.extend(data[(start + offset) % n] for offset in range(config.block_size))
        means.append(sum(sample[:n]) / n)
    means.sort()
    low = means[int(0.025 * (len(means) - 1))]
    high = means[int(0.975 * (len(means) - 1))]
    return Decimal(str(low)), Decimal(str(high))


def _compare(
    account: AccountMetrics,
    baseline: AccountMetrics,
    name: str,
    config: EvaluationConfig,
) -> Comparison:
    if not account.computable or not baseline.computable:
        return Comparison(
            name,
            0,
            None,
            None,
            "insufficient price coverage to compute a comparison or call a winner",
            "two-sided 95% moving-block bootstrap CI for paired mean excess return",
            "A complete Gemini mark is required at every period boundary; "
            "no interpolation is used.",
        )
    sample = min(len(account.returns), len(baseline.returns))
    excess = tuple(
        agent - reference
        for agent, reference in zip(
            account.returns[:sample], baseline.returns[:sample], strict=False
        )
    )
    if sample < config.minimum_compute_samples:
        conclusion = "insufficient data to compute a significance interval or call a winner"
        interval = None
        estimate = None
    else:
        estimate = sum(excess, Decimal(0)) / Decimal(sample)
        interval = _moving_block_ci(excess, config)
        if sample < config.minimum_conclusion_samples:
            conclusion = (
                f"computed from {sample} {account.period_seconds}-second periods, but insufficient "
                "duration to conclude that either strategy is better"
            )
        elif interval[0] > 0:
            conclusion = "evidence favors the agent, but this is not a ranking"
        elif interval[1] < 0:
            conclusion = "evidence favors the baseline; the agent is not better in this comparison"
        else:
            conclusion = "insufficient evidence to call a winner"
    return Comparison(
        name,
        sample,
        estimate,
        interval,
        conclusion,
        "two-sided 95% moving-block bootstrap CI for paired mean excess return",
        "Pairs share timestamps and are serially correlated; blocks preserve local dependence. "
        "Decimal returns are converted to float only inside the bootstrap routine.",
    )


def render_report(result: EvaluationResult) -> str:
    account = result.account
    lines = [
        f"Account: {account.account_id}",
        f"Period length: {account.period_seconds} seconds",
        f"Initial equity: {account.initial_equity}",
        f"Final equity: {account.final_equity}",
        f"Return: {account.return_value}",
        f"Sharpe: {account.sharpe}",
        f"Max drawdown: {account.max_drawdown}",
        f"Turnover: {account.turnover} (ratio {account.turnover_ratio})",
        f"Fee drag: {account.fees} (ratio {account.fee_drag})",
        f"Price coverage: {'complete' if not account.missing_marks else 'incomplete'}",
    ]
    for comparison in result.comparisons:
        lines.extend(
            [
                f"Comparison with {comparison.baseline}: n={comparison.sample_size}",
                f"  Mean excess return: {comparison.estimate}",
                f"  95% CI: {comparison.confidence_interval}",
                f"  Conclusion: {comparison.conclusion}",
                f"  Test: {comparison.test}",
                f"  Assumptions: {comparison.assumptions}",
            ]
        )
    return "\n".join(lines)


def evaluate(
    account_id: str,
    fills: list[FillRecord] | tuple[FillRecord, ...],
    prices: PriceSeries,
    initial_balances: Mapping[str, Decimal],
    *,
    start_ns: int,
    end_ns: int,
    baselines: Mapping[str, tuple[list[FillRecord] | tuple[FillRecord, ...], Mapping[str, Decimal]]]
    | None = None,
    config: EvaluationConfig | None = None,
) -> EvaluationResult:
    config = config or EvaluationConfig()
    account = _metrics(account_id, list(fills), prices, initial_balances, start_ns, end_ns, config)
    comparisons = tuple(
        _compare(
            account,
            _metrics(name, list(baseline_fills), prices, balances, start_ns, end_ns, config),
            name,
            config,
        )
        for name, (baseline_fills, balances) in (baselines or {}).items()
    )
    result = EvaluationResult(account, comparisons, "")
    return EvaluationResult(account, comparisons, render_report(result))


__all__ = [
    "AccountMetrics",
    "Comparison",
    "EquityPoint",
    "EvaluationConfig",
    "EvaluationResult",
    "FillRecord",
    "PriceMark",
    "PriceSeries",
    "evaluate",
    "render_report",
]
