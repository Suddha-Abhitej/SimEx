# ADR 0020: Evaluator marks and statistical conclusions

- Status: Accepted
- Date: 2026-11-05

## Context

The ledger stores fill prices, quantities and fees, but not a price for every instant at which an
account is evaluated. Equity curves, drawdown and return statistics therefore need a separate,
venue-qualified mark series. A short run can also produce a point estimate that is not evidence of
skill, especially because repeated trades share the same book.

## Decision

The evaluator uses only Gemini level-2 recordings from the private recorder Parquet as its mark
source. It replays the recordings through the existing book and records exact Decimal midpoints.
A mark lookup uses the latest mark at or before the requested period boundary, provided it is no
older than one evaluation period; it never interpolates. If a held asset has no such mark, that
equity point is missing and the return series is incomplete. Fill-only cash, position, fee and
turnover totals remain exact, but Sharpe and significance are declined when coverage is incomplete.

Returns use explicitly configured fixed periods (60 seconds by default). Agent/baseline
comparisons report the paired sample size and a two-sided 95% moving-block bootstrap confidence
interval for mean excess return. Decimal returns are converted to float only at the statistical
boundary. Blocks preserve local serial correlation; independent-period bootstrap assumptions are
not made. Thirty one-minute observations are enough to compute an interval, but not enough to
conclude: the default conclusion threshold is 300 periods. The report says when data are
computable but insufficient to conclude and never turns a comparison into a ranking.

## Amendment, 2026-11-05: Sharpe does not separate agents on one product

The first full evaluation (14 hours, 840 one-minute periods, four baselines on BTC-USD) returned
Sharpe ratios of -65.3, -67.3, -67.4 and -71.3 for agents whose exposure differed by a factor of
sixteen. That is arithmetic, not a defect. Equity is `cash + position * price`, so scaling the
position scales both the mean and the standard deviation of the period return by the same factor
and the ratio cancels. Two accounts holding the same asset in the same window therefore produce
almost the same Sharpe whatever size they hold, and the number describes the asset's path rather
than the agent.

The magnitude is equally uninformative: annualising from 60-second periods over a fraction of a
day multiplies a near-deterministic drift by a large constant.

Sharpe stays in the report, because it is comparable for one account across runs and across
accounts holding genuinely different portfolios. It is not evidence about relative skill on a
single product, and the report must not be read that way. Paired excess return with its bootstrap
interval remains the only comparison between accounts.

The same run showed why that comparison still is not a ranking. Losses ordered exactly as
exposure did (turnover ratios 8.0%, 7.0%, 3.0%, 0.5%; losses 0.49%, 0.34%, 0.08%, 0.03%) because
the market fell throughout. The intervals excluded zero because excess return over one price path
is nearly deterministic, not because skill was measured. ADR 0003 already requires live forward
testing over many sessions; this is the measurement that shows why.

## Consequences

The evaluator is honest about missing historical coverage and cannot score an account from fills
alone. Private recordings remain local and Coinbase data can never enter scoring. The block length
and conclusion threshold are explicit configuration, so a future measurement can change them
without silently changing the meaning of an old report.
