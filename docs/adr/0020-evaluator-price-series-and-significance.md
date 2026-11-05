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

## Consequences

The evaluator is honest about missing historical coverage and cannot score an account from fills
alone. Private recordings remain local and Coinbase data can never enter scoring. The block length
and conclusion threshold are explicit configuration, so a future measurement can change them
without silently changing the meaning of an old report.
