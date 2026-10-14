# 0004. `Decimal` for all money

- **Status:** Accepted
- **Date:** 2026-10-13

## Context

Binary floating point cannot represent most decimal fractions exactly (`0.1 + 0.2 != 0.3`). A ledger
built on floats drifts by fractions of a cent over many fills and fails reconciliation.

## Decision

Every price, quantity, fee and balance uses Python's `Decimal`, quantized to the venue's increments.
Floats are allowed only in analytics outputs (Sharpe ratio, charts), never in the ledger or matching
engine. Ledger invariants are checked with Hypothesis property-based tests: balances only change
through recorded fills and fees, every fill is counted exactly once, and money is never created or
destroyed.

## Consequences

- Balances reconcile exactly, to the smallest increment.
- `Decimal` is slower than float; acceptable at three pairs and LLM-speed agents.
- Serialization must carry decimals as strings, never JSON numbers.
