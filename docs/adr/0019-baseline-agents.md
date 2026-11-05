# 0019. Multi-account baseline agents

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Baseline comparisons are invalid if agents use different order-entry paths or share an account.
The live runtime also needs deterministic, private reference agents that can be compared with MCP
agents while preserving the exchange's one-writer boundary and exact ledger reconciliation.

## Decision

Configure baseline agents with `EXCHANGE_BASELINE_AGENTS`, a comma-separated list of
`kind:account[:seed]` entries. Each agent has its own account, is seeded with
`seed:{account_id}:USD:v1`, and is registered separately with reconciliation. The seed is
idempotent per account after restore. Risk limits are one shared configuration applied identically
to every account.

All baselines use the same `ExchangeTools` interface as MCP clients, so orders pass through the
same gate, anti-cheat delay, market band, reservation, idempotency, and engine path. The rules are:

- buy-and-hold buys once at the current best ask;
- DCA buys `EXCHANGE_AGENT_DCA_QUOTE_AMOUNT` at each cadence;
- random uses its explicit configured seed and chooses a side and bounded size from its seeded
  PRNG;
- momentum buys when the mid rises and depth imbalance is at least 0.25, and sells when the mid
  falls and imbalance is at most -0.25.

Momentum uses only the instantaneous `features` values and is a naive reference, not a tuned
strategy. Every order reasoning string mechanically names the rule and condition that fired.
Agent tasks are rebuilt with the runtime; an agent exception increments
`simex_exchange_agent_errors_total`, is logged, and stops only that agent.

## Consequences

Several accounts make reconciliation and startup state explicit, and the same risk limits apply
to every baseline. Agent failures do not stop the exchange or other agents, but a failed agent
must be restarted with the supervised runtime group. Seed and rule metadata remain private.
