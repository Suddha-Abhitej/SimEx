# 0017. Event-sourced ledger and durable fills

- **Status:** Proposed
- **Date:** 2026-10-19
- **Amends:** [0014](0014-order-model-and-fill-model.md) for the explicit `_external`
  counterparty.

## Context

The exchange engine can restart and market recordings can be replayed. In-memory balances and
fill history therefore cannot be authoritative. The ledger must preserve exact Decimal amounts,
make a repeated fill harmless, and leave enough durable history to rebuild balances.

## Decision

Use numbered plain-SQL migrations applied by an asyncpg runner and store money in
`NUMERIC(38,18)`. Decimal strings remain the API and event-wire format. Postgres is authoritative;
the account writer remains the single per-account mutation boundary, and pair engines still only
report fills.

`fills.fill_id` is both the primary key and idempotency key: `{order_id}-{fill_ordinal}`. A replay
uses `ON CONFLICT` semantics with a comparison of domain fields only:
`fill_id`, `order_id`, `account_id`, `product_id`, `side`, `price`, `quantity`, `liquidity`, `cause`,
`fee`, `fee_asset`, and `ts_ns`. Identical payloads are no-ops; a different payload is a hard
integrity error and is never overwritten. Insertion metadata such as `created_at` is excluded.

One transaction inserts the fill, balanced postings, append-only event, and outbox row. Outbox
rows are written now, but no publisher is built until #13/#14. Without an outbox, a crash between
the database commit and Kafka publish loses a notification; publishing first can expose a fill
that rolls back.

A fill is a fact, not a request. The proposed p99 commit budget is 50 ms, with measurement around
transaction begin, each write boundary, commit, and writer acknowledgement. A timeout never drops
the fill: retry with backoff, then halt market-event processing and new order entry, alert, and
require intervention.

The account writer persists order snapshots and reservations at the same mutation boundary as its
in-memory `Accounts` projection. On restart it loads orders, reservations, and posting sums from
Postgres before accepting new entry. The synchronous engine uses a dedicated asyncpg event-loop
thread so a committed result is available before the in-memory mutation is acknowledged; the
pair engine remains the sole owner of matching. A restarted resting order has no queue checkpoint:
the pair engine places it at the back of the venue queue on the first synchronized book, with
`ahead = 0` only when it improves the current best, and discards `crossed` and depletion state.

Version 1 does not persist `ahead`, `crossed`, or the depletion map. Every restart restores a
resting order at the back of the queue at the first synchronized book, or `ahead = 0` if it
improves the current best. `crossed` resets to false and depletion state is discarded. Queue
checkpointing is a possible optimization only if measured restart frequency justifies its
hot-path writes.

Reconciliation compares the balance projection with SQL sums of postings using exact Decimal
equality and no tolerance. A mismatch fails closed, alerts, preserves history, and requires
intervention; any manual override is durable, audited, scoped, expiring, and does not edit history.

This ADR moves to **Accepted** when the three approved ledger PRs are reviewed and merged:
the schema/adapter foundation, account-writer and restart recovery integration, and the
projection/reconciliation job. Until then, it remains Proposed so the complete ledger contract
can still be reviewed without implying that the later projection and recovery pieces exist.

## Consequences

The ledger survives process restart and replay without duplicate fills and supports SQL
reconciliation. It costs synchronous database latency on the fill path and requires explicit
halt/recovery operations when Postgres or the ledger becomes unavailable.
