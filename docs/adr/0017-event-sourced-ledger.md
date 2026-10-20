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

The engine stays synchronous because its matching and replay APIs are synchronous and their
deterministic event ordering is already tested at that boundary. Making the engine async would
propagate an API and scheduling change through every pair engine, runner, and test for no ledger
semantic benefit. `SyncLedger` therefore owns one asyncpg connection and one private event-loop
thread. Calls are submitted with `run_coroutine_threadsafe` and are serialized by that single
connection; this deliberately makes the ledger a per-account blocking mutation boundary rather
than allowing concurrent writes to race the in-memory projection. Every call has a 500 ms hard
timeout. On timeout, the loop-thread task is cancelled and the caller waits up to the
`CANCELLATION_GRACE_SECONDS` grace period for it to settle; an unsettled operation makes the
connection unusable and raises `LedgerHalted` immediately without retrying. A settled timeout is
retried with backoff for fills because fill identity makes the write idempotent. Non-timeout
connection errors at the fill boundary also raise `LedgerHalted` with the original error as its
cause; they are never allowed to escape as a raw retryable-looking exception. The ledger never
drops a fill or blocks forever. Startup waits at most five seconds for connection and migrations.
Shutdown first stops the loop, joins the worker thread, and the worker closes the asyncpg
connection before the thread exits, so no call can outlive the connection.

The ledger exports monotonic timing histograms for intent-to-acknowledgement, transaction begin,
fill insert, postings, event insert, outbox insert, commit, and writer acknowledgement, plus
counters for timeouts, conflicts, and halts. The commit p99 budget is 50 ms; the 500 ms timeout
is a separate failure backstop. The `ExchangeLedgerCommitP99TooHigh` alert fires when the
Prometheus p99 exceeds that budget, making the two-core Oracle deployment measurable rather than
assumed.

Version 1 does not persist `ahead`, `crossed`, or the depletion map. Every restart restores a
resting order at the back of the queue at the first synchronized book, or `ahead = 0` if it
improves the current best. `crossed` resets to false and depletion state is discarded. Queue
checkpointing is a possible optimization only if measured restart frequency justifies its
hot-path writes.

Reconciliation compares the balance projection with SQL sums of postings using exact Decimal
equality and no tolerance. A mismatch fails closed, alerts, preserves history, and requires
intervention; any manual override is durable, audited, scoped, expiring, and does not edit history.

PR (c) implements this as a periodic full fold. The latest 24-hour recording window contained
532,804 rows. A PostgreSQL 16 fold benchmark over that row count completed in 170.898 ms on the
development x86 host, so the initial measured cadence is 60 seconds rather than an assumed round
number; that leaves more than 99% of each interval outside the fold on the measured host. Oracle
ARM64 performance is not verified from this Windows host and must be re-measured before relying on
the cadence in production.

An audited reconciliation override is a `ledger_events` row containing the actor, creation time,
reason, account, asset, and expiry. It never removes the mismatch finding or its alert. It only
permits the affected account to resume while the expiry has not passed; expiry is checked on every
reconciliation cycle and needs no second clearing action. No code path clears a mismatch without
writing this event.

This ADR moves to **Accepted** when the three approved ledger PRs are reviewed and merged:
the schema/adapter foundation, account-writer and restart recovery integration, and the
projection/reconciliation job. Until then, it remains Proposed so the complete ledger contract
can still be reviewed without implying that the later projection and recovery pieces exist.

## Consequences

The ledger survives process restart and replay without duplicate fills and supports SQL
reconciliation. It costs synchronous database latency on the fill path and requires explicit
halt/recovery operations when Postgres or the ledger becomes unavailable.
