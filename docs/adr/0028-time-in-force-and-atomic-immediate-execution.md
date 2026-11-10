# 0028. Time in force and atomic immediate execution

- **Status:** Accepted
- **Date:** 2026-11-10

## Context

Issue #85 exposes GTC, IOC and FOK through the existing private order boundary. The account
writer owns balances; pair engines own matching (0014). Immediate execution must retain the
anti-cheat delay, the gate at effect, and durable idempotency (0027). Checking FOK against raw
book size would ignore depleted liquidity. Checking first but committing each level separately
would still permit a crash to leave a partial FOK in the ledger.

## Decision

`time_in_force` accepts exactly `GTC`, `IOC`, `FOK`, default GTC. Unknown values, including an
explicit null, return `bad_field` with `field`, `rule` and `detail`. The shared parser owns this
rule; REST, MCP and the SDK introduce no second policy. It is part of request identity, including
historical replay before admission checks. Migration 010 defaults old rows to GTC without
rewriting their request JSON; the full projection includes the new immutable column.

| Type and policy | Execution at effect | Unfilled remainder |
| --- | --- | --- |
| Limit GTC | Existing taker crossing, then maker queue | Rests |
| Market GTC | Existing band-bounded immediate walk | Expires |
| Limit or market IOC | Take eligible liquidity once | Cancelled, even with zero fills |
| Limit or market FOK | Full eligible quantity or zero fills | Rejected: `fok_not_fillable` |

Market GTC is a compatibility spelling, not a promise to rest. Keeping its `expired` outcome
preserves old clients' behavior; explicit IOC uses `cancelled` to express the requested policy.
They must not be unified as a cleanup. A different spelling is a different request even where
its execution is equivalent. A fully executed order is `filled` under every policy.

“Immediate” means the first execution decision at the original effective time after the existing
delay, not at HTTP admission. IOC/FOK are durably PENDING with full reservations until then.
They can recover in that state with the original effective time and accepted market band. After
that decision they are terminal and never rest. Intrinsic IOC cancellation does not wait another
cancel delay; explicit client cancellation retains the existing delay and precedence.

The pair engine makes one read-only plan, best opposite level first, using the existing limit or
accepted band and live size minus depletion. Execution applies that exact plan without another
walk. FOK requires the complete quantity before reporting any fill; otherwise the writer records
`REJECTED`, releases the reservation and retains a rule explaining the remedy. Refusal details
contain no book price, band price or available venue quantity.

For explicit IOC/FOK, the account writer stages affected balances, reservation, order and fees
with the existing Decimal settlement calculations. One transaction writes all per-fill facts,
five balanced postings per fill, events, outbox records, the terminal order snapshot and reservation
deletion. Each fill keeps its existing identity and account posting sequence. Only after commit
acknowledgement does the writer publish staged state and fill notifications; the engine then
records depletion. There is no intermediate committed partial snapshot. The existing GTC
settlement/transaction path, including legacy market execution, remains unchanged.

Retries use the identical group and fill IDs. A lost acknowledgement cannot duplicate a fill;
a rolled-back group cannot leak a partial fill. Existing retry limits, timeouts, cancellation and
halt contracts remain. An uncertain exhausted commit halts processing: recovery determines the
committed outcome, never an assumption that nothing traded. This adds an immediate-execution
transaction boundary without changing per-fill accounting, fees, matching prices or timing.

`fok_not_fillable` is an effect rejection after an arrival acceptance (0026). It does not change
arrival counts or their identity. IOC cancellation is not a refusal. HTTP POST normally already
returned acceptance before the decision; the reason's HTTP 400 is observed on a later idempotent
replay/lookup, not a retroactive response to the initial POST.

The fill stream is unchanged: FOK rejection and zero-fill IOC produce **no fill-stream event**.
To poll a terminal order through today's API, repeat the **identical** `place_order` request with
the same client ID; this resolves the original order without new admission. `open_orders` alone
cannot distinguish completion from cancellation or rejection. No new status endpoint or order
notification frame is introduced here.

## Consequences

IOC/FOK retain reservations during the delay, and only GTC limits can enter the maker queue.
Recovery still uses migration 009's live-order index and terminal-status predicate; historical
terminal orders stay out of memory and remain accessible through durable idempotency.

An explicit immediate order groups several fill writes under one hard timeout. Deep walks can
therefore reach that bound sooner than separate transactions; no timeout is increased to hide
this cost. Compare measured multi-level group latency with the existing single-fill commit,
recording hardware and workload, rather than claiming unchanged latency or Oracle capacity.
A process failure can leave either the original pending reservation or the complete terminal
execution, never a committed partial immediate execution. Existing market GTC retains its prior
per-fill crash behavior for compatibility.

New binaries can migrate previous ledgers. An older binary must not operate a ledger containing
new IOC/FOK orders: it would not understand their execution contract. No audit history is purged
and no new shared mutable state crosses pair engines.

## Measurement, 2026-11-10

[Raw aggregate results](../measurements/tif-commit-2026-11-10.json), source commit
`1836c69a6deb99822a1d37ad8ea26c8dd1a90746`. Linux x86 devcontainer under WSL2 on an AMD Ryzen 9
8945HS laptop, local PostgreSQL 16.10; **not Oracle ARM64**. No test suite ran concurrently.
Each case has 100 sequential synthetic orders after ten warm-ups, in a generated `simex_test`
schema, with no Kafka/venue input. Other local services were not stopped. The monotonic clock's
nominal resolution was 1 ns; its smallest observed step in 10,000 reads was 170 ns.

| Case | Commit samples | Transaction-through-commit median / sample p99, ms | Execution-through-ack median / sample p99, ms |
| --- | ---: | ---: | ---: |
| GTC, one level | 100 | 8.894 / 16.195 | 12.773 / 33.538 |
| GTC, three levels (separate transactions) | 300 | 8.977 / 25.717 | 38.963 / 110.706 |
| IOC, three levels plus remainder cancellation | 100 | 19.800 / 48.500 | 20.765 / 49.438 |
| FOK, three levels | 100 | 20.287 / 25.750 | 21.086 / 26.596 |

The commit boundary is the existing adapter's cumulative `commit` phase, including transaction
writes, not isolated COMMIT time. Execution timing starts after admission and advances simulated
time directly to effect: it excludes the anti-cheat wait and live hold-back, and is **not**
tick-to-fill latency or a throughput benchmark. Group commits do more work than a single fill;
the IOC sample p99 is close to the existing 50 ms budget. These small, sequential samples are
not deployment validation and do not justify changing a timeout or budget.

Reproduce with `SIMEX_TEST_POSTGRES_DSN` set to the test database and
`python tools/measure_tif_commit.py --samples 100` from the repository root. The tool refuses any
database other than `simex_test`, validates generated schema names before DDL, and drops its
schema in `finally`. Output contains system metrics only, no synthetic prices or order amounts.
