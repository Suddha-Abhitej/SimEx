# 0027. Live-order recovery and durable historical idempotency

- **Status:** Accepted
- **Date:** 2026-11-09
- **Amends:** [0017](0017-event-sourced-ledger.md), for recovery scope and its timeout only.

## Context

The [restore measurement](../restore-cost.md) found a startup availability cliff: three default
500 ms attempts passed at 25k lifetime orders, one failed at 50k, and all failed at 75k. These are
x86 Linux devcontainer results, not Oracle ARM64 limits. Loading every full order also hit the
2 GiB worker RSS cap at 1M orders. Increasing the timeout alone leaves the memory wall intact.

The account/client key names an order forever (ADR 0014). Returning a historical order requires
the original request, including its rationale, for exact equality, and its current full snapshot
for the response. Retaining only its ID/status cannot satisfy that contract. Keeping every key
in a compact membership structure reduces bytes but still grows with lifetime history, requires
an all-key startup scan, and adds new update invariants to avoid a read not yet measured.

## Decision

With a durable ledger, recover only nonterminal orders, reservations, exact balances and the
latest account posting sequence. Use the partial `orders_live_account_idx` index; its literal
predicate is checked against Python's terminal statuses, and an `EXPLAIN` test proves that the
actual recovery query uses the index over a large synthetic terminal history. Recovery reads one
repeatable-read, read-only snapshot. Compute sums and per-asset maximum sequences in one posting
fold, then take the maximum across assets for the account sequence.

Keep live orders complete, including pending cancellations, fill and fee aggregates and accepted
market bands. Queue recovery, matching, settlement, fill timing and reservation arithmetic stay
as defined in 0014/0017. Retire a terminal order and its key only after successful final snapshot
persistence. A current synchronous engine caller still holds its local `Order` reference. Do not
hydrate terminal bands into runtime state. The event journal remains separately bounded by 0023.

Resolve a nonresident account/client key through the existing unique ledger index **before
admission policy**. Compare the original `OrderRequest` using its existing equality, including
reasoning and numeric Decimal equality. Return the current original order on equality; otherwise
raise the existing `idempotency_key_reused`. Never recompute historical refusals from current
limits. Keep the existing legacy refusal-detail sanitation when hydrating old rows.

An insert-first/handle-unique-conflict design is wrong: it runs today's risk, funding and gate
checks and may take a reservation before discovering that the order already exists. A request
accepted under earlier limits must replay as that order without being readmitted. Unwinding a
reservation on collision introduces a rarely exercised balance-corruption path. The blocking
pre-admission read is therefore a correctness cost, not an optional optimisation.

No terminal cache or historical key map is retained in durable mode: recovery maps retain zero
incremental bytes per historical terminal order. A historical response temporarily needs one
full snapshot. Active orders, database buffers, the bounded event journal and caller-held
responses still consume memory; this is not a total process-memory cap. Non-durable development
mode retains its existing historical objects and keys, and remains lifetime-memory-growing.

Tools share the writer's resolved lookup only within one synchronous submission, consumed once
at writer entry. It cannot be reused as a negative cache, and callers cannot declare a key fresh.
Direct writer submissions perform the same authoritative lookup. A failed or timed-out lookup
never means absence and never starts admission. Existing ledger-failure reasons and exception
causes remain available. No second writer, shared cache, background mutation or API field is added.

Only account recovery gets a **five-second per-account timeout**, using the existing startup
allowance as an operational budget. Ordinary reads/writes retain 500 ms; fill retries and
cancel-and-settle behaviour are unchanged. Recovery has no retry and accepts no traffic until
complete. Multiple accounts can lengthen a runtime rebuild. The original 500 ms recovery mode
remains an explicit measurement so a timeout increase cannot be presented as a query speedup.

## Consequences

New submissions add one blocking indexed read, while live retries use resident state. Terminal
replays hydrate the original row without new admission writes. Measure that read and the partial
index's write overhead against the earlier 44–47 durable accepted orders/s and commit timings;
do not assume a read costs the same as the measured 6.969 ms commit median. A future compact
membership design needs its own evidence if this throughput cost proves unacceptable.

The additive index migration changes no columns or audit rows. It scans history once. If an
existing ledger cannot build it within the migration runner's five-second allowance, the owner
must prebuild the same index with the writer stopped, then start normally. The transactional
`IF NOT EXISTS` migration records its version. No blanket timeout increase or concurrent-index
DDL is smuggled into that runner. See the [upgrade instructions](../restore-cost.md#upgrade).

The posting fold remains linear in account postings, and a large live set still takes time and
memory. **Recovery is not constant-time:** the posting fold can become the next startup cliff.
No audit history is purged, archived or rewritten, and no balance checkpoint is introduced here.

Success is specified before measurement: all three default-mode attempts should recover **1M
lifetime orders with 16 live orders within the five-second load budget**. If the posting fold
binds below that point while order response/construction and retained maps stay bounded, publish
the new measured limit, close #107 on its order-map scope, and file a successor for the posting
fold with the evidence attached. Do not raise the budget to obtain a passing table. Report
smaller-than-hoped improvements and throughput regressions explicitly.

Re-run the baseline points (1k/10k/25k/50k/75k/100k/1M), three fresh processes each, with separate
500 ms compatibility, five-second default and 120-second diagnostic modes. Include the 100k
unfilled control, untimed response-byte pass, individual query timings, retained/peak RSS and
one-time migration timing. Keep the disposable-database safeguards and publish all failed or
censored runs. No Oracle capacity claim follows from an x86 run.
