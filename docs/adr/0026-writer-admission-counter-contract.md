# 0026. Writer admission counters and failure-contained snapshots

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

The journal-derived `simex_exchange_rejections_total` counts arrival and effect rejections,
can miss evicted events, and has no admission denominator (#111). An order accepted at arrival
can fail the gate at effect (ADR 0016); counting both phases against new admissions would
misstate the refusal rate. A transport retry returning the same order is not a new admission
(ADR 0014). The ledger acknowledgement boundary matters (ADR 0017): a persistence exception
cannot be labelled success or refusal, even when the commit might already have happened.

Telemetry must never change trading. With exception containment, independently incrementing
submitted and accepted/rejected counters can lose one increment under partial failure and
**permanently corrupt** `submitted = accepted + arrival-rejected`. This permanent divergence,
rather than a transient scrape race, is the reason for the snapshot collector. A scrape race
alone is usually immaterial to a dashboard computing rates over minutes.

## Decision

Count completed new admissions at `AccountWriter.submit`, after the existing order persistence
and notification path succeeds. Acceptance describes arrival, not current state or a promise
of a fill. Export these counter families:

| Metric (`simex_exchange_` prefix) | Labels | Increment |
| --- | --- | --- |
| `orders_submitted_total` | product_id | Derived: accepted + arrival-rejected |
| `orders_accepted_total` | product_id | New accepted order |
| `orders_arrival_rejected_total` | product_id, reason | New recorded REJECTED order |
| `orders_effect_rejected_total` | product_id, reason | Successful later `reject_on_effect` |
| `orders_replayed_total` | product_id | Existing key/same request, in any status |
| `order_submit_errors_total` | product_id, reason | Submit raises, no completed admission |

A malformed/reused-key `OrderError`, pre-existing halt, persistence failure or unexpected
exception counts only as a submit error; the original exception is preserved. Effect failure
does not change arrival counts. Cancels, deposits and fills are not admissions. Restore does
not backfill counters, and a restored order returned for the same request counts as a replay.
The registry owns the observer across Kafka/runtime rebuilds; only process restart resets it.

One single-writer observer publishes an immutable bounded count snapshot per outcome. A custom
Prometheus collector captures that snapshot once for all six families, deriving submitted at
export. There is no separate submitted increment to lose. Readers hold no lock on trading,
and the observer retains counts, not orders or events. This relies on the existing single
writer execution boundary, not concurrent submissions from threads.

The optional observer is called through an exception-contained writer helper. Normalization,
publication or export failure disables the new families: subsequent scrapes omit them rather
than report apparently healthy frozen counts. A once-only best-effort WARNING contains no
payload or exception text; logging failure is contained too. Scrape construction finishes
before any family is returned, so an export error returns no partial set. No observer is added
to `record_fill` or settlement. Matching, timing and ledger policies are unchanged.

Product labels are writer instrument IDs plus fixed `unknown`. Reasons are an explicit finite
allowlist of server-owned prefixes (before `:`), with fixed `other` for unknown reasons and
fixed halt/persistence/internal-error categories. Failed parsing with no resolved product uses
`unknown`; the metric does not reparse inputs. Account/client/order/fill IDs, arbitrary input,
detail, exception text, market data, amounts, tokens and DSNs are never labels (ADR 0010).

Coverage is the writer boundary only. Request-validation refusals returned earlier by
`ExchangeTools` are visible only to the client that made them, as in `simex.report`. A direct
writer caller whose check produces a recorded rejection does count. The boundary, not the
reason string alone, determines coverage.

## Consequences

Arrival-refusal fraction is arrival-rejected/submitted; acceptance fraction is accepted/submitted.
A zero denominator is unavailable, not zero refusals. Show effect rejections separately: they
can refer to admissions in an earlier window or process. Do not divide the legacy mixed-phase,
lossy rejection counter by submitted, subtract arrivals from it to estimate effects, or combine
baseline-only refusal counts with the writer denominator. The legacy metric and eviction-gap
signal remain for compatibility.

Disabling telemetry loses new observations until a process restart but preserves trading and
the exact identity of every exported snapshot. Monitor absence as unavailable. Snapshot copy
cost is bounded by configured products and the fixed reason set, not lifetime order count.
Dashboards can adopt these counters in a separate PR; continuous causal tick-to-fill latency
remains a separate task ([#121](https://github.com/Suddha-Abhitej/SimEx/issues/121)).
