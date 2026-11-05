# 0024. Private synchronous Python client and acknowledged stream watermark

- **Status:** Accepted; amended during review to prune contiguous applied IDs and clarify disconnects.
- **Date:** 2026-11-05

## Context

Issue #16 makes connecting a Python bot to the private API a short script. Duplicating server
policy would drift from the shared boundary. Floats at this first user-facing Python boundary
would undermine ADR 0004. Retrying with a new order key can create a second trade, and treating a
bounded notification stream as complete history corrupts application accounting.

The owner approved the proposal with live-by-default subscriptions, freely delivered frames with
an acknowledgement watermark, a minimum-size market-buy demo and unknown-frame tolerance.

## Decision

Add `simex.client`, installed from the checkout, without publishing a package (ADR 0010). Promote
httpx to runtime and use `websockets.sync.client`. Choose sync: the exchange serializes this
single-account API on one event loop with a synchronous ledger, so an async bot entry point adds
complexity without gaining server-side request concurrency. There is no async client, CLI, account
selector, order validation, risk implementation or reason taxonomy.

Accept monetary inputs as Decimal or strings only. Reject floats before sending with the field
name and instruction to use `Decimal("0.1")` or `"0.1"`. Serialize Decimal directly to strings;
pass strings unchanged for server validation without quantization. Decode known money fields
structurally to Decimal while preserving nulls, text IDs/reasoning, integer sequence/timestamp
metadata and unknown fields. JSON fractional numbers decode directly to Decimal, never via float.
`APIError` preserves the exact reason, status, complete decoded body and raw bytes. New reasons
flow through old clients unchanged.

Require a caller-persisted `client_order_id`. Serialize once, reusing identical bytes/key for a
configurable bounded retry of connection errors/timeouts. One immediate retry is a provisional
operational default, not a measured market constant; zero disables it. Never retry an HTTP answer.
An exhausted timeout remains uncertain and is resolved using the original key. Do not retry DELETE
automatically or turn delayed cancellation `requested` into `cancelled`.

### Stream contract

A first subscription with no saved checkpoint starts live at hello. Replay from zero is explicit
and gaps if the beginning was evicted. Save the announced peer ID and hello immediately; a live
initial boundary intentionally uses hello's head, but a replay never advances to that head. Keep
the announced ID separate from the saved ID so a new writer's hello/gap cannot relabel an old cursor.

Deliver frames freely, with `ack(frame)` only after application. Advance a safe watermark through
contiguous acknowledged deliveries: acking 5 and 7 without 6 leaves the cursor at 5. Preserve applied
fill IDs beyond that watermark, so reconnect replays 6 while deduplicating 7. Suppress duplicate
in-flight IDs too. Ordered delivery and scanned heartbeat sequences establish safe intervening
holes from filtered/non-fill events, but cannot jump an unapplied fill. This follows ADR 0015's
safe-progress watermark idea. Resume sends both saved sequence and writer ID.

Checkpoint state belongs to the caller and is restorable, including applied IDs. Crash-safe
application requires atomic persistence with application state, or idempotent application by fill
ID; this SDK does not create a database or guarantee transactions over an external store. Use one
active iterator per checkpoint on one thread. Prune an applied fill ID as the contiguous watermark
passes its sequence: replay is strictly after the cursor, so only out-of-order acknowledgements
beyond it need IDs. Restored applied fills join the sequence queue when replayed so they are also
pruned after earlier holes close. Outstanding tracking grows with unacknowledged deliveries.

Hello flags, heartbeat and halt/recovery frames remain visible. Unknown frame types pass through
unchanged, with JSON numeric fractions still Decimal. Their own sequence fields do not advance
the checkpoint because their semantics are unknown. Applications may ignore them.

Every gap raises `StreamGap` with the original reason/full frame and closes the iterator, preserving
acknowledged progress. Never reset silently to live. All disconnections propagate, including
`ConnectionClosedOK` on a clean server close, so a bot cannot silently stop seeing fills. The example
catches `ConnectionClosed`, reports the reason and exits nonzero without a traceback; it does not
undo an accepted order. There is no reconnect or backoff loop. `reconcile()` reads up to 1,000 fills
and the portfolio in two separate non-atomic
requests. It proves no coverage, acknowledges nothing and resets/restarts nothing. Missing history
can require an explicit portfolio baseline or offline ledger reconciliation. Fills can occur between
those reads and a new subscription; there is no atomic snapshot-and-stream handoff.

### Demonstration and validation

The quickstart documents required API/ledger settings and all per-product account caps. The short
example connects before a minimum-size BTC market buy and prints the first fill, fee and portfolio.
It is a connection demonstration, not a strategy. State the deliberate 120 bps taker cost (ADR 0014).
Orders remain bounded IOC orders; partial fills and expiries are possible. Document a resting-limit
variant with no promised fill time. Point to baselines/evaluation and the single-session exposure
versus skill caveat.

Tests use the real FastAPI app over local HTTP/sync WebSockets, synthetic fixtures and committed
Postgres fills. Verify Decimal precision, float rejection before requests, stable-key retries after
lost accepted responses, refusal/cancel non-retries, all four gaps, replay below hello, batch ack
holes, restored deduplication, halt and unknown-frame delivery. Disable safeguards to prove tests
fail. Full Postgres-required validation uses `simex_test`, separate from the live ledger.

## Consequences

Bots use Decimal arithmetic by default and share the authoritative server policy. Unknown reasons
and frames do not require an SDK taxonomy release. Explicit acknowledgements support batching,
with application-owned durable state and deduplication storage. Gaps require a visible recovery
decision; bounded history cannot imply full recovery. Matching, gate, timing, settlement, evaluator
conventions and hard rules remain unchanged.
