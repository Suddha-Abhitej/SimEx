# 0021. Live portfolio P&L with weighted-average cost

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Portfolio balances and open orders do not tell a trader the acquisition cost of a holding or
its profit and loss. Reconstructing that on every decision is error-prone, and the public history
tool returns at most 1,000 fills. A partial sale also needs an explicit convention: average cost
and FIFO allocate different acquisition costs to realised P&L.

The owner approved the design on issue #83 before implementation. This is the live view of the
same committed trades evaluated offline under ADR 0020; the evaluator's conventions stay intact.

## Decision

Use **moving weighted-average cost**, exclusive of fees. This provides one pooled acquisition
cost per holding for live strategy decisions without selecting purchase lots. FIFO is a valid
alternative, but choosing it would change the realised/unrealised split. This is a research
accounting convention, not a tax calculation.

For remaining quantity Q and acquisition cost C, a buy adds its quantity and notional. A sale of
q removes `C * q / Q` from the pool and realises sale proceeds less that cost. A full exit removes
the entire remaining cost, including any division remainder, and clears the pool. Reopening
starts a fresh cost pool; lifetime realised P&L and fees do not reset. The average cost is `C / Q`
while held and null when flat. Calculations use Decimal, without intermediate price-tick
quantisation, and monetary response values are strings.

For example, buy one unit at 100 and another at 200, then sell one at 180. Weighted-average cost
is 150, realised gross P&L is 30 and the remaining cost is 150. FIFO would realise 80 and leave
a cost of 200. Selling the remainder at 160 gives lifetime realised gross P&L of 40 under either
convention. Reopening at 120 gives a new average cost of 120, with that realised 40 preserved.
Tests assert the partial-sale checkpoint, since full exits alone cannot distinguish the methods.

Fees are accumulated separately from committed fills in the product's quote currency (USD for
all current instruments). Realised and unrealised P&L are **gross**; net P&L is
`realised + unrealised - total_fees_paid`. Fees are not capitalised into acquisition cost and
are not subtracted twice.

Keep existing portfolio fields. Add `cost_basis_method: weighted_average`, `ledger_sequence`
and `pnl`, keyed by configured base asset, including flat assets. Each P&L entry has `quantity`,
`quote_asset`, `mark`, `average_cost_basis`, `realised_pnl`, `unrealised_pnl`, `total_fees_paid`,
`net_pnl` and an explicit unavailable `reason` (null when fully available).

Capture quantities and the writer's committed sequence together using its balance snapshot.
Read the account's complete immutable fills through that sequence, ordered by execution timestamp
and then ledger sequence. No later commit, other account's fills, or history display limit enters
the calculation. The read joins each fill's immutable source key to its ledger transaction, and
does not depend on mutable order totals. Reservations do not remove owned inventory: the quantity
includes available and reserved balances.

Use only the current Gemini book midpoint, via `OrderBook.mid()`, when the existing market gate
is open. Missing, unready, resyncing, one-sided, non-Gemini or gate-closed books yield a null mark,
unrealised P&L and net P&L. There is no interpolation or last-fill-price fallback. Known realised
P&L and fees remain available without a mark. Ledger read failures or a missing ledger yield
unavailable accounting fields with reason `ledger_unavailable`, preserving existing portfolio
fields rather than inventing zero lifetime earnings or fees.

Fills must establish the entire acquisition history. A sale without sufficient fill-established
inventory, or a difference between the fill-derived quantity and the captured balance, gives
`unknown_cost_basis`. Basis and P&L are then null, while committed fees remain available. This
conservatively declines lifetime P&L for assets with unpriced non-cash deposits, including after
such inventory has been sold; a deposit is not a zero-cost acquisition. Supplying deposit cost
metadata or recovering a known basis after an unpriced inventory epoch is future work.

For a cash-seeded account with no later external transfers, the agreement test compares
`realised + unrealised - fees` directly with `evaluator.final_equity - evaluator.initial_equity`,
and compares fees, fee drag and net return exactly. It supplies identical committed fills and
the same midpoint at an evaluator period boundary. The evaluator does not itself report a
realised/unrealised decomposition, so this test compares the common economic quantity rather
than changing its conventions.

## Consequences

The live view survives restarts and declares its cost-basis and fee conventions. A real Postgres
test checks partial exit, full exit and reopening against the unchanged evaluator, then checks
the same values after recovery. Additional tests cover the committed sequence boundary, account
isolation, more than 1,000 fills, unavailable basis and unavailable marks.

Each portfolio call replays complete committed fills. This avoids a new mutable accounting
projection, but work grows with account history. The existing ledger timeout applies; a failed
read produces unavailable P&L rather than silently truncating history. A future incremental
projection must retain these sequence and recovery guarantees and be justified by measurements.
