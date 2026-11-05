# 0022. Account order and position risk limits

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Per-order quantity and notional checks in the MCP façade do not protect direct account-writer
calls or future REST entry. They also do not bound accumulation across individually legal
orders. Checking filled inventory alone leaves a second bypass: many resting orders can all pass
while flat, then fill beyond the intended position cap.

The owner approved the proposal on issue #17, including the exposure convention below. This
extends order acceptance and preserves the existing matching, timing and settlement decisions.

## Decision

`RiskLimits` belongs to the exchange layer. A configured `AccountWriter` enforces per-product
order quantity, order notional and maximum absolute position in `submit`, after validation and
required gate/price resolution, before reserving funds or accepting the order. A disabled product
is refused without looking up a gate/book it might not have. Limits must be positive finite
Decimals and cover the same products. Monetary refusal values are serialized as strings.

The live runtime requires a complete policy covering its configured pairs, installs it on the
writer before tasks start, and applies it even with MCP and baselines disabled. The same caps
apply independently to the owner and each baseline account, preserving ADR 0019's shared policy
convention. Other accounts and products do not consume the caller's exposure budget. Isolated
lower-level matching/replay writers can still be constructed without risk policy; MCP order
entry on such a writer fails with `risk_limits_missing`. The existing MCP import of `RiskLimits`
remains available, but façade limits cannot weaken the configured writer's policy.

### Position exposure: bound each side independently

For the submitting account and product, let:

- P be its signed current base-asset balance, including available and reserved holdings;
- B be remaining quantity of its accepted, nonterminal buy orders;
- S be remaining quantity of its accepted, nonterminal sell orders;
- L be the configured maximum absolute position and q the new order's quantity.

A buy requires `P + B + q <= L`. A sell requires `P - S - q >= -L`. These separately bound
the highest and lowest reachable positions; they do not net opposite-side open orders.

Count PENDING orders from acceptance, including market orders and the entire anti-cheat delay;
OPEN/resting orders; and the remaining quantity of PARTIALLY_FILLED orders. The filled part is
already in P, so it must not also count in B or S. A requested cancellation still counts until
the engine actually makes it terminal, since it may fill during the cancel delay. FILLED,
CANCELLED, EXPIRED and REJECTED orders contribute no remaining exposure. Exclude the candidate
from existing-order totals and add q exactly once. Idempotent resubmission returns the existing
order before another risk check and does not consume exposure again.

The no-offset rule deliberately refuses a buy for which a pending reducing sell would have made
room if it filled first. That sell may never fill, may be cancelled, or may fill after the buy.
Crediting it would make the cap depend on an unguaranteed future fill/order sequence. Capacity
becomes available when the sell actually fills and changes P. The same conservatism applies to
pending buys that might reduce a short.

A sell reducing a long at +L is allowed provided its own possible endpoint remains at or above
-L, and a buy reducing a short is symmetric. Existing orders are not retroactively cancelled if
new configuration lowers a cap below restored exposure. Orders in the reducing direction can
still pass the same endpoint rule; further accumulation cannot. All orders, including reducing
ones, remain subject to order-size, notional, instrument, gate and funding checks. This policy is
symmetric for signed positions; it does not introduce borrowing or short settlement into the
existing spot exchange.

Deposits are **not bounded** by the position cap: it applies at **order acceptance**, not to
external funding movements. Deposited base inventory is included in P for subsequent orders.
Consequently a deposit can leave holdings above the cap; reducing orders remain possible while
further accumulation is refused. There is no new check on the deposit or fill paths.

### Order limits and refusals

Order notional uses the same price as the existing reservation: the requested limit price or
the existing bounded market price. Risk adds no price-band or timing constants.

Preserve `quantity_limit`, `notional_limit` and `product_not_allowed`; add `position_limit`.
Numeric refusals carry separate `rule`, `product_id`, `limit` and `attempted` fields. Position
refusals report the signed endpoint as `attempted`, its `side`, and the permitted `minimum` and
`maximum`. No book levels or other accounts' values are included. Reasons remain stable for
agents and refusal metrics.

Risk refusals follow the existing recorded REJECTED-order pattern. Store `reject_details` on
the order and in a JSONB column added by migration 008. Recover it with the order so retries
retain the original cap and attempted value after exposure or configuration changes, including
across ledger recovery. MCP returns the writer's decision and details, and does not rerun risk
against a cached order; market retries also avoid resolving a new band before that decision.
Old order rows default to empty details. Matching and fill production/settlement are unchanged.

### Configuration

Document `EXCHANGE_MAX_QUANTITY_<PAIR>`, `EXCHANGE_MAX_NOTIONAL_<PAIR>` and
`EXCHANGE_MAX_POSITION_<PAIR>` in `.env.example`. Quantity and notional cap each order; position
is a base-asset quantity bound. The former `EXCHANGE_MCP_MAX_QUANTITY/NOTIONAL_<PAIR>` settings
are supported as aliases. Equal Decimal values are equivalent; conflicting aliases fail startup.
Missing, nonpositive, nonfinite or incomplete settings fail startup before starting the live
runtime. Position caps have no inferred or unlimited default: the owner must choose them.

## Consequences

Every entry into the configured writer has the same account checks. Exposure derives from
existing balances and remaining live orders, so it survives recovery without a second mutable
exposure projection or changes to fill callbacks. Checking orders scans the existing order
records; an incremental index would need measurement and separate lifecycle validation.

Tests call the writer directly, attempt twenty pending/resting orders, exercise partial fills,
cancel delay, terminal states, reducing orders and both signed directions, and verify account
and product isolation. Postgres tests recover all live order states and original refusal details.
Runtime tests exercise the owner with MCP disabled. Tests also cover configuration aliases,
invalid limits, immutable policy maps and the unmodified balance projection.
