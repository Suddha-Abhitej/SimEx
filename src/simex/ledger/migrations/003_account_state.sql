ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS filled_quantity numeric(38, 18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS filled_notional numeric(38, 18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS maker_notional numeric(38, 18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS taker_notional numeric(38, 18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fees numeric(38, 18) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS fill_count integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS market_band_price numeric(38, 18),
    ADD COLUMN IF NOT EXISTS reject_reason text,
    ADD COLUMN IF NOT EXISTS cancel_requested_at_ns bigint,
    ADD COLUMN IF NOT EXISTS cancel_effective_at_ns bigint,
    ADD COLUMN IF NOT EXISTS closed_at_ns bigint;

ALTER TABLE ledger_postings
    ALTER COLUMN fill_id DROP NOT NULL;

CREATE TABLE IF NOT EXISTS account_reservations (
    order_id text PRIMARY KEY,
    account_id text NOT NULL,
    asset text NOT NULL,
    remaining numeric(38, 18) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
