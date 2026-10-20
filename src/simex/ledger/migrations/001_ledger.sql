CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_events (
    event_id uuid PRIMARY KEY,
    event_key text NOT NULL UNIQUE,
    aggregate_type text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_seq bigint NOT NULL,
    event_type text NOT NULL,
    occurred_at_ns bigint NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (aggregate_type, aggregate_id, aggregate_seq)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY,
    account_id text NOT NULL,
    client_order_id text NOT NULL,
    request jsonb NOT NULL,
    status text NOT NULL,
    created_at_ns bigint NOT NULL,
    effective_at_ns bigint NOT NULL,
    UNIQUE (account_id, client_order_id)
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id text PRIMARY KEY,
    order_id text NOT NULL,
    account_id text NOT NULL,
    product_id text NOT NULL,
    side text NOT NULL,
    price numeric(38, 18) NOT NULL,
    quantity numeric(38, 18) NOT NULL,
    liquidity text NOT NULL,
    cause text NOT NULL,
    fee numeric(38, 18) NOT NULL,
    fee_asset text NOT NULL,
    ts_ns bigint NOT NULL,
    source_key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_postings (
    posting_id uuid PRIMARY KEY,
    transaction_id text NOT NULL,
    fill_id text NOT NULL REFERENCES fills (fill_id),
    account_id text NOT NULL,
    asset text NOT NULL,
    amount numeric(38, 18) NOT NULL,
    entry_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (transaction_id, account_id, asset, entry_type)
);

CREATE TABLE IF NOT EXISTS outbox (
    outbox_id uuid PRIMARY KEY,
    event_key text NOT NULL UNIQUE,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
