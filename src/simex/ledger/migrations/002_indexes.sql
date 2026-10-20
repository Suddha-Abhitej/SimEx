CREATE INDEX IF NOT EXISTS ledger_events_aggregate_idx
    ON ledger_events (aggregate_type, aggregate_id, aggregate_seq);

CREATE INDEX IF NOT EXISTS ledger_postings_account_asset_idx
    ON ledger_postings (account_id, asset);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;
