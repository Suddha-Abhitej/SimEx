CREATE INDEX IF NOT EXISTS ledger_postings_account_sequence_idx
    ON ledger_postings (account_id, transaction_sequence);

CREATE TABLE IF NOT EXISTS reconciliation_violations (
    transaction_id text NOT NULL,
    asset text NOT NULL,
    total numeric(38,18) NOT NULL,
    sequence bigint NOT NULL,
    first_seen_at_ns bigint NOT NULL,
    PRIMARY KEY (transaction_id, asset)
);
