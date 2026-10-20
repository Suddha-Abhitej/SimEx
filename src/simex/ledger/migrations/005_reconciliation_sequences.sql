CREATE TABLE IF NOT EXISTS ledger_transactions (
    transaction_id text PRIMARY KEY,
    sequence bigserial NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS reconciliation_state (
    state_id boolean PRIMARY KEY DEFAULT true CHECK (state_id),
    verified_sequence bigint NOT NULL DEFAULT 0
);

INSERT INTO reconciliation_state (state_id)
VALUES (true)
ON CONFLICT (state_id) DO NOTHING;

INSERT INTO ledger_transactions (transaction_id)
SELECT DISTINCT transaction_id
FROM ledger_postings
ON CONFLICT (transaction_id) DO NOTHING;

ALTER TABLE ledger_postings
    ADD COLUMN IF NOT EXISTS transaction_sequence bigint;

UPDATE ledger_postings AS p
SET transaction_sequence = t.sequence
FROM ledger_transactions AS t
WHERE p.transaction_id = t.transaction_id
  AND p.transaction_sequence IS NULL;

ALTER TABLE ledger_postings
    ALTER COLUMN transaction_sequence SET NOT NULL;

CREATE INDEX IF NOT EXISTS ledger_postings_transaction_sequence_idx
    ON ledger_postings (transaction_sequence);
