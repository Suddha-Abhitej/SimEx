ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS reject_details jsonb NOT NULL DEFAULT '{}'::jsonb;
