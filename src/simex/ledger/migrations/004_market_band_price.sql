ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS market_band_price numeric(38, 18);
