-- Must agree with OrderStatus.terminal and RECOVERY_ORDERS_SQL (guarded by tests).
CREATE INDEX IF NOT EXISTS orders_live_account_idx ON orders (account_id, order_id)
WHERE status NOT IN ('filled', 'cancelled', 'expired', 'rejected');
