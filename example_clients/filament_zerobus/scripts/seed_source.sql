-- Sample source tables in the "shop" database. The table names come from psql variables set by
-- init_source.sh, so the demo can add TABLE_PREFIX.

-- Full append: Filament sends every row on every run.
CREATE TABLE :"orders_table" (
  id         BIGINT PRIMARY KEY,
  customer   TEXT NOT NULL,
  amount     NUMERIC(10,2),
  status     TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO :"orders_table" (id, customer, amount, status)
SELECT n, 'customer-' || n, round(n * 7.25, 2), CASE WHEN n % 3 = 0 THEN 'SHIPPED' ELSE 'NEW' END
FROM generate_series(1, 20) AS n;

-- CDC (cdc_append): Filament reads the changes from the replication stream.
CREATE TABLE :"payments_table" (
  id         BIGINT PRIMARY KEY,
  order_id   BIGINT NOT NULL,
  amount     NUMERIC(10,2) NOT NULL,
  state      TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO :"payments_table" (id, order_id, amount, state)
SELECT n, n, round(n * 7.25, 2), 'PENDING' FROM generate_series(1, 10) AS n;
