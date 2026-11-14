-- Changes for the CDC demo: one insert, three updates and one delete on payments.
-- Run it with the variable set to the payments table name:
--   docker compose exec -T postgres psql -U filament -d shop -v payments_table=payments < scripts/change_source.sql

INSERT INTO :"payments_table" (id, order_id, amount, state) VALUES (11, 11, 79.75, 'PENDING');
UPDATE :"payments_table" SET state = 'PAID', updated_at = now() WHERE id IN (1, 2, 3);
DELETE FROM :"payments_table" WHERE id = 10;
