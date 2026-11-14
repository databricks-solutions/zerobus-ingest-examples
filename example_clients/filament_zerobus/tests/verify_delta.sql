-- Checks after the demo runs. Replace <catalog>, <schema> and <cdc-schema>, then run in Databricks SQL.
-- Expected after one full run of orders-to-zb: 20 rows, 20 distinct ids. A second run adds 20 more rows,
-- so the table then holds 40 rows for 20 ids (full append resends every row).
SELECT count(*) AS rows_landed, count(DISTINCT id) AS distinct_ids
FROM <catalog>.<schema>.orders;

-- Expected after the snapshot and the changes in scripts/change_source.sql: 15 rows in total
-- (11 insert, 3 update, 1 delete).
SELECT _filament_operation, count(*) AS changes
FROM <catalog>.<cdc-schema>.payments
GROUP BY _filament_operation
ORDER BY _filament_operation;
