-- Run in Databricks SQL after the smoke test. Replace <catalog>.<schema> with your values.
-- Expect at least one 'success' and one 'failure' row tagged player:smoke-test.
SELECT status, count(*) AS requests, max(start_time) AS latest
FROM <catalog>.<schema>.litellm_requests
WHERE array_contains(CAST(request_tags AS ARRAY<STRING>), 'player:smoke-test')
GROUP BY status;
