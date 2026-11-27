-- Target table for logstash-output-zerobus. Replace <catalog>, <schema>, and <table>.
-- Fields that don't match the schema go to the rescue column
-- when the tag zerobus-rescue is on the rescue column itself (a table-level tag did not work in the test run).
-- Wait about 7 minutes after the tag before the first write (the test run wrote about 7 minutes after it).
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> (
  message         STRING,
  sequence        LONG,
  event_timestamp STRING,
  rescue          VARIANT
) USING DELTA;

ALTER TABLE <catalog>.<schema>.<table> ALTER COLUMN rescue SET TAGS ('zerobus-rescue');

-- Grants for the service principal that writes the table. Replace <service-principal-client-id>.
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-client-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-client-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<table> TO `<service-principal-client-id>`;
