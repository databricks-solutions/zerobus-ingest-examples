-- Run once in your workspace (SQL editor or any SQL client), replacing the table name.
-- Zerobus Ingest never creates the table for you.
--
-- The rescue column is required. Without it, one record that doesn't match the table
-- (for example a string in an INT column) closes the MQTT connection with reason 0x83.
-- With it, the mismatched or extra field is stored in rescue and the connection stays up.
-- Keep the columns nullable: the rescue behavior applies to nullable columns.
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.dice_rolls (
  player_name STRING,
  die_sides INT,
  roll INT,
  rolled_at TIMESTAMP,
  rescue VARIANT
);

-- Tag the rescue column. Tag changes can take up to 5 minutes to take effect, so
-- publish only after this statement has been in place for at least 5 minutes.
ALTER TABLE <catalog>.<schema>.dice_rolls
  ALTER COLUMN rescue SET TAGS ('zerobus-rescue');

-- Grant the service principal the four privileges the MQTT endpoint needs.
-- Replace <service-principal-application-id> with the service principal's application ID.
GRANT USE CATALOG ON CATALOG <catalog> TO `<service-principal-application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.<schema> TO `<service-principal-application-id>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.dice_rolls TO `<service-principal-application-id>`;
