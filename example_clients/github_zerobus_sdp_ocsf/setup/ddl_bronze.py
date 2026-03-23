"""
Setup DDL — creates the catalog, schemas, and the bronze Zerobus target table.

  - cyber_lakehouse                               (catalog)
  - cyber_lakehouse.github                        (bronze / silver schema)
  - cyber_lakehouse.ocsf                          (gold OCSF schema)
  - cyber_lakehouse.github.github_events_bronze   (Zerobus target table)

The bronze table must exist before push_zerobus.py starts streaming.

Execute as a Databricks notebook or via dbsql / spark.sql().
"""

from utils_setup import UC, FQN

DDL = f"""
CREATE CATALOG IF NOT EXISTS {UC['catalog']};

CREATE DATABASE IF NOT EXISTS {UC['catalog']}.{UC['bronze_database']};

CREATE DATABASE IF NOT EXISTS {UC['catalog']}.{UC['gold_database']};

CREATE TABLE IF NOT EXISTS {FQN['bronze']} (
    data            VARIANT       COMMENT 'Raw GitHub event JSON',
    event_time      TIMESTAMP     COMMENT 'Event timestamp extracted from created_at',
    event_date      DATE          COMMENT 'Event date partition key',
    source          STRING        COMMENT 'Source identifier, e.g. github_events_api',
    source_type     STRING        COMMENT 'Source type, e.g. rest_api',
    source_path     STRING        COMMENT 'API endpoint URL that produced this record',
    processed_time  TIMESTAMP     COMMENT 'Zerobus ingestion timestamp'
)
USING DELTA
CLUSTER BY (event_time)
TBLPROPERTIES (
  'delta.minWriterVersion' = '7',
  'delta.enableDeletionVectors' = 'true',
  'delta.minReaderVersion' = '3',
  'delta.feature.variantType-preview' = 'supported',
  'delta.enableVariantShredding' = 'true',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'otel.schemaVersion' = 'v2'
)
COMMENT 'Bronze table for GitHub public events — populated by Zerobus SDK'
;
"""

if __name__ == "__main__":
    print(DDL)

    try:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                spark.sql(stmt)
        print(f"✓  {UC['catalog']} catalog ready")
        print(f"✓  {UC['catalog']}.{UC['bronze_database']} database ready")
        print(f"✓  {UC['catalog']}.{UC['gold_database']} database ready")
        print(f"✓  {FQN['bronze']} table created")
    except NameError:
        print("(spark session not available — copy the DDL above into a Databricks notebook)")
