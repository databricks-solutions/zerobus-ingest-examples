"""
DDLs for the OCSF gold Delta tables.

Run this once before deploying the SDP pipeline so the gold tables exist
as Delta sinks in cyber_lakehouse.ocsf.

Execute as a Databricks notebook or via dbsql / spark.sql().
"""

from utils_setup import FQN

# ── shared OCSF struct types ─────────────────────────────────────────────
_METADATA_TYPE = """STRUCT<
    version:        STRING,
    product:        STRUCT<name: STRING, vendor_name: STRING, version: STRING>,
    log_provider:   STRING,
    log_name:       STRING,
    log_format:     STRING,
    log_version:    STRING,
    processed_time: TIMESTAMP,
    logged_time:    TIMESTAMP
>"""

_ACTOR_TYPE = "STRUCT<user: STRUCT<name: STRING, uid: STRING, url: STRING>>"

_SRC_ENDPOINT_TYPE = "STRUCT<url: STRING>"

# ═══════════════════════════════════════════════════════════════════════════
# 1) API Activity  (class_uid 6003)
# ═══════════════════════════════════════════════════════════════════════════
DDL_API_ACTIVITY = f"""
CREATE TABLE IF NOT EXISTS {FQN['api_activity']} (
    -- classification
    class_uid       INT,
    class_name      STRING,
    category_uid    INT,
    category_name   STRING,
    activity_id     INT,
    activity_name   STRING,
    type_uid        BIGINT,
    type_name       STRING,
    severity_id     INT,
    severity        STRING,

    -- occurrence
    time            TIMESTAMP,
    event_date      DATE,

    -- status
    status          STRING,
    status_id       INT,

    -- api object
    api                 STRUCT<operation: STRING, service: STRUCT<name: STRING>>,

    -- actor
    actor               {_ACTOR_TYPE},

    -- source endpoint
    src_endpoint        {_SRC_ENDPOINT_TYPE},

    -- resources
    resources           STRUCT<name: STRING, uid: STRING, url: STRING>,

    -- message
    message             STRING,

    -- metadata (OCSF struct)
    metadata            {_METADATA_TYPE},

    -- raw data
    raw_data    VARIANT,

    -- unmapped
    unmapped    VARIANT
)
USING DELTA
CLUSTER BY (time)
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
COMMENT 'OCSF v1.7.0 API Activity (6003) — all GitHub public events as API calls'
;
"""

# ═══════════════════════════════════════════════════════════════════════════
# 2) Entity Management  (class_uid 3004)
# ═══════════════════════════════════════════════════════════════════════════
DDL_ENTITY_MANAGEMENT = f"""
CREATE TABLE IF NOT EXISTS {FQN['entity_management']} (
    -- classification
    class_uid       INT,
    class_name      STRING,
    category_uid    INT,
    category_name   STRING,
    activity_id     INT,
    activity_name   STRING,
    type_uid        BIGINT,
    type_name       STRING,
    severity_id     INT,
    severity        STRING,

    -- occurrence
    time            TIMESTAMP,
    event_date      DATE,

    -- status
    status          STRING,
    status_id       INT,

    -- entity (managed entity being acted upon)
    entity              STRUCT<name: STRING, uid: STRING, type: STRING, data: STRING>,

    -- actor
    actor               {_ACTOR_TYPE},

    -- source endpoint
    src_endpoint        {_SRC_ENDPOINT_TYPE},

    -- message
    message             STRING,

    -- metadata (OCSF struct)
    metadata            {_METADATA_TYPE},

    -- raw data
    raw_data    VARIANT,

    -- unmapped
    unmapped    VARIANT
)
USING DELTA
CLUSTER BY (time)
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
COMMENT 'OCSF v1.7.0 Entity Management (3004) — GitHub resource CRUD (branches, tags, repos, releases)'
;
"""

# ═══════════════════════════════════════════════════════════════════════════
# 3) File System Activity  (class_uid 1001)
# ═══════════════════════════════════════════════════════════════════════════
DDL_FILE_SYSTEM_ACTIVITY = f"""
CREATE TABLE IF NOT EXISTS {FQN['file_system_activity']} (
    -- classification
    class_uid       INT,
    class_name      STRING,
    category_uid    INT,
    category_name   STRING,
    activity_id     INT,
    activity_name   STRING,
    type_uid        BIGINT,
    type_name       STRING,
    severity_id     INT,
    severity        STRING,

    -- occurrence
    time            TIMESTAMP,
    event_date      DATE,

    -- status
    status          STRING,
    status_id       INT,

    -- file object
    file                STRUCT<name: STRING, path: STRING>,

    -- actor
    actor               {_ACTOR_TYPE},

    -- source endpoint
    src_endpoint        {_SRC_ENDPOINT_TYPE},

    -- message
    message             STRING,

    -- metadata (OCSF struct)
    metadata            {_METADATA_TYPE},

    -- raw data
    raw_data    VARIANT,

    -- unmapped
    unmapped    VARIANT
)
USING DELTA
CLUSTER BY (time)
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
COMMENT 'OCSF v1.7.0 File System Activity (1001) — GitHub PushEvent file modifications'
;
"""

ALL_DDLS = [DDL_API_ACTIVITY, DDL_ENTITY_MANAGEMENT, DDL_FILE_SYSTEM_ACTIVITY]

if __name__ == "__main__":
    for ddl in ALL_DDLS:
        print(ddl)

    try:
        for ddl in ALL_DDLS:
            for stmt in ddl.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    spark.sql(stmt)
        print(f"✓  {FQN['api_activity']} created")
        print(f"✓  {FQN['entity_management']} created")
        print(f"✓  {FQN['file_system_activity']} created")
    except NameError:
        print("(spark session not available — copy the DDLs above into a Databricks notebook)")
