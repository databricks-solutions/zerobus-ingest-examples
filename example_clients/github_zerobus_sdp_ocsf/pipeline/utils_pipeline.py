"""
SDP pipeline configuration — catalog, table names, and OCSF constants.
"""

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
UC = {
    "catalog":          "cyber_lakehouse",
    "bronze_database":  "github",
    "silver_database":  "github",
    "gold_database":    "ocsf",
}

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
TABLES = {
    "bronze":                 "github_events_bronze",
    "silver":                 "github_events_silver",
    "api_activity":           "api_activity",
    "entity_management":      "entity_management",
    "file_system_activity":   "file_system_activity",
}

# ---------------------------------------------------------------------------
# Fully-qualified table references
# ---------------------------------------------------------------------------
FQN = {
    "bronze": f"{UC['catalog']}.{UC['bronze_database']}.{TABLES['bronze']}",
    "silver": f"{UC['catalog']}.{UC['silver_database']}.{TABLES['silver']}",
    "api_activity":         f"{UC['catalog']}.{UC['gold_database']}.{TABLES['api_activity']}",
    "entity_management":    f"{UC['catalog']}.{UC['gold_database']}.{TABLES['entity_management']}",
    "file_system_activity": f"{UC['catalog']}.{UC['gold_database']}.{TABLES['file_system_activity']}",
}

# ---------------------------------------------------------------------------
# Table properties
# ---------------------------------------------------------------------------
TABLE_PROPERTIES = {
    "delta.minWriterVersion": "7",
    "delta.enableDeletionVectors": "true",
    "delta.minReaderVersion": "3",
    "delta.feature.variantType-preview": "supported",
    "delta.enableVariantShredding": "true",
    "delta.feature.deletionVectors": "supported",
    "delta.feature.invariants": "supported",
    "otel.schemaVersion": "v2",
}

# ---------------------------------------------------------------------------
# OCSF constants (v1.7.0)
# ---------------------------------------------------------------------------
OCSF = {
    "version":    "1.7.0",
    "category": {
        "system":      1,
        "iam":         3,
        "application": 6,
    },
    "class": {
        "file_system_activity": 1001,
        "entity_management":    3004,
        "api_activity":         6003,
    },
}
