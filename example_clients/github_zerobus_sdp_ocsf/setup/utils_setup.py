"""
Shared configuration for setup scripts (DDLs and Zerobus producer).
Keep in sync with pipeline/utils_pipeline.py.
"""

# ---------------------------------------------------------------------------
# Unity Catalog
# ---------------------------------------------------------------------------
UC = {
    "catalog":         "cyber_lakehouse",
    "bronze_database": "github",
    "gold_database":   "ocsf",
}

# ---------------------------------------------------------------------------
# Table names
# ---------------------------------------------------------------------------
TABLES = {
    "bronze":               "github_events_bronze",
    "api_activity":         "api_activity",
    "entity_management":    "entity_management",
    "file_system_activity": "file_system_activity",
}

# ---------------------------------------------------------------------------
# Fully-qualified table references
# ---------------------------------------------------------------------------
FQN = {
    "bronze":               f"{UC['catalog']}.{UC['bronze_database']}.{TABLES['bronze']}",
    "api_activity":         f"{UC['catalog']}.{UC['gold_database']}.{TABLES['api_activity']}",
    "entity_management":    f"{UC['catalog']}.{UC['gold_database']}.{TABLES['entity_management']}",
    "file_system_activity": f"{UC['catalog']}.{UC['gold_database']}.{TABLES['file_system_activity']}",
}

# ---------------------------------------------------------------------------
# GitHub Events API
# ---------------------------------------------------------------------------
GITHUB = {
    "events_url":  "https://api.github.com/events",
    "source":      "github",
    "source_type": "events",
}

# ---------------------------------------------------------------------------
# Zerobus connection  (populate before running push_zerobus.py)
# ---------------------------------------------------------------------------
ZEROBUS = {
    "workspace_url": "<your-workspace-url>",
    "workspace_id":  "<your-workspace-id>",
    "region":        "<your-region>",
    "secrets_scope": "zerobus",
}
ZEROBUS["endpoint"] = f"https://{ZEROBUS['workspace_id']}.zerobus.{ZEROBUS['region']}.cloud.databricks.com"
