"""
SDP Gold Layer — OCSF-normalized streaming tables (Delta sinks).

Maps the flattened silver GitHub events to three OCSF v1.7.0 event classes:

  1. API Activity         (class_uid 6003)  — all GitHub events as API calls
  2. Entity Management    (class_uid 3004)  — resource CRUD (branches, tags, repos, releases)
  3. File System Activity (class_uid 1001)  — PushEvent → file modifications

Each gold table is a streaming table with an append flow that reads from
the silver streaming table and writes OCSF-normalized records into the
pre-created Delta sinks (see ddl_ocsf.py).
"""

from pyspark import pipelines as sdp

from utils_pipeline import TABLES, FQN, OCSF

# ═══════════════════════════════════════════════════════════════════════════
# Reusable OCSF metadata struct expression
# ═══════════════════════════════════════════════════════════════════════════

METADATA_EXPR = f"""named_struct(
    'version',        '{OCSF["version"]}',
    'product',        named_struct(
                          'name',        'GitHub',
                          'vendor_name', 'GitHub, Inc.',
                          'version',     '2022-11-28'
                      ),
    'log_provider',   source,
    'log_name',       source_type,
    'log_format',     'JSON',
    'log_version',    'github@events:version@1.0',
    'processed_time', current_timestamp(),
    'logged_time',    event_time
) AS metadata"""


# ═══════════════════════════════════════════════════════════════════════════
# GitHub event type → OCSF activity mapping helpers
# ═══════════════════════════════════════════════════════════════════════════

_API_ACTIVITY_MAP = {
    "CreateEvent":             "1",   # Create
    "PushEvent":               "3",   # Update
    "DeleteEvent":             "4",   # Delete
    "PullRequestEvent":        "3",   # Update
    "PullRequestReviewEvent":  "2",   # Read
    "IssuesEvent":             "3",   # Update
    "IssueCommentEvent":       "1",   # Create
    "WatchEvent":              "2",   # Read
    "ForkEvent":               "1",   # Create
    "ReleaseEvent":            "1",   # Create
    "MemberEvent":             "3",   # Update
    "PublicEvent":             "3",   # Update
    "GollumEvent":             "3",   # Update
    "CommitCommentEvent":      "1",   # Create
}

_API_ACTIVITY_NAME_MAP = {
    "1": "Create",
    "2": "Read",
    "3": "Update",
    "4": "Delete",
}


def _build_case_expr(mapping: dict[str, str], input_col: str, default: str) -> str:
    """Build a SQL CASE WHEN expression from a mapping dict."""
    clauses = " ".join(
        f"WHEN {input_col} = '{k}' THEN '{v}'" for k, v in mapping.items()
    )
    return f"CASE {clauses} ELSE '{default}' END"


# ═══════════════════════════════════════════════════════════════════════════
# 1) API Activity  (class_uid 6003) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

sdp.create_sink(
    name="api_activity",
    format="delta",
    options={"tableName": FQN["api_activity"]},
)

@sdp.append_flow(target="api_activity")
def api_activity_flow():
    """Stream all silver events into the API Activity (6003) Delta sink."""
    activity_id_expr = _build_case_expr(_API_ACTIVITY_MAP, "event_type", "99")
    activity_name_expr = _build_case_expr(
        {v: _API_ACTIVITY_NAME_MAP.get(v, "Other") for v in set(_API_ACTIVITY_MAP.values())},
        f"({activity_id_expr})",
        "Other",
    )

    return (
        spark.readStream
        .table(TABLES["silver"])
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['api_activity']}                   AS class_uid",
            "'API Activity'                                     AS class_name",
            f"{OCSF['category']['application']}                 AS category_uid",
            "'Application Activity'                             AS category_name",
            f"CAST({activity_id_expr} AS INT)                   AS activity_id",
            f"{activity_name_expr}                              AS activity_name",
            f"CAST({OCSF['class']['api_activity']} * 100 + CAST({activity_id_expr} AS INT) AS BIGINT) AS type_uid",
            f"CONCAT('API Activity: ', {activity_name_expr})    AS type_name",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── api object ──
            "named_struct('operation', event_type, 'service', named_struct('name', 'GitHub')) AS api",

            # ── actor ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── resources ──
            "named_struct('name', repo_name, 'uid', CAST(repo_id AS STRING), 'url', repo_url) AS resources",

            # ── message ──
            "CONCAT(actor_login, ' performed ', event_type, ' on ', repo_name) AS message",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'payload_action', payload_action,
                'org_login',      org_login,
                'event_id',       event_id
            )))                                                 AS unmapped""",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2) Entity Management  (class_uid 3004) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

_ENTITY_MGMT_ACTIVITY_MAP = {
    "CreateEvent":  "1",   # Create
    "DeleteEvent":  "4",   # Delete
    "ForkEvent":    "1",   # Create
    "ReleaseEvent": "1",   # Create
    "PublicEvent":  "3",   # Update
}

_ENTITY_MGMT_ACTIVITY_NAME_MAP = {
    "1": "Create",
    "3": "Update",
    "4": "Delete",
}

sdp.create_sink(
    name="entity_management",
    format="delta",
    options={"tableName": FQN["entity_management"]},
)

@sdp.append_flow(target="entity_management")
def entity_management_flow():
    """Stream resource CRUD events into the Entity Management (3004) Delta sink."""
    activity_id_expr = _build_case_expr(_ENTITY_MGMT_ACTIVITY_MAP, "event_type", "99")
    activity_name_expr = _build_case_expr(
        {v: _ENTITY_MGMT_ACTIVITY_NAME_MAP.get(v, "Other") for v in set(_ENTITY_MGMT_ACTIVITY_MAP.values())},
        f"({activity_id_expr})",
        "Other",
    )

    return (
        spark.readStream
        .table(TABLES["silver"])
        .where("event_type IN ('CreateEvent', 'DeleteEvent', 'ForkEvent', 'ReleaseEvent', 'PublicEvent')")
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['entity_management']}              AS class_uid",
            "'Entity Management'                                AS class_name",
            f"{OCSF['category']['iam']}                         AS category_uid",
            "'Identity & Access Management'                     AS category_name",
            f"CAST({activity_id_expr} AS INT)                   AS activity_id",
            f"{activity_name_expr}                              AS activity_name",
            f"CAST({OCSF['class']['entity_management']} * 100 + CAST({activity_id_expr} AS INT) AS BIGINT) AS type_uid",
            f"CONCAT('Entity Management: ', {activity_name_expr}) AS type_name",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── entity (managed entity being acted upon) ──
            """named_struct(
                'name', CASE
                    WHEN event_type IN ('CreateEvent', 'DeleteEvent') AND payload_ref IS NOT NULL THEN payload_ref
                    ELSE repo_name
                END,
                'uid',  CAST(repo_id AS STRING),
                'type', CASE
                    WHEN event_type IN ('CreateEvent', 'DeleteEvent') THEN COALESCE(payload_ref_type, 'repository')
                    WHEN event_type = 'ForkEvent'    THEN 'repository'
                    WHEN event_type = 'ReleaseEvent' THEN 'release'
                    WHEN event_type = 'PublicEvent'   THEN 'repository'
                    ELSE 'unknown'
                END,
                'data', CAST(NULL AS STRING)
            ) AS entity""",

            # ── actor ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── message ──
            """CASE
                WHEN event_type = 'CreateEvent'  THEN CONCAT(actor_login, ' created ', COALESCE(payload_ref_type, 'repository'), ' ', COALESCE(payload_ref, repo_name), ' in ', repo_name)
                WHEN event_type = 'DeleteEvent'  THEN CONCAT(actor_login, ' deleted ', COALESCE(payload_ref_type, 'ref'), ' ', COALESCE(payload_ref, ''), ' in ', repo_name)
                WHEN event_type = 'ForkEvent'    THEN CONCAT(actor_login, ' forked ', repo_name)
                WHEN event_type = 'ReleaseEvent' THEN CONCAT(actor_login, ' published a release on ', repo_name)
                WHEN event_type = 'PublicEvent'  THEN CONCAT(actor_login, ' made ', repo_name, ' public')
                ELSE CONCAT(actor_login, ' performed ', event_type, ' on ', repo_name)
            END                                                 AS message""",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'payload_action',   payload_action,
                'payload_ref',      payload_ref,
                'payload_ref_type', payload_ref_type,
                'repo_url',         repo_url,
                'org_login',        org_login,
                'event_id',         event_id
            )))                                                 AS unmapped""",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# 3) File System Activity  (class_uid 1001) — Delta sink
# ═══════════════════════════════════════════════════════════════════════════

sdp.create_sink(
    name="file_system_activity",
    format="delta",
    options={"tableName": FQN["file_system_activity"]},
)

@sdp.append_flow(target="file_system_activity")
def file_system_activity_flow():
    """Stream PushEvent records into the File System Activity (1001) Delta sink."""
    return (
        spark.readStream
        .table(TABLES["silver"])
        .where("event_type = 'PushEvent'")
        .selectExpr(
            # ── classification ──
            f"{OCSF['class']['file_system_activity']}           AS class_uid",
            "'File System Activity'                             AS class_name",
            f"{OCSF['category']['system']}                     AS category_uid",
            "'System Activity'                                  AS category_name",
            "3                                                  AS activity_id",
            "'Update'                                           AS activity_name",
            f"CAST({OCSF['class']['file_system_activity'] * 100 + 3} AS BIGINT) AS type_uid",
            "'File System Activity: Update'                     AS type_name",
            "1                                                  AS severity_id",
            "'Informational'                                    AS severity",

            # ── occurrence ──
            "event_time                                         AS time",
            "event_date",

            # ── status ──
            "'Success'                                          AS status",
            "1                                                  AS status_id",

            # ── file object (branch ref as the file path) ──
            "named_struct('name', payload_ref, 'path', CONCAT(repo_name, '/', COALESCE(payload_ref, ''))) AS file",

            # ── actor ──
            "named_struct('user', named_struct('name', actor_login, 'uid', CAST(actor_id AS STRING), 'url', actor_url)) AS actor",

            # ── source endpoint ──
            "named_struct('url', actor_url) AS src_endpoint",

            # ── message ──
            "CONCAT(actor_login, ' pushed ', "
            "COALESCE(CAST(payload_distinct_size AS STRING), '0'), "
            "' commit(s) to ', payload_ref, ' in ', repo_name) AS message",

            # ── metadata (OCSF struct) ──
            METADATA_EXPR,

            # ── raw data (full event payload) ──
            "CAST(to_json(named_struct('payload', data, 'source', source, 'sourcetype', source_type)) AS VARIANT) AS raw_data",

            # ── unmapped (single VARIANT) ──
            """parse_json(to_json(named_struct(
                'head_sha',       payload_head_sha,
                'before_sha',     payload_before_sha,
                'push_size',      CAST(payload_size AS STRING),
                'distinct_size',  CAST(payload_distinct_size AS STRING),
                'commits_json',   payload_commits_json,
                'repo_id',        CAST(repo_id AS STRING),
                'org_login',      org_login,
                'event_id',       event_id
            )))                                                 AS unmapped""",
        )
    )
