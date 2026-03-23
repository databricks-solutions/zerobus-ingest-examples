"""
SDP Silver Layer — flattened GitHub events.

Reads from the bronze streaming table and extracts key fields from the
VARIANT `data` column using Spark SQL variant path notation.  The result
is a strongly-typed, query-friendly table that feeds the gold OCSF layer.
"""

from pyspark import pipelines as sdp

from utils_pipeline import TABLES, TABLE_PROPERTIES


@sdp.table(
    name=TABLES["silver"],
    cluster_by=["event_date"],
    table_properties=TABLE_PROPERTIES,
)
def github_events_silver():
    """Flatten variant bronze data into typed silver columns."""
    return (
        spark.readStream
        .table(TABLES["bronze"])
        .selectExpr(
            # ── event identifiers ──
            "data:id::string                    AS event_id",
            "data:type::string                  AS event_type",

            # ── timestamps ──
            "event_time",
            "event_date",

            # ── actor ──
            "data:actor.id::long                AS actor_id",
            "data:actor.login::string           AS actor_login",
            "data:actor.display_login::string   AS actor_display_login",
            "data:actor.url::string             AS actor_url",
            "data:actor.avatar_url::string      AS actor_avatar_url",

            # ── repository ──
            "data:repo.id::long                 AS repo_id",
            "data:repo.name::string             AS repo_name",
            "data:repo.url::string              AS repo_url",

            # ── organisation (nullable) ──
            "data:org.id::long                  AS org_id",
            "data:org.login::string             AS org_login",

            # ── payload (flattened common fields) ──
            "data:payload.action::string        AS payload_action",
            "data:payload.ref::string           AS payload_ref",
            "data:payload.ref_type::string      AS payload_ref_type",
            "data:payload.head::string          AS payload_head_sha",
            "data:payload.before::string        AS payload_before_sha",
            "data:payload.size::int             AS payload_size",
            "data:payload.distinct_size::int    AS payload_distinct_size",

            # ── member event fields ──
            "data:payload.member.id::long       AS member_id",
            "data:payload.member.login::string  AS member_login",
            "data:payload.member.type::string   AS member_type",

            # ── push event commit summary ──
            "data:payload.commits::string       AS payload_commits_json",

            # ── pull request fields ──
            "data:payload.pull_request.id::long             AS pr_id",
            "data:payload.pull_request.number::int          AS pr_number",
            "data:payload.pull_request.title::string        AS pr_title",
            "data:payload.pull_request.state::string        AS pr_state",
            "data:payload.pull_request.user.login::string   AS pr_user_login",

            # ── visibility & metadata ──
            "data:public::boolean               AS is_public",
            "data",
            "source",
            "source_type",
            "source_path",
            "processed_time",
        )
    )
