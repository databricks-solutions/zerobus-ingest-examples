#!/usr/bin/env bash
#
# Prepares the Databricks side of the Debezium -> Zerobus example and renders the
# Debezium config. Run this once before `docker compose up`.
#
# Usage: ./setup.sh <databricks-cli-profile>
#
# Prerequisites:
#   - Databricks CLI, authenticated: databricks auth login --host <workspace-url>
#   - A catalog you can create a schema in, named in .env
#
# Everything here is idempotent. Re-running it is safe and changes nothing that is
# already correct.

set -e

PROFILE="${1:?Usage: ./setup.sh <databricks-cli-profile>}"

CLI="databricks"
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

fail() { echo "ERROR: $*" >&2; exit 1; }

no_catalog() {
    fail "catalog $UC_CATALOG does not exist.
       Point DATABRICKS_CATALOG in .env at one you can already write to, which is
       all this needs. To create it here instead, set
       DATABRICKS_CATALOG_STORAGE_ROOT to an external location and re-run, which
       does additionally need CREATE CATALOG on the metastore. To list catalogs:
         databricks catalogs list --profile $PROFILE"
}

# Name of the catalog if it exists, empty if it does not. Reads the Unity Catalog
# API directly, so it needs no warehouse and can run before anything is created.
catalog_name() {
    $CLI catalogs get "$UC_CATALOG" --profile "$PROFILE" --output json 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("name") or "")' 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Talking to Databricks SQL. Statements go through a file so that quoting inside
# the SQL never has to survive the shell.
# ---------------------------------------------------------------------------

_sql_call() {
    python3 - "$WAREHOUSE_ID" "$1" > "$WORK/req.json" <<'PY'
import json, sys
print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2],
                  "wait_timeout": "50s", "on_wait_timeout": "CANCEL"}))
PY
    $CLI api post /api/2.0/sql/statements --profile "$PROFILE" \
        --json "@$WORK/req.json" > "$WORK/res.json" 2>"$WORK/res.err"
}

# Runs a statement, echoes any rows it returned, and fails loudly if it did not
# succeed.
sql_exec() {
    _sql_call "$1" || { echo "  CLI call failed:"; sed 's/^/    /' "$WORK/res.err" >&2; return 1; }
    python3 - "$WORK/res.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
st = d.get("status", {})
state = st.get("state", "UNKNOWN")
if state != "SUCCEEDED":
    print("  " + state + ": " + st.get("error", {}).get("message", "no message"))
    raise SystemExit(1)
for row in (d.get("result", {}).get("data_array") or []):
    print("    " + " | ".join("NULL" if c is None else str(c) for c in row))
PY
}

# Runs a statement and prints just the rows, comma-joined, for capture. Stays
# silent on failure so callers can treat "no rows" and "no table" alike.
sql_query() {
    _sql_call "$1" || return 0
    python3 - "$WORK/res.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
if d.get("status", {}).get("state") != "SUCCEEDED":
    raise SystemExit(0)
for row in (d.get("result", {}).get("data_array") or []):
    print(",".join("" if c is None else str(c) for c in row))
PY
}

# Splits a .sql file into single statements, substituting the <placeholders>.
# Quote- and comment-aware, so a semicolon inside a string literal or a trailing
# comment cannot split a statement in half.
render_sql() {
    python3 - "$1" "$2" "$UC_CATALOG" "$UC_SCHEMA" "$SP_CLIENT_ID" <<'PY'
import sys

src, outdir, cat, sch, sp = sys.argv[1:6]
sql = open(src).read()
for placeholder, value in (("<catalog>", cat), ("<schema>", sch),
                           ("<service-principal-id>", sp)):
    sql = sql.replace(placeholder, value)

statements, buf, i, n, quote = [], [], 0, len(sql), None
while i < n:
    ch = sql[i]
    if quote:
        buf.append(ch)
        if ch == quote:
            quote = None
        i += 1
    elif ch == "'" or ch == "`":
        quote = ch
        buf.append(ch)
        i += 1
    elif sql.startswith("--", i):
        nl = sql.find("\n", i)
        i = n if nl < 0 else nl
    elif ch == ";":
        statements.append("".join(buf))
        buf = []
        i += 1
    else:
        buf.append(ch)
        i += 1
statements.append("".join(buf))

written = 0
for stmt in statements:
    stmt = " ".join(stmt.split())
    if stmt:
        written += 1
        with open("%s/stmt_%03d.sql" % (outdir, written), "w") as fh:
            fh.write(stmt)
print(written)
PY
}

# ---------------------------------------------------------------------------
echo "========================================="
echo "Debezium -> Zerobus setup"
echo "========================================="

# ---- [1/8] configuration --------------------------------------------------
echo "[1/8] Reading .env"

if [ ! -f "$HERE/.env" ]; then
    cp "$HERE/.env.example" "$HERE/.env"
    fail ".env did not exist, so I created it from .env.example.
       Fill in DATABRICKS_WORKSPACE_URL, DATABRICKS_CLIENT_ID and
       DATABRICKS_CLIENT_SECRET, then run this again."
fi

set -a
# shellcheck disable=SC1091
. "$HERE/.env"
set +a

WORKSPACE_URL="${DATABRICKS_WORKSPACE_URL:?set DATABRICKS_WORKSPACE_URL in .env}"
WORKSPACE_URL="${WORKSPACE_URL#https://}"
WORKSPACE_URL="${WORKSPACE_URL%/}"
SP_CLIENT_ID="${DATABRICKS_CLIENT_ID:-}"
SP_CLIENT_SECRET="${DATABRICKS_CLIENT_SECRET:-}"
SP_NAME="${DATABRICKS_SP_NAME:-debezium-zerobus-example}"
UC_CATALOG="${DATABRICKS_CATALOG:-}"
[ -n "$UC_CATALOG" ] || fail "DATABRICKS_CATALOG is not set in .env.
       Set it to a catalog you can create a schema in. It has to be backed by your
       own cloud storage, because Zerobus cannot write to Databricks default
       storage. To see what you have:
         databricks catalogs list --profile $PROFILE"
UC_SCHEMA="${DATABRICKS_SCHEMA:-zerobus_cdc}"

echo "  workspace: $WORKSPACE_URL"
echo "  target:    $UC_CATALOG.$UC_SCHEMA"

# Check this now rather than at step 5, so a wrong catalog name does not leave a
# service principal and a freshly minted secret behind before it fails.
if [ -z "$(catalog_name)" ] && [ -z "${DATABRICKS_CATALOG_STORAGE_ROOT:-}" ]; then
    no_catalog
fi

# ---- [2/8] service principal ---------------------------------------------
echo "[2/8] Service principal"

if [ -n "$SP_CLIENT_ID" ] && [ -n "$SP_CLIENT_SECRET" ]; then
    echo "  using the one already in .env: $SP_CLIENT_ID"
else
    # Secrets can be minted at workspace level through the proxy endpoint, so
    # workspace admin is enough here and account admin is not required.
    echo "  none in .env, so creating or reusing $SP_NAME"

    $CLI service-principals list --profile "$PROFILE" --output json \
        > "$WORK/sps.json" 2>/dev/null || echo "[]" > "$WORK/sps.json"

    python3 - "$WORK/sps.json" "$SP_NAME" > "$WORK/sp_lookup" <<'SP_LOOKUP_PY'
import json, sys
try:
    principals = json.load(open(sys.argv[1])) or []
except Exception:
    principals = []
for sp in principals:
    if sp.get("displayName") == sys.argv[2]:
        print((sp.get("id") or "") + " " + (sp.get("applicationId") or ""))
        break
SP_LOOKUP_PY
    SP_LOOKUP="$(cat "$WORK/sp_lookup")"

    if [ -n "$SP_LOOKUP" ]; then
        SP_NUMERIC_ID="${SP_LOOKUP%% *}"
        SP_CLIENT_ID="${SP_LOOKUP##* }"
        echo "  reusing $SP_CLIENT_ID"
    else
        $CLI service-principals create --display-name "$SP_NAME" --active \
            --profile "$PROFILE" --output json > "$WORK/sp_new.json" 2>"$WORK/sp_new.err" \
            || fail "could not create a service principal, which needs workspace admin.
       Either ask an admin to create one, or create it yourself and put its application
       ID and secret in .env as DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET."
        SP_NUMERIC_ID="$(python3 - "$WORK/sp_new.json" <<'SP_ID_PY'
import json, sys
print(json.load(open(sys.argv[1]))["id"])
SP_ID_PY
)"
        SP_CLIENT_ID="$(python3 - "$WORK/sp_new.json" <<'SP_APP_PY'
import json, sys
print(json.load(open(sys.argv[1]))["applicationId"])
SP_APP_PY
)"
        echo "  created $SP_CLIENT_ID"
    fi

    # A service principal created a moment ago is not always visible to the secrets
    # endpoint yet, so retry instead of giving up on the first attempt.
    MINTED=false
    for attempt in 1 2 3 4 5 6; do
        if $CLI service-principal-secrets-proxy create "$SP_NUMERIC_ID" --profile "$PROFILE" \
                --output json > "$WORK/sp_secret.json" 2>"$WORK/sp_secret.err"; then
            MINTED=true
            break
        fi
        if [ "$attempt" -lt 6 ]; then
            echo "  secret not ready yet, retrying in 5s"
            sleep 5
        fi
    done
    [ "$MINTED" = true ] || fail "created the service principal but could not mint a secret for it.
       The endpoint said: $(tr -d '\n' < "$WORK/sp_secret.err" | head -c 200)
       Add one under Settings > Identity and access > Service principals > $SP_NAME,
       then put it in .env as DATABRICKS_CLIENT_SECRET."

    SP_CLIENT_SECRET="$(python3 - "$WORK/sp_secret.json" <<'SP_SECRET_PY'
import json, sys
print(json.load(open(sys.argv[1])).get("secret") or "")
SP_SECRET_PY
)"
    [ -n "$SP_CLIENT_SECRET" ] || fail "the secret minted for $SP_NAME came back empty."

    # Write both back, so a re-run reuses this secret rather than minting another.
    python3 - "$HERE/.env" "$SP_CLIENT_ID" "$SP_CLIENT_SECRET" <<'SP_WRITE_PY'
import pathlib, sys

env = pathlib.Path(sys.argv[1])
values = {"DATABRICKS_CLIENT_ID": sys.argv[2], "DATABRICKS_CLIENT_SECRET": sys.argv[3]}
seen = dict.fromkeys(values, False)
lines = []
for line in env.read_text().splitlines():
    key = line.split("=", 1)[0].strip() if "=" in line else ""
    if key in values:
        lines.append(key + "=" + values[key])
        seen[key] = True
    else:
        lines.append(line)
for key, found in seen.items():
    if not found:
        lines.append(key + "=" + values[key])
env.write_text("\n".join(lines) + "\n")
SP_WRITE_PY
    chmod 600 "$HERE/.env"
    echo "  secret minted and written back to .env"
fi

# ---- [3/8] workspace identity --------------------------------------------
echo "[3/8] Resolving workspace ID, region and Zerobus endpoint"

if [ -z "${DATABRICKS_INGEST_ENDPOINT:-}" ]; then
    # The workspace reports its own numeric ID in a response header, so nobody has
    # to go and read it off the settings page.
    WORKSPACE_ID="$(curl -sS -o /dev/null -D - "https://$WORKSPACE_URL/login.html" \
        | tr -d '\r' | awk 'tolower($1) == "x-databricks-org-id:" { print $2 }')"
    [ -n "$WORKSPACE_ID" ] || fail "could not read the workspace ID from $WORKSPACE_URL.
       Set DATABRICKS_INGEST_ENDPOINT in .env by hand instead."

    SUMMARY="$($CLI metastores summary --profile "$PROFILE" --output json)"
    REGION="$(printf '%s' "$SUMMARY" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("region",""))')"
    CLOUD="$(printf '%s' "$SUMMARY" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("cloud",""))')"
    REGION="${DATABRICKS_REGION:-$REGION}"
    [ -n "$REGION" ] || fail "could not determine the region. Set DATABRICKS_REGION in .env."

    case "$CLOUD" in
        aws)   SUFFIX="cloud.databricks.com" ;;
        azure) SUFFIX="azuredatabricks.net" ;;
        gcp)   SUFFIX="gcp.databricks.com" ;;
        *)     fail "unrecognised cloud '$CLOUD'. Set DATABRICKS_INGEST_ENDPOINT in .env." ;;
    esac
    ZEROBUS_ENDPOINT="$WORKSPACE_ID.zerobus.$REGION.$SUFFIX"
else
    ZEROBUS_ENDPOINT="$DATABRICKS_INGEST_ENDPOINT"
    REGION="${DATABRICKS_REGION:-set in .env}"
fi

echo "  region:    $REGION"
echo "  endpoint:  $ZEROBUS_ENDPOINT"

# ---- [4/8] warehouse ------------------------------------------------------
echo "[4/8] Finding a SQL warehouse"

WAREHOUSE_ID="$($CLI warehouses list --profile "$PROFILE" --output json | python3 -c '
import json, sys
warehouses = json.load(sys.stdin)
if not warehouses:
    raise SystemExit(1)
# Prefer one already running, so the first statement is not stuck on a cold start.
warehouses.sort(key=lambda w: w.get("state") != "RUNNING")
print(warehouses[0]["id"])
')" || fail "no SQL warehouse in this workspace. Create one and run this again."

echo "  warehouse: $WAREHOUSE_ID (starts on its own if stopped)"

# ---- [5/8] catalog and schema --------------------------------------------
echo "[5/8] Preparing the catalog and schema"

STORAGE_ROOT="${DATABRICKS_CATALOG_STORAGE_ROOT:-}"

# Use a catalog that already exists wherever possible, because creating one needs
# CREATE CATALOG on the metastore and many workspaces do not grant that. Creating
# the schema only needs CREATE SCHEMA on the catalog.
if [ -n "$(catalog_name)" ]; then
    echo "  using existing catalog $UC_CATALOG"
elif [ -n "$STORAGE_ROOT" ]; then
    sql_exec "CREATE CATALOG IF NOT EXISTS \`$UC_CATALOG\` MANAGED LOCATION '$STORAGE_ROOT'"
    echo "  created catalog $UC_CATALOG on $STORAGE_ROOT"
else
    no_catalog
fi

sql_exec "CREATE SCHEMA IF NOT EXISTS \`$UC_CATALOG\`.\`$UC_SCHEMA\`"

# Zerobus cannot write to a table in Databricks default storage, so check the
# catalog's storage before Debezium ever starts. Comparing against the metastore's
# Databricks-managed location is the reliable test; the bucket name alone is not.
MANAGED_LOC="$($CLI external-locations get __databricks_managed_storage_location \
    --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("url") or "")' 2>/dev/null || true)"
CAT_ROOT="$($CLI catalogs get "$UC_CATALOG" --profile "$PROFILE" --output json 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin).get("storage_root") or "")' 2>/dev/null || true)"

if [ -n "$MANAGED_LOC" ] && [ -n "$CAT_ROOT" ] && [ "${CAT_ROOT#"$MANAGED_LOC"}" != "$CAT_ROOT" ]; then
    fail "catalog $UC_CATALOG stores data in Databricks default storage:
         $CAT_ROOT
       Zerobus cannot write there and fails with error 4024, \"Unsupported table
       kind\". Point DATABRICKS_CATALOG in .env at a catalog backed by your own
       cloud storage. To see what each catalog uses:
         databricks catalogs list --profile $PROFILE --output json"
fi
echo "  ready"

# ---- [6/8] target tables --------------------------------------------------
echo "[6/8] Creating target tables"

COUNT="$(render_sql "$HERE/create_table.sql" "$WORK")"
echo "  applying $COUNT statements from create_table.sql"
for f in "$WORK"/stmt_*.sql; do
    sql_exec "$(cat "$f")" || fail "failed on: $(head -c 140 "$f")"
done
echo "  tables and grants in place"

# ---- [7/8] render the Debezium config ------------------------------------
echo "[7/8] Rendering conf/application.properties"

TEMPLATE="$HERE/conf/application.properties.tmpl"
[ -f "$TEMPLATE" ] || fail "missing template $TEMPLATE"

python3 - "$TEMPLATE" "$HERE/conf/application.properties" \
    __DBZ_SQL_USER__     "${DEBEZIUM_SQL_USER:-debezium}" \
    __DBZ_SQL_PASSWORD__ "${DEBEZIUM_SQL_PASSWORD:?set DEBEZIUM_SQL_PASSWORD in .env}" \
    __MSSQL_DATABASE__   "${MSSQL_DATABASE:-inventory}" \
    __UC_CATALOG__       "$UC_CATALOG" \
    __UC_SCHEMA__        "$UC_SCHEMA" \
    __ZEROBUS_ENDPOINT__ "$ZEROBUS_ENDPOINT" \
    __WORKSPACE_URL__    "$WORKSPACE_URL" \
    __SP_CLIENT_ID__     "$SP_CLIENT_ID" <<'PY'
import sys

template, out = sys.argv[1], sys.argv[2]
text = open(template).read()
for key, value in zip(sys.argv[3::2], sys.argv[4::2]):
    text = text.replace(key, value)

# A leftover placeholder would fail much later and much less clearly, inside
# Debezium, so catch it now.
leftover = [line for line in text.splitlines()
            if "__" in line and not line.lstrip().startswith("#")]
if leftover:
    print("unsubstituted placeholders remain:", file=sys.stderr)
    for line in leftover:
        print("  " + line, file=sys.stderr)
    raise SystemExit(1)

with open(out, "w") as fh:
    fh.write(text)
PY

# The Debezium container runs as uid 185, not as you, and a bind mount passes the
# owner through unchanged on Linux. A restrictive mode here makes the container fail
# with AccessDeniedException. The OAuth secret is passed as an environment variable
# instead, so there is nothing in this file that needs guarding.
chmod 644 "$HERE/conf/application.properties"
# The SQL Server containers run as their own user too and read sqlserver/ through a
# bind mount. A checkout made under a strict umask (027, say) leaves it unreadable to
# them, and sqlcmd then reports only "Invalid filename".
chmod -R a+rX "$HERE/sqlserver"
echo "  written (gitignored; the OAuth secret is passed via the environment, not this file)"

# ---- [8/8] done -----------------------------------------------------------
echo "[8/8] Done"
echo ""
echo "========================================="
echo "Ready"
echo "========================================="
echo "Target tables : $UC_CATALOG.$UC_SCHEMA.orders"
echo "                $UC_CATALOG.$UC_SCHEMA.customers"
echo "Zerobus       : $ZEROBUS_ENDPOINT"
echo ""
echo "Start the pipeline:"
echo "  docker compose up -d"
echo "  docker compose logs -f debezium"
echo ""
echo "Then run this in Databricks SQL and re-run it to watch rows arrive:"
echo ""
echo "  SELECT order_id, order_status, amount, order_date, updated_at, __deleted"
echo "  FROM $UC_CATALOG.$UC_SCHEMA.orders"
echo "  ORDER BY updated_at DESC"
echo "  LIMIT 20;"
echo ""
echo "How far behind the source it is, in seconds:"
echo ""
echo "  SELECT round((unix_millis(current_timestamp())"
echo "                - max(unix_millis(updated_at))) / 1000.0, 1) AS lag_seconds"
echo "  FROM $UC_CATALOG.$UC_SCHEMA.orders;"
