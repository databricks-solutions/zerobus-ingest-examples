"""Static checks for the Node-RED example flow. Standard library only.

Run from the example folder:  python3 -I tests/test_flows.py

Checks that flows.json is importable (valid JSON, every wire points at a node that exists, no flat wire arrays),
that it carries no credentials or hosts (every connection value comes from an environment variable), that the
token request and the connect action use the table-scoped OAuth flow, that every reconnect (start, refresh, watch)
fetches a token before its connect action, and that settings.js and run-docker.sh keep the editor private (container
listens on all its interfaces; the host publishes it on 127.0.0.1 only).
"""
import json
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
FLOWS = ROOT / "node-red" / "flows.json"
SETTINGS = ROOT / "node-red" / "settings.js"
REQUIRED_ENV = [
    "DATABRICKS_WORKSPACE_URL",
    "DATABRICKS_WORKSPACE_ID",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_TABLE_NAME",
    "ZEROBUS_MQTT_HOST",
]
SECRET_LIKE = re.compile(r"dapi[0-9a-f]{20,}|eyJ[A-Za-z0-9_-]{20,}|[0-9a-f]{32}|\.zerobus\.|databricks\.com|@")


def main():
    failures = []

    def check(ok, message):
        print(("PASS " if ok else "FAIL ") + message)
        if not ok:
            failures.append(message)

    nodes = json.loads(FLOWS.read_text())
    ids = {n["id"] for n in nodes if "id" in n}
    check(bool(nodes), "flows.json parses and has nodes")

    flat = [n["id"] for n in nodes if "wires" in n and any(not isinstance(w, list) for w in n["wires"])]
    check(not flat, "every wire array is nested (no flat arrays): %s" % (flat or "ok"))

    dangling = []
    for n in nodes:
        for port in n.get("wires", []):
            for target in port:
                if target not in ids:
                    dangling.append((n["id"], target))
    check(not dangling, "every wire points at an existing node: %s" % (dangling or "ok"))

    text = FLOWS.read_text()
    for name in REQUIRED_ENV:
        check(("env.get('%s')" % name) in text or name in text, "flow reads %s from the environment" % name)

    leaks = [m.group(0) for m in SECRET_LIKE.finditer(text)]
    check(not leaks, "no credential, token or host literal in flows.json: %s" % (leaks or "ok"))

    check("authorization_details" in text and "zerobusDirectWriteApi" in text,
          "token request uses table-scoped authorization_details and the zerobusDirectWriteApi resource")
    check("client_credentials" in text and "/oidc/v1/token" in text,
          "token request uses the OAuth client credentials grant at /oidc/v1/token")
    check("'x-databricks-zerobus-table-name'" in text, "CONNECT carries the table-name user property")
    check("force: msg.cycle === true" in text, "refresh path cycles the connection with force:true")

    # Fresh token on every reconnect: every route into the connect action passes the token request.
    by_id = {n["id"]: n for n in nodes if "id" in n}
    fn_plan = by_id.get("fn-plan", {})
    plan_targets = [t for port in fn_plan.get("wires", []) for t in port]
    check(plan_targets == ["fn-token-req"],
          "trigger planner sends every reconnect (start, refresh, watch) to the token request: %s" % plan_targets)
    watch_wired = [n["id"] for n in nodes if n.get("id") == "inj-watch"
                   and any("fn-plan" in port for port in n.get("wires", []))]
    check(bool(watch_wired), "watch trigger feeds the trigger planner")
    connect_in = [n["id"] for n in nodes if any("fn-connect" in port for port in n.get("wires", []))]
    check(connect_in == ["fn-store"], "only the token store feeds the connect action: %s" % connect_in)
    store_in = [n["id"] for n in nodes if any("fn-store" in port for port in n.get("wires", []))]
    check(store_in == ["http-token"], "only the token HTTP request feeds the token store: %s" % store_in)
    token_in = [n["id"] for n in nodes if any("fn-token-req" in port for port in n.get("wires", []))]
    check(sorted(token_in) == ["fn-plan"], "only the planner feeds the token request: %s" % token_in)
    check("zbLinkUp" in fn_plan.get("func", "") and "topic === 'watch'" in fn_plan.get("func", ""),
          "watch trigger skips the token request while the link is up (flow.zbLinkUp)")
    status_node = by_id.get("status-link", {})
    check(status_node.get("type") == "status" and status_node.get("scope") == ["mqtt-out"],
          "link state comes from a status node scoped to the mqtt out node")
    check("zbLinkUp" in by_id.get("fn-link", {}).get("func", "") and "common\\.status\\.disconnected" in by_id.get("fn-link", {}).get("func", ""),
          "link-state function sets zbLinkUp from the connected and disconnected status")
    check("'x-databricks-zerobus-table-name'" in by_id.get("fn-connect", {}).get("func", "")
          and "msg.action = 'connect'" in by_id.get("fn-connect", {}).get("func", ""),
          "connect action keeps msg.action = 'connect' and the JSON string user properties")

    mqtt = [n for n in nodes if n.get("type") == "mqtt-broker"]
    check(len(mqtt) == 1 and mqtt[0].get("autoConnect") is False, "broker config does not auto-connect (no static credential)")
    check(all(not n.get("username") and not n.get("password") for n in mqtt), "broker config has no username or password")

    sql = (ROOT / "scripts" / "create_table.sql").read_text()
    check("rescue VARIANT" in sql and "ALTER COLUMN rescue SET TAGS ('zerobus-rescue')" in sql,
          "create_table.sql adds the rescue VARIANT column and applies the zerobus-rescue tag")
    check("rescue" in sql and all(col in sql for col in ["roll_id BIGINT", "die_sides INT", "roll INT"]),
          "create_table.sql keeps the typed columns next to rescue")

    readme = (ROOT / "README.md").read_text()
    head = "\n".join(readme.splitlines()[:6])
    check("rescue" in head and "required" in head,
          "README states near the top that the rescue column is required")
    check("create_table.sql" in readme and "zerobus-rescue" in readme,
          "README points to create_table.sql and the tag in Prerequisites")

    ids_by_name = {n.get("id"): n for n in nodes}
    extra = ids_by_name.get("fn-extra")
    text_fn = ids_by_name.get("fn-text")
    check(extra is not None and "extra_field" in extra.get("func", ""),
          "extra-field trigger publishes a record with an extra_field value")
    check(text_fn is not None and "not json" in text_fn.get("func", "")
          and "JSON.stringify" not in text_fn.get("func", ""),
          "non-JSON trigger publishes a plain string payload that is not JSON")
    check(ids_by_name.get("fn-bad", {}).get("func", "").count("roll: 'six'") == 1,
          "type-mismatch trigger sends roll as a string")

    config = (ROOT / "config.example.env").read_text()
    check("PORT=" in config and "PORT=1880" not in config,
          "config.example.env uses a high host port, not 1880")

    settings = SETTINGS.read_text()
    check('uiHost: process.env.UI_HOST || "0.0.0.0"' in settings,
          "settings.js listens on all container interfaces (published on loopback by run-docker.sh)")
    check("process.env.PORT" in settings, "settings.js takes the port from PORT")
    check("mqttReconnectTime: 2147483647" in settings,
          "settings.js disables the MQTT client's own reconnect (it would reuse the cached token)")
    script = (ROOT / "scripts" / "run-docker.sh").read_text()
    check('-p "127.0.0.1:${PORT}:${PORT}"' in script,
          "run-docker.sh publishes the editor port on the host's loopback only")
    check(not any("--network host" in line for line in script.splitlines() if not line.lstrip().startswith("#")),
          "run-docker.sh does not use host networking (-p would be ignored and the editor exposed)")

    if failures:
        print("%d check(s) failed" % len(failures))
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
