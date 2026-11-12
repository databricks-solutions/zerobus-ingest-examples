#!/bin/sh
# Vector `exec` secrets backend (protocol v1): answers from the container's environment.
# Vector 0.57.0 and later don't interpolate ${VAR} in config files by default, so the
# config reads these values as SECRET[env.<key>] instead.
# Vector writes {"version":"1.0","secrets":[...]} to stdin; we answer every known key.
cat >/dev/null
printf '{"zerobus_endpoint":{"value":"%s","error":null},"workspace_url":{"value":"%s","error":null},"client_id":{"value":"%s","error":null},"client_secret":{"value":"%s","error":null}}\n' \
  "$ZEROBUS_SERVER_ENDPOINT" "$DATABRICKS_WORKSPACE_URL" "$DATABRICKS_CLIENT_ID" "$DATABRICKS_CLIENT_SECRET"
