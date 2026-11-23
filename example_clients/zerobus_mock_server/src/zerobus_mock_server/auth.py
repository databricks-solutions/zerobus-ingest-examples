"""OAuth token issuing and checking for the mock.

The token endpoint follows the shape in the Zerobus REST docs: client
credentials (Basic auth or form fields), ``scope=all-apis``, a ``resource`` of
``api://databricks/workspaces/<workspace-id>/zerobusDirectWriteApi``, and an
``authorization_details`` JSON array that names one table. The mock checks
that the table exists and that the privileges are SELECT and/or MODIFY. It
doesn't check real grants: any client is treated as granted, unless a
permission fault is injected.

Tokens are opaque strings bound to one table and expire after
``token_ttl_seconds`` (the docs say one hour).
"""

from __future__ import annotations

import base64
import binascii
import json
import re
import secrets
import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

RESOURCE_RE = re.compile(r"^api://databricks/workspaces/([^/]+)/zerobusDirectWriteApi$")
ALLOWED_TABLE_PRIVILEGES = {"SELECT", "MODIFY"}


class TokenProblem(Exception):
    """A token request is refused. ``status``/``error`` follow the OAuth error shape."""

    def __init__(self, status: int, error: str, description: str):
        super().__init__(description)
        self.status = status
        self.error = error
        self.description = description


@dataclass(frozen=True)
class Grant:
    client_id: str
    table: str
    workspace_id: str
    expires_at: float


def parse_basic(header: Optional[str]) -> Optional[Tuple[str, str]]:
    if not header or not header.lower().startswith("basic "):
        return None
    try:
        raw = base64.b64decode(header.split(" ", 1)[1].strip(), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, ValueError):
        return None
    if ":" not in raw:
        return None
    cid, secret = raw.split(":", 1)
    return cid, secret


def parse_authorization_details(raw: str, known_tables: Iterable[str]) -> str:
    """Return the one table named by ``authorization_details``. Raises TokenProblem."""
    known = set(known_tables)

    def bad(msg: str) -> TokenProblem:
        return TokenProblem(401, "invalid_authorization_details", msg)

    try:
        entries = json.loads(raw)
    except (TypeError, ValueError):
        raise bad("authorization_details is not JSON")
    if not isinstance(entries, list) or not entries:
        raise bad("authorization_details must be a non-empty JSON array")
    tables = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("type") != "unity_catalog_privileges":
            raise bad("each authorization_details entry needs type unity_catalog_privileges")
        object_type = entry.get("object_type")
        path = entry.get("object_full_path")
        privileges = entry.get("privileges")
        if object_type not in ("CATALOG", "SCHEMA", "TABLE") or not isinstance(path, str):
            raise bad("object_type must be CATALOG, SCHEMA or TABLE with an object_full_path")
        if not isinstance(privileges, list) or not privileges:
            raise bad("privileges must be a non-empty list")
        if object_type == "TABLE":
            extra = {str(p).upper() for p in privileges} - ALLOWED_TABLE_PRIVILEGES
            if extra:
                raise bad(
                    "table privileges other than SELECT and MODIFY are not accepted: "
                    + ", ".join(sorted(extra))
                )
            tables.append(path)
    if len(tables) != 1:
        raise bad("authorization_details must name exactly one TABLE")
    table = tables[0]
    if table not in known:
        raise bad(f"table {table} is not known to the mock")
    return table


class TokenStore:
    def __init__(self, ttl_seconds: int, now=time.time):
        self.ttl = ttl_seconds
        self._now = now
        self._grants: Dict[str, Grant] = {}
        self._lock = threading.Lock()

    def issue(self, client_id: str, table: str, workspace_id: str) -> Tuple[str, int]:
        token = "mock_" + secrets.token_urlsafe(24)
        with self._lock:
            self._grants[token] = Grant(client_id, table, workspace_id, self._now() + self.ttl)
        return token, self.ttl

    def check(self, token: Optional[str], table: str) -> Optional[str]:
        """Return None when ``token`` may write ``table``; otherwise an error code.

        ``UNAUTHENTICATED`` covers a missing, unknown or expired token.
        ``PERMISSION_DENIED`` covers a token issued for a different table.
        """
        if not token:
            return "UNAUTHENTICATED"
        with self._lock:
            grant = self._grants.get(token)
        if grant is None or grant.expires_at <= self._now():
            return "UNAUTHENTICATED"
        if grant.table != table:
            return "PERMISSION_DENIED"
        return None

    def clear(self) -> None:
        with self._lock:
            self._grants.clear()
