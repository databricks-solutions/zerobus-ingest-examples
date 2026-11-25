"""OAuth tokens: one table per token, checked on every authenticated call. Tokens expire by clock or by the expiry budget."""

from __future__ import annotations

import json
import secrets
import time
from typing import Dict, Iterable, Optional

PRIVILEGES = {"SELECT", "MODIFY"}


def table_from_details(raw: str, known: Iterable[str]) -> str:
    """The one TABLE named by authorization_details. Raises ValueError with the reason."""
    try:
        entries = json.loads(raw)
    except (TypeError, ValueError):
        raise ValueError("authorization_details is not JSON")
    if not isinstance(entries, list) or not entries or not all(isinstance(e, dict) for e in entries):
        raise ValueError("authorization_details must be a non-empty JSON array of objects")
    tables = [e.get("object_full_path") for e in entries if e.get("object_type") == "TABLE"]
    privileges = [{str(p).upper() for p in e.get("privileges") or []} for e in entries if e.get("object_type") == "TABLE"]
    if len(tables) != 1:
        raise ValueError("authorization_details must name exactly one TABLE")
    if not privileges[0] or privileges[0] - PRIVILEGES:
        raise ValueError("table privileges other than SELECT and MODIFY are not accepted")
    if tables[0] not in set(known):
        raise ValueError(f"table {tables[0]} is not known to the mock")
    return tables[0]


TTL_SECONDS = 3600  # the docs say tokens last one hour


class TokenStore:
    def __init__(self) -> None:
        self._grants: Dict[str, tuple] = {}  # token -> (table, expires_at)
        self._expire_budget = 0

    def issue(self, table: str) -> str:
        token = "mock_" + secrets.token_urlsafe(24)
        self._grants[token] = (table, time.time() + TTL_SECONDS)
        return token

    def expire_next(self, count: int = 1) -> None:
        """The next ``count`` checks of issued tokens find the token expired."""
        self._expire_budget += count

    def check(self, token: Optional[str], table: str) -> Optional[str]:
        """None when the token may write the table; else UNAUTHENTICATED or PERMISSION_DENIED."""
        grant = self._grants.get(token or "")
        if grant is None:
            return "UNAUTHENTICATED"
        if self._expire_budget > 0:
            self._expire_budget -= 1
            return "UNAUTHENTICATED"
        if grant[1] <= time.time():
            return "UNAUTHENTICATED"
        return None if grant[0] == table else "PERMISSION_DENIED"

    def clear(self) -> None:
        self._grants = {}
        self._expire_budget = 0
