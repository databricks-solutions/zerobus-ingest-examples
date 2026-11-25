"""Client for the control API. Each method is one HTTP call; nothing waits on a timer."""

from __future__ import annotations

import ssl
from typing import Any, Dict, List, Optional

import httpx


class MockControl:
    def __init__(self, base_url: str, ca_path: Optional[str] = None, timeout: float = 10.0):
        verify = ssl.create_default_context(cafile=ca_path) if ca_path else True
        self._client = httpx.Client(base_url=base_url, verify=verify, timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def _ok(self, resp: httpx.Response) -> Dict[str, Any]:
        if resp.status_code >= 400:
            raise RuntimeError(f"mock control call failed ({resp.status_code}): {resp.text}")
        return resp.json()

    def fail_next(self, scenario: str, *, count: int = 1, after: int = 0, table: str = "*", **params) -> Dict[str, Any]:
        body = {"scenario": scenario, "count": count, "after": after, "table": table, "params": params}
        return self._ok(self._client.post("/_mock/fail-next", json=body))

    def expire_token(self, count: int = 1) -> Dict[str, Any]:
        return self._ok(self._client.post("/_mock/expire-token", json={"count": count}))

    def set_scenario(self, scenario: str, *, table: str = "*", **params) -> Dict[str, Any]:
        return self._ok(self._client.post("/_mock/scenario", json={"scenario": scenario, "table": table, "params": params}))

    def clear_scenario(self) -> Dict[str, Any]:
        return self._ok(self._client.delete("/_mock/scenario"))

    def reset(self, tables: Optional[List[str]] = None, *, schema: bool = False) -> Dict[str, Any]:
        return self._ok(self._client.post("/_mock/reset", json={"tables": tables, "schema": schema}))

    def rows(self, table: str, *, limit: int = 1000, destination: str = "table") -> List[Dict[str, Any]]:
        recs = self._ok(self._client.get(f"/_mock/tables/{table}/rows",
                                         params={"limit": limit, "destination": destination}))["rows"]
        return [r["values"] for r in recs]

    def coverage(self, table: str, *, expected: int, key: str) -> Dict[str, Any]:
        return self._ok(self._client.get(f"/_mock/tables/{table}/coverage", params={"expected": expected, "key": key}))
