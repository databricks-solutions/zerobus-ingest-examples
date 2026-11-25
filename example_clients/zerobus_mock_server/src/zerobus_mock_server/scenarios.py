"""Named scenarios. The outcome of each is in scenarios.json (shared with the mock libraries);
this module adds the protocol point and action for each. fail_next and set_scenario start one."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .faults import FaultRegistry, Rule
from .schema import make_column

CONTRACT: Dict[str, Dict[str, Any]] = json.loads(
    (Path(__file__).with_name("scenarios.json")).read_text(encoding="utf-8"))["scenarios"]
TOKEN_EXPIRY = "token-expired"
# The non-nullable column that schema-change-while-open adds to the table.
SCHEMA_CHANGE = (make_column("region", "STRING", nullable=False),)

# Scenario -> its rules: (points, action, default params).
MECHANICS: Dict[str, List[Tuple[Tuple[str, ...], str, Dict[str, Any]]]] = {
    "auth-failure": [(("token",), "invalid_client", {})],
    "table-not-found": [(("insert", "create"), "not_found", {})],
    "schema-mismatch": [(("record",), "schema_mismatch", {})],
    "record-too-large": [(("record",), "too_large", {})],
    "server-unavailable": [(("token", "insert"), "http_503", {}), (("create",), "unavailable", {}),
                           (("ingest",), "abort", {"retryable": True, "persist": False})],
    "stream-abort-retryable": [(("ingest",), "abort", {"retryable": True, "persist": True})],
    "stream-abort-fatal": [(("ingest",), "abort", {"retryable": False, "persist": False})],
    "graceful-close": [(("ingest",), "close", {"duration_ms": 500})],
    "no-acks": [(("ack",), "drop", {})],
    "schema-change-while-open": [(("insert", "ingest"), "add_columns", {})],
    TOKEN_EXPIRY: [],
}
if set(MECHANICS) != set(CONTRACT):
    raise RuntimeError(f"scenarios.json and MECHANICS disagree: {sorted(set(MECHANICS) ^ set(CONTRACT))}")


class ScenarioError(ValueError):
    """Bad parameters for a scenario."""


class UnknownScenario(ScenarioError):
    pass


class ScenarioManager:
    def __init__(self, tokens) -> None:
        self.tokens = tokens
        self.faults = FaultRegistry()

    def fail_next(self, name: str, *, count: int = 1, after: int = 0, table: str = "*",
                  params: Optional[Dict[str, Any]] = None) -> None:
        if count < 1 or after < 0:
            raise ScenarioError("count must be >= 1 and after >= 0")
        if name == TOKEN_EXPIRY:
            self.tokens.expire_next(count)
            return
        for rule in self._rules(name, table, params, after, count):
            self.faults.add(rule, held=False)

    def set_scenario(self, name: str, *, table: str = "*", params: Optional[Dict[str, Any]] = None) -> None:
        if name == TOKEN_EXPIRY:
            raise ScenarioError("token-expired is one-shot: use fail_next or expire_token")
        rules = self._rules(name, table, params, 0, None)  # validated before the held scenario is replaced
        self.faults.clear(held_only=True)
        for rule in rules:
            self.faults.add(rule, held=True)

    def clear_scenario(self) -> None:
        self.faults.clear(held_only=True)

    def reset(self) -> None:
        self.faults.clear()

    def fire(self, point: str, table: str) -> List[Rule]:
        return self.faults.fire(point, table)

    def _rules(self, name: str, table: str, params: Optional[Dict[str, Any]], after: int,
               times: Optional[int]) -> List[Rule]:
        if name not in CONTRACT:
            raise UnknownScenario(f"unknown scenario {name!r}; known: {sorted(CONTRACT)}")
        params = dict(params or {})
        allowed = {k for _, _, defaults in MECHANICS[name] for k in defaults}
        if set(params) - allowed:
            raise ScenarioError(f"{name} takes no param(s) {sorted(set(params) - allowed)}; allowed: {sorted(allowed)}")
        if params.get("duration_ms", 0) < 0:
            raise ScenarioError("duration_ms must be a non-negative number")
        return [Rule(name, points, action, {k: params.get(k, v) for k, v in defaults.items()}, table or "*", after, times)
                for points, action, defaults in MECHANICS[name]]
