"""Injected faults: rules that fire at a protocol point for the next N matching calls, or until cleared. No clock."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Rule:
    scenario: str
    points: Tuple[str, ...]
    action: str
    params: Dict[str, Any]
    table: str
    after: int
    times: Optional[int]
    matched: int = 0
    fired: int = 0

    @property
    def spent(self) -> bool:
        return self.times is not None and self.fired >= self.times


@dataclass
class FaultRegistry:
    once: List[Rule] = field(default_factory=list)  # fail_next; spent rules are dropped
    held: List[Rule] = field(default_factory=list)  # set_scenario; until cleared

    def add(self, rule: Rule, *, held: bool) -> None:
        (self.held if held else self.once).append(rule)

    def fire(self, point: str, table: str) -> List[Rule]:
        """The rules that fire for this call. Every matching rule counts the call."""
        out: List[Rule] = []
        for rule in self.held + self.once:
            if point in rule.points and rule.table in ("*", table):
                rule.matched += 1
                if rule.matched > rule.after and not rule.spent:
                    rule.fired += 1
                    out.append(rule)
        self.once = [r for r in self.once if not r.spent]
        return out

    def clear(self, *, held_only: bool = False) -> None:
        self.held = []
        if not held_only:
            self.once = []
