"""Numeric edge compression utilities (NONE/CHANGE_ONLY/DEADBAND/SDT).

Implements PI-like Swinging Door Trending (SDT) semantics for numeric streams:
- Emit first point.
- Maintain door slope bounds from last emitted anchor with +/- deviation.
- Emit previous point when door closes.
- Optional max-interval force emit.
- Optional min-interval suppression.
- Out-of-order timestamps reset state and emit current.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Dict, List, Optional, Tuple


class NumericCompressionMode(str, Enum):
    NONE = "none"
    CHANGE_ONLY = "change_only"
    DEADBAND = "deadband"
    SDT = "sdt"


@dataclass
class CompressionRule:
    tag_path_regex: str
    mode: Optional[NumericCompressionMode] = None
    deadband: Optional[float] = None
    sdt_deviation: Optional[float] = None
    sdt_max_interval_ms: Optional[int] = None
    sdt_min_interval_ms: Optional[int] = None
    change_only: Optional[bool] = None
    _compiled: Optional[re.Pattern[str]] = None

    def matches(self, tag_path: str) -> bool:
        if not self.tag_path_regex:
            return False
        if self._compiled is None:
            self._compiled = re.compile(self.tag_path_regex)
        return bool(self._compiled.match(tag_path))


@dataclass
class CompressionPolicy:
    mode: NumericCompressionMode = NumericCompressionMode.NONE
    change_only: bool = False
    deadband: float = 0.0
    sdt_deviation: float = 0.0
    sdt_max_interval_ms: int = 0
    sdt_min_interval_ms: int = 0


@dataclass
class SdtOutcome:
    emit_payload: Optional[Dict[str, Any]]
    filtered: bool
    forced_by_max_interval: bool
    reset_due_to_out_of_order: bool


class SdtState:
    """Per-tag SDT state machine."""

    def __init__(self) -> None:
        self.initialized = False
        self.anchor_time_ms = 0
        self.anchor_value = 0.0

        self.prev_time_ms = 0
        self.prev_value = 0.0
        self.prev_payload: Optional[Dict[str, Any]] = None

        self.lower_slope = float("-inf")
        self.upper_slope = float("inf")
        self.last_emit_time_ms = 0

    def offer(
        self,
        payload: Dict[str, Any],
        t_ms: int,
        numeric_value: float,
        deviation: float,
        max_interval_ms: int,
        min_interval_ms: int,
    ) -> SdtOutcome:
        # First point emits and initializes state.
        if not self.initialized:
            self.initialized = True
            self.anchor_time_ms = t_ms
            self.anchor_value = numeric_value
            self.prev_time_ms = t_ms
            self.prev_value = numeric_value
            self.prev_payload = dict(payload)
            self.lower_slope = float("-inf")
            self.upper_slope = float("inf")
            self.last_emit_time_ms = t_ms
            return SdtOutcome(dict(payload), filtered=False, forced_by_max_interval=False, reset_due_to_out_of_order=False)

        # Out-of-order reset behavior.
        if t_ms <= self.prev_time_ms:
            self._reset_with_current(payload, t_ms, numeric_value)
            return SdtOutcome(dict(payload), filtered=False, forced_by_max_interval=False, reset_due_to_out_of_order=True)

        # CompMin style suppression.
        if min_interval_ms > 0 and (t_ms - self.last_emit_time_ms) < min_interval_ms:
            self.prev_time_ms = t_ms
            self.prev_value = numeric_value
            self.prev_payload = dict(payload)
            return SdtOutcome(None, filtered=True, forced_by_max_interval=False, reset_due_to_out_of_order=False)

        # Max interval forced emit.
        if max_interval_ms > 0 and (t_ms - self.last_emit_time_ms) >= max_interval_ms:
            self._reset_with_current(payload, t_ms, numeric_value)
            return SdtOutcome(dict(payload), filtered=False, forced_by_max_interval=True, reset_due_to_out_of_order=False)

        dt = t_ms - self.anchor_time_ms
        if dt <= 0:
            self._reset_with_current(payload, t_ms, numeric_value)
            return SdtOutcome(dict(payload), filtered=False, forced_by_max_interval=False, reset_due_to_out_of_order=True)

        s_low = (numeric_value - self.anchor_value - deviation) / float(dt)
        s_high = (numeric_value - self.anchor_value + deviation) / float(dt)
        self.lower_slope = max(self.lower_slope, s_low)
        self.upper_slope = min(self.upper_slope, s_high)

        # Door closes -> emit previous point.
        if self.lower_slope > self.upper_slope:
            closing = dict(self.prev_payload or payload)
            closing["event_time"] = int(self.prev_time_ms * 1000)

            # Reset door at closing pivot.
            self.anchor_time_ms = self.prev_time_ms
            self.anchor_value = self.prev_value
            self.lower_slope = float("-inf")
            self.upper_slope = float("inf")
            self.last_emit_time_ms = self.prev_time_ms

            # Incorporate current into new door.
            self.prev_time_ms = t_ms
            self.prev_value = numeric_value
            self.prev_payload = dict(payload)

            dt2 = t_ms - self.anchor_time_ms
            if dt2 > 0:
                s_low2 = (numeric_value - self.anchor_value - deviation) / float(dt2)
                s_high2 = (numeric_value - self.anchor_value + deviation) / float(dt2)
                self.lower_slope = max(self.lower_slope, s_low2)
                self.upper_slope = min(self.upper_slope, s_high2)

            return SdtOutcome(closing, filtered=False, forced_by_max_interval=False, reset_due_to_out_of_order=False)

        # No emit.
        self.prev_time_ms = t_ms
        self.prev_value = numeric_value
        self.prev_payload = dict(payload)
        return SdtOutcome(None, filtered=True, forced_by_max_interval=False, reset_due_to_out_of_order=False)

    def _reset_with_current(self, payload: Dict[str, Any], t_ms: int, numeric_value: float) -> None:
        self.anchor_time_ms = t_ms
        self.anchor_value = numeric_value
        self.prev_time_ms = t_ms
        self.prev_value = numeric_value
        self.prev_payload = dict(payload)
        self.lower_slope = float("-inf")
        self.upper_slope = float("inf")
        self.last_emit_time_ms = t_ms


def parse_policy_dict(cfg: Optional[Dict[str, Any]]) -> CompressionPolicy:
    cfg = cfg or {}
    mode_raw = str(cfg.get("mode", "none")).strip().lower()
    try:
        mode = NumericCompressionMode(mode_raw)
    except ValueError:
        mode = NumericCompressionMode.NONE

    return CompressionPolicy(
        mode=mode,
        change_only=bool(cfg.get("change_only", False)),
        deadband=float(cfg.get("deadband", 0.0) or 0.0),
        sdt_deviation=float(cfg.get("sdt_deviation", 0.0) or 0.0),
        sdt_max_interval_ms=int(cfg.get("sdt_max_interval_ms", 0) or 0),
        sdt_min_interval_ms=int(cfg.get("sdt_min_interval_ms", 0) or 0),
    )


def parse_rules(cfg: Optional[Dict[str, Any]]) -> List[CompressionRule]:
    rules_cfg = (cfg or {}).get("rules") or []
    out: List[CompressionRule] = []
    for r in rules_cfg:
        if not isinstance(r, dict):
            continue
        mode_raw = r.get("mode")
        mode = None
        if mode_raw is not None:
            try:
                mode = NumericCompressionMode(str(mode_raw).strip().lower())
            except ValueError:
                mode = None
        try:
            rule = CompressionRule(
                tag_path_regex=str(r.get("tag_path_regex", "")),
                mode=mode,
                deadband=float(r["deadband"]) if "deadband" in r and r["deadband"] is not None else None,
                sdt_deviation=float(r["sdt_deviation"]) if "sdt_deviation" in r and r["sdt_deviation"] is not None else None,
                sdt_max_interval_ms=int(r["sdt_max_interval_ms"]) if "sdt_max_interval_ms" in r and r["sdt_max_interval_ms"] is not None else None,
                sdt_min_interval_ms=int(r["sdt_min_interval_ms"]) if "sdt_min_interval_ms" in r and r["sdt_min_interval_ms"] is not None else None,
                change_only=bool(r["change_only"]) if "change_only" in r and r["change_only"] is not None else None,
            )
            # validate regex compilation now
            if rule.tag_path_regex:
                rule._compiled = re.compile(rule.tag_path_regex)
            out.append(rule)
        except Exception:
            # skip invalid rules silently; caller can report diagnostics if needed
            continue
    return out


def apply_rule(policy: CompressionPolicy, rule: CompressionRule) -> CompressionPolicy:
    mode = rule.mode if rule.mode is not None else policy.mode
    return CompressionPolicy(
        mode=mode,
        change_only=policy.change_only if rule.change_only is None else rule.change_only,
        deadband=policy.deadband if rule.deadband is None else rule.deadband,
        sdt_deviation=policy.sdt_deviation if rule.sdt_deviation is None else rule.sdt_deviation,
        sdt_max_interval_ms=policy.sdt_max_interval_ms if rule.sdt_max_interval_ms is None else rule.sdt_max_interval_ms,
        sdt_min_interval_ms=policy.sdt_min_interval_ms if rule.sdt_min_interval_ms is None else rule.sdt_min_interval_ms,
    )


def resolve_effective_policy(
    global_policy: CompressionPolicy,
    global_rules: List[CompressionRule],
    source_policy: Optional[CompressionPolicy],
    source_rules: Optional[List[CompressionRule]],
    tag_path: str,
) -> CompressionPolicy:
    # Merge source over global first.
    p = CompressionPolicy(
        mode=source_policy.mode if source_policy is not None else global_policy.mode,
        change_only=source_policy.change_only if source_policy is not None else global_policy.change_only,
        deadband=source_policy.deadband if source_policy is not None else global_policy.deadband,
        sdt_deviation=source_policy.sdt_deviation if source_policy is not None else global_policy.sdt_deviation,
        sdt_max_interval_ms=source_policy.sdt_max_interval_ms if source_policy is not None else global_policy.sdt_max_interval_ms,
        sdt_min_interval_ms=source_policy.sdt_min_interval_ms if source_policy is not None else global_policy.sdt_min_interval_ms,
    )

    # Rule precedence: source rules first, then global rules.
    for r in (source_rules or []):
        if r.matches(tag_path):
            return apply_rule(p, r)

    for r in global_rules:
        if r.matches(tag_path):
            return apply_rule(p, r)

    return p


def is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def changed(last: Any, current: Any) -> bool:
    if last is None and current is None:
        return False
    if last is None or current is None:
        return True
    return last != current


def meaningful_numeric_change(last: Any, current: Any, deadband: float) -> bool:
    a = as_float(last)
    b = as_float(current)
    if a is None or b is None:
        return changed(last, current)
    return abs(a - b) > max(0.0, deadband)


def event_time_us_to_ms(event_time_us: Any) -> Optional[int]:
    try:
        if event_time_us is None:
            return None
        return int(int(event_time_us) / 1000)
    except (TypeError, ValueError):
        return None

