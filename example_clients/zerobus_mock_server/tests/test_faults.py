"""Scenario mechanics without a server: which calls fire, how many, and what a bad request is refused for."""

import pytest

from zerobus_mock_server.auth import TokenStore
from zerobus_mock_server.scenarios import ScenarioError, ScenarioManager, UnknownScenario


def _manager():
    return ScenarioManager(TokenStore())


def test_fail_next_fires_count_matching_calls_after_skipping_after():
    sm = _manager()
    sm.fail_next("table-not-found", count=2, after=1)
    fired = [bool(sm.fire("create", "t.s.x")) for _ in range(4)]
    assert fired == [False, True, True, False]


def test_set_scenario_fires_every_matching_call_until_cleared():
    sm = _manager()
    sm.set_scenario("server-unavailable")
    assert [r.action for r in sm.fire("token", "t.s.x")] == ["http_503"]
    sm.clear_scenario()
    assert sm.fire("token", "t.s.x") == []


def test_table_scope_and_params_are_applied_to_the_rule():
    sm = _manager()
    sm.fail_next("stream-abort-fatal", table="t.s.one")
    assert sm.fire("ingest", "t.s.two") == []
    (rule,) = sm.fire("ingest", "t.s.one")
    assert rule.action == "abort" and rule.params == {"retryable": False, "persist": False}
    sm.set_scenario("graceful-close", params={"duration_ms": 50})
    assert sm.fire("ingest", "t.s.one")[0].params == {"duration_ms": 50}


def test_bad_scenario_requests_are_refused():
    sm = _manager()
    with pytest.raises(UnknownScenario):
        sm.fail_next("no-such-scenario")
    with pytest.raises(ScenarioError):
        sm.set_scenario("token-expired")
    with pytest.raises(ScenarioError):
        sm.fail_next("auth-failure", count=0)
    with pytest.raises(ScenarioError):
        sm.set_scenario("graceful-close", params={"persist": True})
    with pytest.raises(ScenarioError):
        sm.set_scenario("graceful-close", params={"duration_ms": -1})
