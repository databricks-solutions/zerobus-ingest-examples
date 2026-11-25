"""The HTTP control API (fail-next, expire-token, scenario set and clear, reset, rows, coverage),
the pytest plugin's README example, and the shared scenario contract.

Every control call here is one HTTP request through the ``zerobus_mock`` client. No timers.
"""

from pathlib import Path

import pytest

from helpers import AQ, get_token
from sdk_helpers import create, drive
from zerobus_mock_server.scenarios import CONTRACT, MECHANICS


def _insert(rest, tok, rows, table=AQ):
    return rest.post(f"/zerobus/v1/tables/{table}/insert", json=rows, headers={"authorization": f"Bearer {tok}"})


def test_fail_next_fails_the_count_then_is_over(rest, zerobus_mock):
    tok = get_token(rest, AQ)
    zerobus_mock.fail_next("table-not-found", count=1)
    assert _insert(rest, tok, [{"device_name": "a"}]).status_code == 404
    assert _insert(rest, tok, [{"device_name": "b"}]).status_code == 200


def test_fail_next_after_skips_matching_calls_first(rest, zerobus_mock):
    tok = get_token(rest, AQ)
    zerobus_mock.fail_next("table-not-found", count=1, after=2)
    codes = [_insert(rest, tok, [{"device_name": str(i)}]).status_code for i in range(4)]
    assert codes == [200, 200, 404, 200]


def test_set_scenario_holds_until_cleared(rest, zerobus_mock):
    tok = get_token(rest, AQ)  # taken first: the scenario also covers the token endpoint
    zerobus_mock.set_scenario("server-unavailable")
    assert [_insert(rest, tok, [{"device_name": "x"}]).status_code for _ in range(3)] == [503, 503, 503]
    zerobus_mock.clear_scenario()
    assert _insert(rest, tok, [{"device_name": "y"}]).status_code == 200


def test_expire_token_fails_the_next_authenticated_calls_only(rest, zerobus_mock):
    tok = get_token(rest, AQ)
    zerobus_mock.expire_token(count=2)
    codes = [_insert(rest, tok, [{"device_name": n}]).status_code for n in ("a", "b", "c")]
    assert codes == [401, 401, 200]


def test_token_expired_cannot_be_held_as_a_scenario(zerobus_mock):
    with pytest.raises(RuntimeError, match="400"):
        zerobus_mock.set_scenario("token-expired")


def test_unknown_scenario_is_404(zerobus_mock):
    with pytest.raises(RuntimeError, match="404"):
        zerobus_mock.fail_next("no-such-scenario")


def test_reset_clears_held_scenarios_and_pending_expiries(rest, zerobus_mock):
    get_token(rest, AQ)
    zerobus_mock.set_scenario("table-not-found")
    zerobus_mock.expire_token(count=3)
    zerobus_mock.reset()  # also clears issued tokens, so take a new one
    assert _insert(rest, get_token(rest, AQ), [{"device_name": "after-reset"}]).status_code == 200


def test_rows_and_coverage_read_back_over_http(rest, zerobus_mock):
    tok = get_token(rest, AQ)
    assert _insert(rest, tok, [{"device_name": f"sensor-{i}"} for i in (0, 1, 3)]).status_code == 200
    cov = zerobus_mock.coverage(AQ, expected=4, key="device_name")
    assert cov["missing"] == 1 and cov["missing_keys"] == [2]
    assert [r["device_name"] for r in zerobus_mock.rows(AQ)] == ["sensor-0", "sensor-1", "sensor-3"]
    assert zerobus_mock.rows(AQ, destination="fallback") == []


def test_readme_pytest_example(zerobus_mock, zerobus_mock_server):
    """The 'Testing patterns' example in README.md, run as written."""
    zerobus_mock.fail_next("stream-abort-retryable", after=4)  # the 5th submission is aborted, once
    rows = [{"device_name": f"ex-{i}", "temp": i} for i in range(10)]
    assert drive(create(zerobus_mock_server, AQ), rows) is None
    assert zerobus_mock.coverage(AQ, expected=10, key="device_name")["missing"] == 0


def test_scenarios_json_copy_matches_the_shared_contract():
    assert set(CONTRACT) == set(MECHANICS)
    shared = Path(__file__).resolve().parents[1] / "mock_lib" / "scenarios.json"
    if not shared.exists():  # not present inside the test image
        pytest.skip("mock_lib is not in this checkout")
    copy = Path(__file__).resolve().parents[1] / "src" / "zerobus_mock_server" / "scenarios.json"
    assert copy.read_bytes() == shared.read_bytes()
