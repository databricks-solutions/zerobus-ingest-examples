"""An in-process fake of the Zerobus Python SDK's entry point and stream, for unit tests.

    from zerobus_mock_lib import FakeZerobusSdk, FakeMock
    mock = FakeMock()
    sdk = FakeZerobusSdk(mock)
    stream = sdk.create_stream("id", "secret", table_properties)
    mock.fail_next("stream-abort-fatal", after=2)

Method names follow zerobus.sdk.sync (create_stream, ingest_record_offset, ingest_records_offset,
flush, wait_for_offset, get_unacked_records, close). Nothing is sent anywhere. The caller sees the
outcome in scenarios.json, not the SDK's retry loop.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:  # the real exception type, when the SDK is installed; otherwise a local one with the same name
    from zerobus.sdk.shared import ZerobusException  # type: ignore
except Exception:  # pragma: no cover - depends on the environment
    class ZerobusException(Exception):
        pass

# A copy of ../scenarios.json. python/tests/test_fake.py checks it against that file.
SCENARIOS: Dict[str, Dict[str, Any]] = {
    "auth-failure": {"where": "create", "message": "invalid_client", "retried_by_sdk": False},
    "table-not-found": {"where": "create", "message": "cannot be found", "retried_by_sdk": False},
    "schema-mismatch": {"where": "ingest", "message": "does not fit", "retried_by_sdk": False,
                        "earlier_records_stored": True},
    "record-too-large": {"where": "ingest", "message": "maximum message size", "retried_by_sdk": False},
    "server-unavailable": {"where": "create", "message": "UNAVAILABLE", "retried_by_sdk": True},
    "stream-abort-retryable": {"where": None, "message": None, "retried_by_sdk": True,
                               "duplicates_possible": True},
    "stream-abort-fatal": {"where": "ingest", "message": "terminal stream error", "retried_by_sdk": False,
                           "earlier_records_stored": True},
    "graceful-close": {"where": None, "message": None, "retried_by_sdk": True},
    "no-acks": {"where": "flush", "message": "timed out", "retried_by_sdk": False},
    "schema-change-while-open": {"where": None, "message": None, "retried_by_sdk": False,
                                 "rows_go_to": "fallback"},
    "token-expired": {"where": "ingest", "message": "expired", "retried_by_sdk": False,
                      "note": "at create the SDK refreshes the token and succeeds"},
}
TOKEN_EXPIRED = "token-expired"


def _require(scenario: str) -> None:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}")


def _raise(scenario: str, detail: str = "") -> None:
    message = SCENARIOS[scenario]["message"] or scenario
    raise ZerobusException(f"{message}{': ' + detail if detail else ''}")


class FakeMock:
    """Holds the scenario controls. Same names as the server's control API."""

    def __init__(self) -> None:
        self._fail_next: Dict[str, Dict[str, int]] = {}  # name -> {"after": n, "left": k}
        self._held: Optional[str] = None
        self._expire = 0

    def fail_next(self, scenario: str, *, count: int = 1, after: int = 0) -> None:
        _require(scenario)
        if count < 1 or after < 0:
            raise ValueError("count must be >= 1 and after >= 0")
        if scenario == TOKEN_EXPIRED:
            if after > 0:
                raise ValueError("token-expired takes no after; use expire_token")
            self._expire += count
            return
        self._fail_next[scenario] = {"after": after, "left": count}

    def set_scenario(self, scenario: str) -> None:
        _require(scenario)
        if scenario == TOKEN_EXPIRED:
            raise ValueError("token-expired is one-shot: use fail_next or expire_token")
        self._held = scenario

    def clear_scenario(self) -> None:
        self._held = None

    def expire_token(self, count: int = 1) -> None:
        self._expire += count

    def reset(self) -> None:
        self._fail_next.clear()
        self._held = None
        self._expire = 0

    def _take(self, name: str) -> bool:
        """True when a fail-next budget fires on this matching call."""
        entry = self._fail_next.get(name)
        if entry is None:
            return False
        if entry["after"] > 0:
            entry["after"] -= 1
            return False
        if entry["left"] <= 0:
            return False
        entry["left"] -= 1
        return True

    def fire(self, name: str) -> bool:
        """A held scenario fires on every matching call; otherwise a fail-next budget may."""
        return self.active(name) or self._take(name)

    def active(self, name: str) -> bool:
        return self._held == name

    def take_expiry(self) -> bool:
        if self._expire > 0:
            self._expire -= 1
            return True
        return False


class FakeStream:
    """ingested: every record the stream accepted (a fallback row included). stored: what landed in the
    table, so a retried abort shows up twice. fallback: rows routed away by schema-change-while-open.
    A record is acked when ingested, unless no-acks is held."""

    def __init__(self, mock: FakeMock, table: str):
        self._mock = mock
        self.table = table
        self.ingested: List[Any] = []
        self.stored: List[Any] = []
        self.fallback: List[Any] = []
        self._offset = -1
        self._acked = -1  # highest offset acknowledged; a hold on no-acks stops this advancing
        self._closed = False
        self._terminal: Optional[str] = None  # scenario that failed the stream for good

    def _check_terminal(self) -> None:
        if self._terminal is not None:
            _raise(self._terminal)

    # --- ingest
    def _ingest_one(self, record: Any) -> int:
        if self._closed:
            raise ZerobusException("stream is closed")
        self._check_terminal()
        if self._mock.take_expiry():
            self._terminal = TOKEN_EXPIRED
            _raise(TOKEN_EXPIRED)
        for name in ("schema-mismatch", "record-too-large"):
            if self._mock.fire(name):
                _raise(name)
        if self._mock.fire("stream-abort-fatal"):
            self._terminal = "stream-abort-fatal"
            _raise(self._terminal)
        self.ingested.append(record)
        self._offset += 1
        if not self._mock.active("no-acks"):
            self._acked = self._offset
        if self._mock.fire("schema-change-while-open"):
            self.fallback.append(record)
            return self._offset
        self.stored.append(record)
        if self._mock.fire("stream-abort-retryable"):
            self.stored.append(record)  # the aborted copy was stored unacked; the SDK's resend stores it again
        return self._offset

    def ingest_record_offset(self, record: Any) -> int:
        return self._ingest_one(record)

    def ingest_records_offset(self, records: List[Any]) -> int:
        last = -1
        for r in records:
            last = self._ingest_one(r)
        return last

    def wait_for_offset(self, offset: int) -> None:
        self._check_terminal()
        if self._mock._take("no-acks") or (self._mock.active("no-acks") and offset > self._acked):
            _raise("no-acks", "offset %d not acknowledged" % offset)
        self._acked = max(self._acked, offset)

    def flush(self) -> None:
        self._check_terminal()
        if self._mock._take("no-acks") or (self._mock.active("no-acks") and self._acked < self._offset):
            _raise("no-acks", "flush")
        self._acked = self._offset

    def get_unacked_records(self) -> List[Any]:
        return list(self.ingested[self._acked + 1:])

    def close(self) -> None:
        self._closed = True


class FakeZerobusSdk:
    """Same entry point as zerobus.sdk.sync.ZerobusSdk.create_stream(client_id, client_secret, table_properties, options)."""

    def __init__(self, mock: Optional[FakeMock] = None):
        self.mock = mock or FakeMock()

    def create_stream(self, client_id: str = None, client_secret: str = None, table_properties: Any = None,
                      options: Any = None) -> FakeStream:
        table = getattr(table_properties, "table_name", table_properties)
        for name in ("auth-failure", "table-not-found", "server-unavailable"):
            if self.mock.fire(name):
                _raise(name)
        self.mock.take_expiry()  # at create the SDK refreshes the token and retries: the caller sees success
        return FakeStream(self.mock, str(table))
