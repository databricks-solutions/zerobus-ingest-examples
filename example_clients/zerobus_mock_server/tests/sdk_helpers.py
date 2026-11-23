"""Helpers for driving the real Python SDK against the mock."""

import threading

from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk
from zerobus_mock_server.scenarios import CONTRACT

from helpers import CLIENT_ID, CLIENT_SECRET


def contract(name: str) -> str:
    """The SDK-visible text of a scenario, from scenarios.json (the shared contract)."""
    return CONTRACT[name]["message"]


def sdk_for(mock) -> ZerobusSdk:
    return ZerobusSdk(mock.grpc_endpoint, mock.unity_catalog_url)


def create(mock, table: str, descriptor=None, options=None):
    props = TableProperties(table, descriptor) if descriptor is not None else TableProperties(table)
    args = (CLIENT_ID, CLIENT_SECRET, props) + ((options,) if options is not None else ())
    return sdk_for(mock).create_stream(*args)


def drive(stream, records, *, close: bool = True):
    """Ingests every record, flushes and closes. Returns the first exception raised, or None."""
    try:
        for rec in records:
            stream.ingest_record_offset(rec)
        stream.flush()
    except Exception as exc:  # noqa: BLE001 - the tests inspect the SDK's error
        _quietly_close(stream)
        return exc
    if close:
        try:
            stream.close()
        except Exception as exc:  # noqa: BLE001
            return exc
    return None


def _quietly_close(stream) -> None:
    try:
        stream.close()
    except Exception:  # noqa: BLE001
        pass


def create_error(mock, table: str, descriptor=None, options=None):
    """The exception that create_stream raises. Fails the test if it succeeds."""
    try:
        create(mock, table, descriptor=descriptor, options=options)
    except Exception as exc:  # noqa: BLE001
        return exc
    raise AssertionError("create_stream should have failed")


def run_in_thread(fn):
    """Runs ``fn`` on a thread. Returns (thread, box); box['exc'] holds any exception."""
    box = {}

    def target():
        try:
            box["result"] = fn()
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t, box
