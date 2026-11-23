"""Helpers for driving the real Python SDK against the mock."""

import threading
from typing import Iterable, Optional

from zerobus.sdk.shared import TableProperties
from zerobus.sdk.sync import ZerobusSdk


def sdk_for(mock) -> ZerobusSdk:
    return ZerobusSdk(mock.grpc_endpoint, mock.unity_catalog_url)


def create(mock, table: str, descriptor: Optional[bytes] = None):
    props = TableProperties(table, descriptor) if descriptor is not None else TableProperties(table)
    return sdk_for(mock).create_stream("mock-client-id", "mock-client-secret", props)


def drive(stream, records: Iterable, *, close: bool = True):
    """Ingest every record, flush, close. Returns the first exception raised, or None."""
    try:
        for rec in records:
            stream.ingest_record_offset(rec)
        stream.flush()
    except Exception as exc:  # noqa: BLE001 - the tests inspect the SDK's error
        if close:
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


def run_in_thread(fn):
    """Run ``fn`` on a thread and return (thread, box). ``box['exc']`` holds any exception."""
    box = {}

    def target():
        try:
            box["result"] = fn()
        except Exception as exc:  # noqa: BLE001
            box["exc"] = exc

    t = threading.Thread(target=target, daemon=True)
    t.start()
    return t, box
