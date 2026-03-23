"""
Zerobus producer: polls the GitHub Public Events API and streams each event
into the bronze Delta table via the Databricks Zerobus Ingest SDK (JSON mode).

Run as a Databricks notebook or locally:

    pip install databricks-zerobus-ingest-sdk requests
    python push_zerobus.py
"""

import json
import logging
import time
from datetime import datetime, timezone

from utils_setup import FQN, GITHUB, ZEROBUS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

POLL_INTERVAL_SEC = 60
BATCH_SIZE = 100
RUN_DURATION_SEC = 5 * 60


def _epoch_micros(iso_str: str) -> int:
    """Convert ISO-8601 timestamp to microseconds since epoch (TIMESTAMP)."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1_000_000)


def _epoch_days(iso_str: str) -> int:
    """Convert ISO-8601 timestamp to days since epoch (DATE)."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (dt - epoch).days


def _now_micros() -> int:
    """Return current UTC time as microseconds since epoch."""
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def fetch_github_events(etag=None):
    """Fetch the latest page of public GitHub events, respecting ETags for dedup."""
    import requests
    headers = {"Accept": "application/vnd.github+json"}
    if etag:
        headers["If-None-Match"] = etag

    resp = requests.get(GITHUB["events_url"], headers=headers, timeout=30)

    if resp.status_code == 304:
        log.info("No new events (304 Not Modified)")
        return [], etag

    resp.raise_for_status()
    return resp.json(), resp.headers.get("ETag")


def build_bronze_record(event: dict) -> dict:
    """Shape a raw GitHub event into the bronze table schema."""
    created_at = event.get("created_at", datetime.now(timezone.utc).isoformat())
    return {
        "data": json.dumps(event),
        "event_time": _epoch_micros(created_at),
        "event_date": _epoch_days(created_at),
        "source": GITHUB["source"],
        "source_type": GITHUB["source_type"],
        "source_path": GITHUB["events_url"],
        "processed_time": _now_micros(),
    }


def main() -> None:
    """Poll GitHub Events API and push bronze records to Zerobus in a loop."""
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q",
                           "databricks-zerobus-ingest-sdk", "requests"])

    from zerobus.sdk.sync import ZerobusSdk
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    CLIENT_ID     = dbutils.secrets.get(scope=ZEROBUS["secrets_scope"], key="client-id")
    CLIENT_SECRET = dbutils.secrets.get(scope=ZEROBUS["secrets_scope"], key="client-secret")

    log.info("Initializing Zerobus SDK  →  %s", FQN["bronze"])

    sdk = ZerobusSdk(ZEROBUS["endpoint"], ZEROBUS["workspace_url"])
    table_props = TableProperties(FQN["bronze"])
    options = StreamConfigurationOptions(record_type=RecordType.JSON)

    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props, options)
    log.info("Zerobus stream created")

    seen_ids: set[str] = set()
    etag: str | None = None
    start_time = time.monotonic()
    total_pushed = 0

    try:
        while True:
            if RUN_DURATION_SEC is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= RUN_DURATION_SEC:
                    log.info(
                        "Run duration reached (%ds). Pushed %d events total.",
                        RUN_DURATION_SEC,
                        total_pushed,
                    )
                    break

            events, etag = fetch_github_events(etag)

            new_events = [e for e in events if e.get("id") not in seen_ids]
            if not new_events:
                log.info("Sleeping %ds …", POLL_INTERVAL_SEC)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            records = [build_bronze_record(e) for e in new_events]

            for batch_start in range(0, len(records), BATCH_SIZE):
                batch = records[batch_start : batch_start + BATCH_SIZE]
                stream.ingest_records_nowait(batch)

            stream.flush()
            seen_ids.update(e["id"] for e in new_events)
            total_pushed += len(new_events)
            log.info("Pushed %d new events (%d total)", len(new_events), total_pushed)

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        stream.close()
        log.info("Zerobus stream closed. %d events pushed.", total_pushed)


if __name__ == "__main__":
    main()
