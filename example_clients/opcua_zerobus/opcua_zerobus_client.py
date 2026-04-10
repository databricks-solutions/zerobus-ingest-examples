"""
OPC UA Zerobus Client — polls OPC UA tags and ingests directly to Databricks via Zerobus.

This is the simple, single-process architecture:
  OPC UA Server → [this client] → Zerobus → Delta Lake

Tag node IDs follow the KEPServerEX convention:
    ns={namespace};s={device_path}.{line}.{tag_name}

Resilience features:
  - Batching with configurable size and timeout
  - Disk-based JSONL backup when Zerobus is unreachable
  - Automatic drain of backup file on reconnect
  - Recovery-enabled Zerobus stream with retries
  - Graceful shutdown on SIGTERM
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from math import floor
from pathlib import Path

import yaml
from asyncua import Client, ua
from dotenv import load_dotenv
from zerobus.sdk.sync import (
    ZerobusSdk,
    TableProperties,
    StreamConfigurationOptions,
    RecordType,
    ZerobusException,
)

load_dotenv(Path(__file__).resolve().parent / ".env")

_REQUIRED_ENV = [
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_INGEST_ENDPOINT",
    "DATABRICKS_WORKSPACE_URL",
    "DATABRICKS_TABLE_NAME",
    "OPCUA_URL",
]

_missing = [k for k in _REQUIRED_ENV if not os.getenv(k, "").strip()]
if _missing:
    sys.exit(f"Missing required env vars: {', '.join(_missing)}")

OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/freeopcua/server/")
POLL_INTERVAL = float(os.getenv("OPCUA_POLL_INTERVAL", "5"))
OPCUA_NAMESPACE = int(os.getenv("OPCUA_NAMESPACE", "2"))
_SCRIPT_DIR = Path(__file__).resolve().parent
_tags_raw = os.getenv("TAGS_CONFIG_FILE", "tags_config.yaml")
TAGS_CONFIG = str(_SCRIPT_DIR / _tags_raw) if not os.path.isabs(_tags_raw) else _tags_raw

_raw_host = os.getenv("DATABRICKS_INGEST_ENDPOINT", "")
ZEROBUS_HOST = _raw_host if _raw_host.startswith("https://") else f"https://{_raw_host}"
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "")
TABLE_NAME = os.getenv("DATABRICKS_TABLE_NAME", "")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_TIMEOUT_SEC = float(os.getenv("BATCH_TIMEOUT", "2.0"))
MAX_STREAM_RETRIES = 3
RETRY_BACKOFF_SEC = 5

OPCUA_SECURITY_POLICY = os.getenv("OPCUA_SECURITY_POLICY", "").strip()
OPCUA_CERT_PATH = os.getenv("OPCUA_CERT_PATH", "").strip()
OPCUA_KEY_PATH = os.getenv("OPCUA_KEY_PATH", "").strip()

_backup_raw = os.getenv("BACKUP_PATH", "backup_opcua.jsonl")
BACKUP_PATH = str(_SCRIPT_DIR / _backup_raw) if not os.path.isabs(_backup_raw) else _backup_raw

NAMESPACE_URI = "http://example.com/opcua/zerobus-demo"

logger = logging.getLogger(__name__)


def load_tags_config(path: str) -> dict:
    """Load tag definitions from YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_zerobus_stream():
    """Initialize Zerobus SDK with recovery-enabled configuration."""
    logger.info(
        "Initializing Zerobus SDK\n"
        "  host           = %s\n"
        "  workspace_url  = %s\n"
        "  table          = %s\n"
        "  client_id      = %s",
        ZEROBUS_HOST,
        WORKSPACE_URL,
        TABLE_NAME,
        CLIENT_ID,
    )
    sdk = ZerobusSdk(host=ZEROBUS_HOST, unity_catalog_url=WORKSPACE_URL)

    props = TableProperties(table_name=TABLE_NAME)
    options = StreamConfigurationOptions(
        record_type=RecordType.JSON,
        recovery=True,
        recovery_retries=5,
        recovery_timeout_ms=15000,
        recovery_backoff_ms=5000,
        server_lack_of_ack_timeout_ms=180000,
        max_inflight_records=50000,
        flush_timeout_ms=300000,
    )

    stream = sdk.create_stream(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        table_properties=props,
        options=options,
    )
    logger.info("Zerobus stream created successfully with recovery enabled")
    return sdk, stream


def resolve_line_nodes(client: Client, config: dict, ns: int):
    """Build a list of (line_name, tag_names, node_objects) from YAML config.

    Uses direct node ID lookup (no browsing), matching KEPServerEX convention.
    Returns a list of tuples: (line_name, [tag_name, ...], [node, ...])
    """
    device_path = config["opcua"]["device_path"]
    lines_resolved = []

    for line_name, line_cfg in config["lines"].items():
        tag_names = []
        nodes = []
        for tag_def in line_cfg["tags"]:
            tag_name = tag_def["name"]
            node_id = f"ns={ns};s={device_path}.{line_name}.{tag_name}"
            node = client.get_node(node_id)
            tag_names.append(tag_name)
            nodes.append(node)
        lines_resolved.append((line_name, tag_names, nodes))

    return lines_resolved


async def poll_lines(client: Client, lines_resolved: list) -> list[dict]:
    """Batch-read all tags per line using a single OPC UA ReadRequest per line.

    Returns one wide dict per line. Tags whose StatusCode is not Good are
    set to None so a single offline sensor never blocks the rest of the line.
    """
    records = []
    ts = floor(datetime.now(timezone.utc).timestamp() * 1_000_000)

    for line_name, tag_names, nodes in lines_resolved:
        dvs = await client.read_attributes(nodes, ua.AttributeIds.Value)
        record = {"line": line_name}
        for name, dv in zip(tag_names, dvs):
            record[name] = dv.Value.Value if dv.StatusCode.is_good() else None
        record["ts"] = ts
        records.append(record)

    return records


def save_to_backup(record: dict) -> None:
    """Append a record to the JSONL backup file for later retry."""
    try:
        with open(BACKUP_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as exc:
        logger.error("Failed to write backup record: %s", exc)


def backup_has_records() -> bool:
    """Check if backup file exists and has content."""
    try:
        return os.path.isfile(BACKUP_PATH) and os.path.getsize(BACKUP_PATH) > 0
    except OSError:
        return False


def drain_backup(sdk, stream) -> tuple[int, object]:
    """Ingest all records from the backup file into Zerobus, then truncate.

    Returns (count_drained, possibly_new_stream).
    """
    if not backup_has_records():
        return 0, stream

    drained = 0
    payloads = []
    try:
        with open(BACKUP_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                payloads.append(line)
                drained += 1

        if payloads:
            stream, _ = ingest_batch_with_retry(sdk, stream, payloads)

        with open(BACKUP_PATH, "w", encoding="utf-8") as f:
            f.truncate(0)

        logger.info("Drained %d backup records into Zerobus", drained)
    except ZerobusException:
        logger.warning("Zerobus unavailable during backup drain after %d records", drained)
        raise
    except OSError as exc:
        logger.error("Error reading backup file: %s", exc)

    return drained, stream


def ingest_batch_with_retry(sdk, stream, payloads: list[str]):
    """Batch-ingest payloads with retry + stream recreation on failure."""
    last_error = None
    for attempt in range(1, MAX_STREAM_RETRIES + 1):
        try:
            offset = stream.ingest_records_offset(payloads)
            if offset is not None:
                stream.wait_for_offset(offset)
            return stream, offset
        except ZerobusException as exc:
            last_error = exc
            logger.warning(
                "Batch ingest failed (attempt %d/%d): %s — recreating stream in %ds...",
                attempt,
                MAX_STREAM_RETRIES,
                exc,
                RETRY_BACKOFF_SEC,
            )
            time.sleep(RETRY_BACKOFF_SEC)
            try:
                stream = sdk.recreate_stream(stream)
                logger.info("Stream recreated")
            except ZerobusException as recreate_exc:
                logger.error("Stream recreation failed: %s", recreate_exc)
    raise last_error


async def main():
    config = load_tags_config(TAGS_CONFIG)
    total_tags = sum(len(lc["tags"]) for lc in config["lines"].values())
    logger.info(
        "Loaded %d tags across %d lines from %s",
        total_tags,
        len(config["lines"]),
        TAGS_CONFIG,
    )

    sdk, stream = create_zerobus_stream()
    zerobus_ok = True

    if backup_has_records():
        logger.info("Found leftover backup file from previous run — draining...")
        try:
            _, stream = drain_backup(sdk, stream)
        except ZerobusException:
            logger.warning("Could not drain backup — will retry later")
            zerobus_ok = False

    batch_buffer = []
    batch_start = time.monotonic()

    while True:
        try:
            client = Client(url=OPCUA_URL)
            if OPCUA_SECURITY_POLICY:
                sec = f"{OPCUA_SECURITY_POLICY},{OPCUA_CERT_PATH},{OPCUA_KEY_PATH}"
                await client.set_security_string(sec)
                logger.info("OPC UA security enabled: %s", OPCUA_SECURITY_POLICY)
            async with client:
                logger.info("Connected to OPC UA server at %s", OPCUA_URL)

                lines_resolved = resolve_line_nodes(client, config, OPCUA_NAMESPACE)
                line_names = [lr[0] for lr in lines_resolved]
                logger.info(
                    "Resolved %d tags — polling lines %s every %ds",
                    total_tags,
                    ", ".join(line_names),
                    POLL_INTERVAL,
                )

                while True:
                    records = await poll_lines(client, lines_resolved)

                    for record in records:
                        batch_buffer.append(json.dumps(record))

                    batch_elapsed = time.monotonic() - batch_start
                    should_flush = (
                        len(batch_buffer) >= BATCH_SIZE or batch_elapsed >= BATCH_TIMEOUT_SEC
                    )

                    if should_flush and batch_buffer:
                        if not zerobus_ok:
                            try:
                                sdk, stream = create_zerobus_stream()
                                _, stream = drain_backup(sdk, stream)
                                zerobus_ok = True
                            except (KeyboardInterrupt, SystemExit):
                                raise
                            except Exception:
                                for payload in batch_buffer:
                                    save_to_backup(json.loads(payload))
                                batch_buffer.clear()
                                batch_start = time.monotonic()
                                await asyncio.sleep(POLL_INTERVAL)
                                continue

                        try:
                            stream, offset = ingest_batch_with_retry(sdk, stream, batch_buffer)
                            logger.info(
                                "Batch ingested  count=%d  offset=%s",
                                len(batch_buffer),
                                offset,
                            )
                        except (KeyboardInterrupt, SystemExit):
                            raise
                        except ZerobusException:
                            logger.warning(
                                "Zerobus ingest failed — saving %d records to backup",
                                len(batch_buffer),
                            )
                            zerobus_ok = False
                            for payload in batch_buffer:
                                save_to_backup(json.loads(payload))

                        batch_buffer.clear()
                        batch_start = time.monotonic()

                    await asyncio.sleep(POLL_INTERVAL)

        except (KeyboardInterrupt, SystemExit):
            raise
        except (OSError, ConnectionError) as exc:
            logger.warning(
                "OPC UA server unreachable (%s) — retrying in %ds...", exc, POLL_INTERVAL
            )
            await asyncio.sleep(POLL_INTERVAL)
        except Exception:
            logger.exception("Unexpected OPC UA error — retrying in %ds...", POLL_INTERVAL)
            await asyncio.sleep(POLL_INTERVAL)


def shutdown_handler(signum, frame):
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
