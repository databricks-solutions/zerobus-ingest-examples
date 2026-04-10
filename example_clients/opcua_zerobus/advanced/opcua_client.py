"""
OPC UA Client — polls tags defined in tags_config.yaml and publishes wide JSON
records (one per production line) to RabbitMQ.

Tag node IDs follow the KEPServerEX convention:
    ns={namespace};s={device_path}.{line}.{tag_name}

Resilience features:
  - Exponential backoff with jitter on RabbitMQ reconnection
  - Publisher confirms (basic_publish raises on broker rejection)
  - Disk-based JSONL backup when RabbitMQ is unreachable
  - Automatic drain of backup file on reconnect
  - OPC UA and RabbitMQ failures are isolated from each other
"""

import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import datetime, timezone
from math import floor
from pathlib import Path

import pika
import pika.exceptions
import yaml
from asyncua import Client, ua
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

_REQUIRED_ENV = [
    "OPCUA_URL",
    "RABBITMQ_HOST",
    "RABBITMQ_QUEUE_NAME",
]

_missing = [k for k in _REQUIRED_ENV if not os.getenv(k, "").strip()]
if _missing:
    sys.exit(f"Missing required env vars: {', '.join(_missing)}")

OPCUA_URL = os.getenv("OPCUA_URL", "opc.tcp://localhost:4840/freeopcua/server/")
POLL_INTERVAL = float(os.getenv("OPCUA_POLL_INTERVAL", "5"))
OPCUA_NAMESPACE = int(os.getenv("OPCUA_NAMESPACE", "2"))
_SCRIPT_DIR = Path(__file__).resolve().parent
_tags_raw = os.getenv("TAGS_CONFIG_FILE", "../tags_config.yaml")
TAGS_CONFIG = str(_SCRIPT_DIR / _tags_raw) if not os.path.isabs(_tags_raw) else _tags_raw

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBIT_USER = os.getenv("RABBITMQ_USERNAME", "guest")
RABBIT_PASS = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_NAME", "opcua_data")
MESSAGE_TTL = int(os.getenv("RABBITMQ_MESSAGE_TTL", "3600000"))

_backup_raw = os.getenv("BACKUP_PATH", "backup_opcua.jsonl")
BACKUP_PATH = str(_SCRIPT_DIR / _backup_raw) if not os.path.isabs(_backup_raw) else _backup_raw

OPCUA_SECURITY_POLICY = os.getenv("OPCUA_SECURITY_POLICY", "").strip()
OPCUA_CERT_PATH = os.getenv("OPCUA_CERT_PATH", "").strip()
OPCUA_KEY_PATH = os.getenv("OPCUA_KEY_PATH", "").strip()

BACKOFF_BASE = 1
BACKOFF_MAX = 60

NAMESPACE_URI = "http://example.com/opcua/zerobus-demo"

logger = logging.getLogger(__name__)


def load_tags_config(path: str) -> dict:
    """Load tag definitions from YAML configuration file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def connect_rabbitmq() -> tuple[pika.BlockingConnection, pika.channel.Channel]:
    """Connect to RabbitMQ and declare the durable queue."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
        heartbeat=0,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={"x-message-ttl": MESSAGE_TTL},
    )
    channel.confirm_delivery()
    logger.info("Connected to RabbitMQ at %s:%s  queue=%s", RABBIT_HOST, RABBIT_PORT, QUEUE_NAME)
    return connection, channel


async def connect_rabbitmq_with_backoff():
    """Retry connect_rabbitmq with exponential backoff + jitter."""
    attempt = 0
    while True:
        try:
            return connect_rabbitmq()
        except pika.exceptions.AMQPConnectionError as exc:
            attempt += 1
            wait = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
            wait *= 0.75 + random.random() * 0.5
            logger.warning(
                "RabbitMQ connect failed (attempt %d): %s — retrying in %.1fs",
                attempt,
                exc,
                wait,
            )
            await asyncio.sleep(wait)


def publish_records(channel: pika.channel.Channel, records: list[dict]) -> None:
    """Publish all records to RabbitMQ. Raises on connection/channel failure."""
    for record in records:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(record),
            properties=pika.BasicProperties(delivery_mode=2),
        )


def save_to_backup(record: dict) -> None:
    """Append a record to the JSONL backup file."""
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


def drain_backup(channel: pika.channel.Channel) -> int:
    """Publish all records from the backup file into RabbitMQ, then truncate."""
    if not backup_has_records():
        return 0

    drained = 0
    try:
        with open(BACKUP_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=line,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                drained += 1

        with open(BACKUP_PATH, "w", encoding="utf-8") as f:
            f.truncate(0)

        logger.info("Drained %d backup records into RabbitMQ", drained)
    except pika.exceptions.AMQPConnectionError:
        logger.warning("RabbitMQ lost during backup drain after %d records", drained)
        raise
    except OSError as exc:
        logger.error("Error reading backup file: %s", exc)

    return drained


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


async def main():
    config = load_tags_config(TAGS_CONFIG)
    total_tags = sum(len(lc["tags"]) for lc in config["lines"].values())
    logger.info(
        "Loaded %d tags across %d lines from %s",
        total_tags,
        len(config["lines"]),
        TAGS_CONFIG,
    )

    connection, channel = await connect_rabbitmq_with_backoff()
    rabbitmq_ok = True

    if backup_has_records():
        logger.info("Found leftover backup file from previous run — draining...")
        drain_backup(channel)

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

                    if not rabbitmq_ok:
                        try:
                            connection, channel = connect_rabbitmq()
                            drain_backup(channel)
                            rabbitmq_ok = True
                        except (KeyboardInterrupt, SystemExit):
                            raise
                        except (
                            pika.exceptions.AMQPConnectionError,
                            pika.exceptions.AMQPChannelError,
                        ):
                            for r in records:
                                save_to_backup(r)
                            await asyncio.sleep(POLL_INTERVAL)
                            continue

                    try:
                        publish_records(channel, records)
                        logger.info(
                            "Published %d records (%s)",
                            len(records),
                            ", ".join(r["line"] for r in records),
                        )
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except (
                        pika.exceptions.AMQPConnectionError,
                        pika.exceptions.AMQPChannelError,
                    ):
                        logger.warning(
                            "RabbitMQ publish failed — saving %d records to backup", len(records)
                        )
                        rabbitmq_ok = False
                        for r in records:
                            save_to_backup(r)

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
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    logging.getLogger("asyncua").setLevel(logging.WARNING)
    signal.signal(signal.SIGTERM, shutdown_handler)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
