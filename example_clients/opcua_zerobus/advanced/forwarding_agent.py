"""
Forwarding Agent — drains RabbitMQ into Zerobus Ingest for Delta materialization.

Consumes messages in batches from RabbitMQ, ingests them via the Zerobus batch API,
waits for a single durability ack per batch, then acks all messages at once.

Resilience features:
  - Exponential backoff with jitter on RabbitMQ reconnection
  - Automatic reconnect when RabbitMQ connection drops mid-operation
  - Batch ingest retry with Zerobus stream recreation on failure
  - Recovery-enabled Zerobus stream configuration
  - Unacked messages are auto-requeued by RabbitMQ on disconnect (no data loss)
"""

import logging
import os
import random
import signal
import sys
import time
from pathlib import Path

import pika
import pika.exceptions
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
    "RABBITMQ_HOST",
    "RABBITMQ_QUEUE_NAME",
]

_missing = [k for k in _REQUIRED_ENV if not os.getenv(k, "").strip()]
if _missing:
    sys.exit(f"Missing required env vars: {', '.join(_missing)}")

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBIT_USER = os.getenv("RABBITMQ_USERNAME", "guest")
RABBIT_PASS = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE_NAME", "opcua_data")
MESSAGE_TTL = int(os.getenv("RABBITMQ_MESSAGE_TTL", "3600000"))

_raw_host = os.getenv("DATABRICKS_INGEST_ENDPOINT", "")
ZEROBUS_HOST = _raw_host if _raw_host.startswith("https://") else f"https://{_raw_host}"
WORKSPACE_URL = os.getenv("DATABRICKS_WORKSPACE_URL", "")
TABLE_NAME = os.getenv("DATABRICKS_TABLE_NAME", "")
CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET", "")

BATCH_SIZE = int(os.getenv("FORWARDING_BATCH_SIZE", "50"))
BATCH_TIMEOUT_SEC = float(os.getenv("FORWARDING_BATCH_TIMEOUT", "2.0"))
MAX_STREAM_RETRIES = 3
RETRY_BACKOFF_SEC = 5

BACKOFF_BASE = 1
BACKOFF_MAX = 60

logger = logging.getLogger(__name__)


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


def connect_rabbitmq():
    """Connect to RabbitMQ and declare the durable queue."""
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True,
        arguments={"x-message-ttl": MESSAGE_TTL},
    )
    logger.info("Connected to RabbitMQ at %s:%s  queue=%s", RABBIT_HOST, RABBIT_PORT, QUEUE_NAME)
    return connection, channel


def connect_rabbitmq_with_backoff():
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
            time.sleep(wait)


def drain_batch(channel):
    """Pull up to BATCH_SIZE messages from RabbitMQ, or stop after BATCH_TIMEOUT_SEC."""
    messages = []
    deadline = time.monotonic() + BATCH_TIMEOUT_SEC

    while len(messages) < BATCH_SIZE and time.monotonic() < deadline:
        method, properties, body = channel.basic_get(queue=QUEUE_NAME, auto_ack=False)
        if method is None:
            if not messages:
                time.sleep(0.1)
            else:
                break
        else:
            messages.append((method.delivery_tag, body))

    return messages


def ingest_batch_with_retry(sdk, stream, payloads):
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


def main():
    sdk, stream = create_zerobus_stream()

    connection, channel = connect_rabbitmq_with_backoff()
    channel.basic_qos(prefetch_count=BATCH_SIZE)

    logger.info(
        "Forwarding agent running — batch_size=%d, batch_timeout=%.1fs",
        BATCH_SIZE,
        BATCH_TIMEOUT_SEC,
    )

    try:
        while True:
            try:
                messages = drain_batch(channel)
                if not messages:
                    continue

                payloads = []
                for _, body in messages:
                    payloads.append(body if isinstance(body, str) else body.decode("utf-8"))

                try:
                    stream, offset = ingest_batch_with_retry(sdk, stream, payloads)

                    last_tag = messages[-1][0]
                    channel.basic_ack(delivery_tag=last_tag, multiple=True)
                    logger.info(
                        "Batch ingested & acked  count=%d  offset=%s",
                        len(messages),
                        offset,
                    )

                except ZerobusException:
                    logger.exception(
                        "Failed to ingest batch of %d after %d retries — nacking all",
                        len(messages),
                        MAX_STREAM_RETRIES,
                    )
                    last_tag = messages[-1][0]
                    channel.basic_nack(delivery_tag=last_tag, multiple=True, requeue=True)
                    logger.info("Pausing 30s before next batch...")
                    time.sleep(30)

            except (
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.AMQPChannelError,
            ) as exc:
                logger.warning("RabbitMQ connection lost: %s — reconnecting...", exc)
                connection, channel = connect_rabbitmq_with_backoff()
                channel.basic_qos(prefetch_count=BATCH_SIZE)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
    finally:
        try:
            stream.flush()
        except Exception:
            logger.warning("stream.flush() failed during shutdown", exc_info=True)
        try:
            stream.close()
        except Exception:
            logger.warning("stream.close() failed during shutdown", exc_info=True)
        try:
            connection.close()
        except Exception:
            pass
        logger.info("Cleanup complete")


def shutdown_handler(signum, frame):
    logger.info("Received shutdown signal, exiting...")
    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    signal.signal(signal.SIGTERM, shutdown_handler)
    main()
