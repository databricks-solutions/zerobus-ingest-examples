"""Dice-roll demo: rolls dice for a few players and publishes each roll as one JSON record.

Run it with the settings in config.example.env (exported as environment variables).
"""

import argparse
import logging
import random
import signal
import threading
import time
import uuid

from zerobus_mqtt import Config, ZerobusMqttPublisher

PLAYERS = ["ada", "grace", "linus"]


def roll_event(rng: random.Random, sides: int = 6, dice: int = 2) -> dict:
    """One roll of `dice` dice with `sides` faces, shaped like the target table."""
    return {
        "roll_id": str(uuid.uuid4()),
        "player": rng.choice(PLAYERS),
        "sides": sides,
        "dice": dice,
        "total": sum(rng.randint(1, sides) for _ in range(dice)),
    }


def invalid_records(rng: random.Random) -> list[dict]:
    """Two records the table can't take as-is. Each has a roll_id, so its row can be found in `rescue`."""
    good = roll_event(rng)
    type_mismatch = {**good, "roll_id": f"invalid-type-{uuid.uuid4()}", "sides": "six"}
    extra_field = {**good, "roll_id": f"invalid-extra-{uuid.uuid4()}", "extra_field": "hello"}
    return [type_mismatch, extra_field]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rolls", type=int, default=20, help="number of rolls to publish")
    parser.add_argument("--interval", type=float, default=0.5, help="seconds between rolls")
    parser.add_argument("--force-reconnect-after", type=int, default=0,
                        help="drop the connection after this many rolls and reconnect with a new token")
    parser.add_argument("--send-invalid-record", action="store_true",
                        help="also send three bad records: a type mismatch, an extra field, and a non-JSON payload. "
                             "With the rescue column, the first two land in rescue; the third gets PUBACK 0x99.")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--verbose", action="store_true", help="log each PUBLISH and PUBACK (no tokens)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.verbose:
        logging.getLogger("zerobus_mqtt").setLevel(logging.DEBUG)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    rng = random.Random(args.seed)
    publisher = ZerobusMqttPublisher(Config.from_env())
    publisher.connect()

    for i in range(1, args.rolls + 1):
        if stop.is_set():
            break
        if args.force_reconnect_after and i == args.force_reconnect_after + 1:
            publisher.reconnect("demo: forced reconnect")
        publisher.submit(roll_event(rng))
        time.sleep(args.interval)
        publisher.ensure_connected()
        publisher.pump()
    if args.send_invalid_record:
        for record in invalid_records(rng):
            publisher.submit(record)
            publisher.ensure_connected()
            publisher.pump()
        publisher.submit_raw(
            f"not json {uuid.uuid4()} ((".encode("utf-8"),
            {"roll_id": "invalid-non-json"},
        )

    clean = publisher.close()
    logging.info("connections=%d durable=%d failed=%d rejected=%d clean_shutdown=%s",
                 publisher.connect_count, publisher.durable_count, publisher.failed_count,
                 publisher.rejected_count, clean)
    # Counts are exact. Only the most recent few records that weren't durable are listed.
    for record, reason in list(publisher.recent_failed) + list(publisher.recent_rejected):
        logging.error("not durable: roll_id=%s reason=%s", record.get("roll_id"), reason)
    return 0 if not publisher.failed_count and not publisher.rejected_count and clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
