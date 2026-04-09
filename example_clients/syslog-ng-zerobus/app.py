"""
Flask Health Monitor — syslog-ng + Zerobus Ingest OTLP example

A simple application health monitor that emits structured syslog messages.
syslog-ng reads from the Unix socket this app writes to, maps the syslog
fields to OpenTelemetry log attributes, and forwards them to Zerobus Ingest
via OTLP/gRPC — landing the data in a Unity Catalog Delta table.

Endpoints:
  GET  /health          — Returns service health status. Logs at INFO.
  GET  /metrics         — Returns current metrics snapshot. Logs at INFO.
  POST /simulate/error  — Simulates a failure event. Logs at ERROR + WARN.

A background thread emits a health log every 10 seconds and occasionally
simulates latency degradation at WARN level.

Usage:
  python app.py
"""

import logging
import logging.handlers
import random
import threading
import time

from flask import Flask, jsonify

# ── App setup ──────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ── Logging: write to the Unix socket syslog-ng listens on ────────────────────
SYSLOG_SOCKET = "/tmp/flask-app.sock"

logger = logging.getLogger("health-monitor")
logger.setLevel(logging.DEBUG)

try:
    syslog_handler = logging.handlers.SysLogHandler(address=SYSLOG_SOCKET)
    syslog_handler.setFormatter(
        logging.Formatter("%(name)s[%(process)d]: %(levelname)s %(message)s")
    )
    logger.addHandler(syslog_handler)
except (OSError, ConnectionRefusedError):
    # Fallback to stdout if syslog-ng isn't running yet
    fallback = logging.StreamHandler()
    fallback.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(fallback)
    logger.warning(
        f"Could not connect to {SYSLOG_SOCKET} — is syslog-ng running? "
        "Falling back to stdout."
    )

# ── In-memory state ────────────────────────────────────────────────────────────
_state = {
    "requests": 0,
    "errors": 0,
    "avg_latency_ms": 45,
    "memory_usage_pct": 62,
}
_lock = threading.Lock()


def get_state():
    with _lock:
        return dict(_state)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/health")
def health():
    with _lock:
        _state["requests"] += 1
        snap = dict(_state)

    logger.info(
        f"health_check status=ok "
        f"memory_pct={snap['memory_usage_pct']} "
        f"latency_ms={snap['avg_latency_ms']} "
        f"requests={snap['requests']}"
    )
    return jsonify({"status": "ok", **snap})


@app.route("/metrics")
def metrics():
    with _lock:
        _state["requests"] += 1
        snap = dict(_state)

    logger.info(
        f"metrics_polled "
        f"requests={snap['requests']} "
        f"errors={snap['errors']} "
        f"latency_ms={snap['avg_latency_ms']} "
        f"memory_pct={snap['memory_usage_pct']}"
    )
    return jsonify(snap)


@app.route("/simulate/error", methods=["POST"])
def simulate_error():
    with _lock:
        _state["errors"] += 1
        _state["avg_latency_ms"] = random.randint(800, 2000)
        _state["memory_usage_pct"] = random.randint(85, 99)
        snap = dict(_state)

    logger.error(
        f"simulated_failure "
        f"latency_ms={snap['avg_latency_ms']} "
        f"memory_pct={snap['memory_usage_pct']} "
        f"error_count={snap['errors']}"
    )
    logger.warning(
        f"memory_threshold_exceeded "
        f"memory_pct={snap['memory_usage_pct']} threshold=85"
    )
    return jsonify({"status": "error simulated", **snap}), 500


# ── Background monitor ─────────────────────────────────────────────────────────

def background_monitor():
    """Emit periodic health logs. 20% chance of simulating latency degradation."""
    while True:
        time.sleep(10)
        with _lock:
            if random.random() < 0.2:
                _state["avg_latency_ms"] = random.randint(300, 700)
                snap = dict(_state)
                degraded = True
            else:
                _state["avg_latency_ms"] = random.randint(30, 80)
                snap = dict(_state)
                degraded = False

        if degraded:
            logger.warning(
                f"latency_degradation_detected "
                f"latency_ms={snap['avg_latency_ms']} "
                f"memory_pct={snap['memory_usage_pct']}"
            )
        else:
            logger.info(
                f"system_healthy "
                f"latency_ms={snap['avg_latency_ms']} "
                f"memory_pct={snap['memory_usage_pct']}"
            )


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    monitor = threading.Thread(target=background_monitor, daemon=True)
    monitor.start()
    logger.info("health_monitor_starting host=localhost port=5000")
    app.run(host="localhost", port=5000, debug=False)
