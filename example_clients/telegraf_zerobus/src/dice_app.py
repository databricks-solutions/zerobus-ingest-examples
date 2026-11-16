"""Dice-roll service for the Telegraf Zerobus example.

Each GET /roll rolls a six-sided die and writes one point, `dice_roll,... value=<n>i`, in InfluxDB
line protocol to Telegraf's http_listener_v2 input. Telegraf sends the point on through its
OpenTelemetry output to the Zerobus Ingest OTLP metrics endpoint.
"""

import os
import random
import socket

import httpx
from fastapi import FastAPI, HTTPException, Query

TELEGRAF_URL = os.getenv("TELEGRAF_URL", "http://127.0.0.1:8186/telegraf")
APP_HOST = os.getenv("DICE_APP_HOST", socket.gethostname())

app = FastAPI(title="Dice roll", description="Rolls a six-sided die and reports each roll to Telegraf.")


def escape_tag(value: str) -> str:
    """Escape a tag key or value for line protocol: commas, spaces and equals signs."""
    return value.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")


def line_protocol(player: str, value: int, host: str = APP_HOST) -> str:
    """One roll as a line-protocol point: measurement dice_roll, tags host and player, integer field value."""
    return f"dice_roll,host={escape_tag(host)},player={escape_tag(player)} value={value}i"


async def post_to_telegraf(body: str) -> int:
    """POST one line-protocol body to Telegraf's HTTP listener and return the HTTP status."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(TELEGRAF_URL, content=body)
    return response.status_code


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/roll")
async def roll(player: str = Query("anonymous", pattern=r"^[A-Za-z0-9_-]{1,40}$")) -> dict:
    value = random.randint(1, 6)
    try:
        status = await post_to_telegraf(line_protocol(player, value))
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=f"Telegraf not reachable: {exc.__class__.__name__}") from exc
    if status != 204:
        raise HTTPException(status_code=502, detail=f"Telegraf answered HTTP {status}")
    return {"player": player, "value": value}
