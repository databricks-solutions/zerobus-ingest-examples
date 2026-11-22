"""Dice roller: a small FastAPI service that asks an LLM, through the LiteLLM Gateway, to narrate each roll.

GET /rolldice?player=<name> rolls a six-sided die and sends one chat completion to the gateway
(LITELLM_BASE_URL, default http://litellm:4000). The gateway's zerobus callback logs that request,
with the player as a request tag, to a Delta table through Zerobus Ingest.

GET /rolldice?player=<name>&fail=true calls a model that always fails, so the table also gets a
failure row.
"""
import os
import random

import httpx
from fastapi import FastAPI

GATEWAY = os.getenv("LITELLM_BASE_URL", "http://litellm:4000").rstrip("/")
API_KEY = os.environ["LITELLM_API_KEY"]
MODEL = os.getenv("NARRATOR_MODEL", "dice-narrator")
BROKEN_MODEL = os.getenv("BROKEN_MODEL", "dice-broken")

app = FastAPI(title="Dice roller")
client = httpx.Client(timeout=30)


@app.get("/rolldice")
def roll_dice(player: str = "anonymous", fail: bool = False):
    value = random.randint(1, 6)
    response = client.post(
        f"{GATEWAY}/v1/chat/completions",
        headers={"Authorization": f"Bearer {API_KEY}"},
        json={
            "model": BROKEN_MODEL if fail else MODEL,
            "messages": [{"role": "user", "content": f"Narrate a dice roll of {value} for {player}."}],
            "metadata": {"tags": [f"player:{player}", "app:dice-roller"]},
        },
    )
    if response.status_code != 200:
        return {"player": player, "value": value, "narration": None, "gateway_status": response.status_code}
    narration = response.json()["choices"][0]["message"]["content"]
    return {"player": player, "value": value, "narration": narration, "gateway_status": 200}
