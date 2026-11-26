"""Runs a Google ADK agent in a loop and lets ADK export its own OpenTelemetry spans.

ADK creates the invoke_agent, generate_content and execute_tool spans itself.
maybe_set_otel_providers() reads OTEL_EXPORTER_OTLP_TRACES_ENDPOINT and sends
the spans over OTLP/HTTP to the local OpenTelemetry Collector.
"""

from __future__ import annotations

import argparse
import asyncio
import uuid

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.telemetry.setup import maybe_set_otel_providers
from google.genai import types

from fake_model import FakeDiceModel
from fake_model import roll_dice

APP_NAME = "dice_app"


def build_runner() -> Runner:
  agent = LlmAgent(
      name="dice_agent",
      model=FakeDiceModel(model="fake-dice-model"),
      instruction="Roll dice when the user asks, then report the result.",
      tools=[roll_dice],
  )
  return Runner(
      app_name=APP_NAME,
      agent=agent,
      session_service=InMemorySessionService(),
  )


async def run_once(runner: Runner, player: str, sides: int) -> str:
  user_id = player
  session_id = str(uuid.uuid4())
  await runner.session_service.create_session(
      app_name=APP_NAME, user_id=user_id, session_id=session_id
  )
  message = types.Content(
      role="user",
      parts=[types.Part(text=f"Roll a d{sides} for {player}.")],
  )
  answer = ""
  async for event in runner.run_async(
      user_id=user_id, session_id=session_id, new_message=message
  ):
    if event.content and event.content.parts:
      for part in event.content.parts:
        if part.text and event.author == "dice_agent":
          answer = part.text
  return answer


async def main_async(runs: int, interval: float, player: str) -> None:
  runner = build_runner()
  for i in range(runs):
    answer = await run_once(runner, player, sides=6)
    print(f"run {i + 1}/{runs}: {answer}", flush=True)
    if interval and i + 1 < runs:
      await asyncio.sleep(interval)


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--runs", type=int, default=5, help="agent invocations to send")
  parser.add_argument("--interval", type=float, default=1.0, help="seconds between runs")
  parser.add_argument("--player", default="demo", help="player name in the prompt")
  args = parser.parse_args()

  # Reads OTEL_EXPORTER_OTLP_TRACES_ENDPOINT and OTEL_SERVICE_NAME. Call it
  # once, before the first agent run.
  maybe_set_otel_providers()
  asyncio.run(main_async(args.runs, args.interval, args.player))


if __name__ == "__main__":
  main()
