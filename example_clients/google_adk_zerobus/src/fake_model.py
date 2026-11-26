"""A deterministic stand-in for an LLM, so the example runs with no API key.

The model asks for one tool call (roll_dice) on the first turn and writes a
short answer once it has the tool result. It makes no network calls.
"""

from __future__ import annotations

import random
from typing import AsyncGenerator

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.genai import types


def roll_dice(sides: int = 6) -> dict:
  """Rolls one die with the given number of sides.

  Args:
    sides: The number of sides on the die.

  Returns:
    A dict with the die size and the roll result.
  """
  return {"sides": sides, "result": random.randint(1, sides)}


def _usage(prompt: int, output: int) -> types.GenerateContentResponseUsageMetadata:
  """Fixed token counts, so the spans carry usage attributes. These are not real."""
  return types.GenerateContentResponseUsageMetadata(
      prompt_token_count=prompt,
      candidates_token_count=output,
      total_token_count=prompt + output,
  )


class FakeDiceModel(BaseLlm):
  """Returns one tool call, then a text answer built from the tool result."""

  model: str = "fake-dice-model"

  @classmethod
  def supported_models(cls) -> list[str]:
    # Not registered with ADK's model registry: it is passed to the agent directly.
    return []

  async def generate_content_async(
      self, llm_request: LlmRequest, stream: bool = False
  ) -> AsyncGenerator[LlmResponse, None]:
    last = llm_request.contents[-1] if llm_request.contents else None
    responses = [
        part.function_response
        for part in (last.parts or [])
        if part.function_response
    ] if last else []

    if responses:
      result = responses[0].response or {}
      text = (
          f"Rolled a d{result.get('sides')} and got {result.get('result')}."
      )
      yield LlmResponse(
          content=types.Content(role="model", parts=[types.Part(text=text)]),
          usage_metadata=_usage(prompt=24, output=9),
      )
      return

    yield LlmResponse(
        content=types.Content(
            role="model",
            parts=[
                types.Part(
                    function_call=types.FunctionCall(
                        name="roll_dice", args={"sides": 6}
                    )
                )
            ],
        ),
        usage_metadata=_usage(prompt=18, output=4),
    )
