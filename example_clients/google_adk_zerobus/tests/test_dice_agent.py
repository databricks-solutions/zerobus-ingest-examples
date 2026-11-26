"""Offline tests: the agent runs with the fake model and ADK emits its own spans.

No network access and no Databricks credentials are needed.
"""

import asyncio

from fake_model import FakeDiceModel
from fake_model import roll_dice
from google.adk.models.llm_request import LlmRequest
from google.genai import types

import dice_agent


def _run(span_exporter, runs=1):
  runner = dice_agent.build_runner()
  for _ in range(runs):
    asyncio.run(dice_agent.run_once(runner, player="test", sides=6))
  return [span.name for span in span_exporter.get_finished_spans()]


def test_fake_model_asks_for_roll_then_answers():
  model = FakeDiceModel(model="fake-dice-model")
  first = LlmRequest(
      model="fake-dice-model",
      contents=[types.Content(role="user", parts=[types.Part(text="Roll a d6.")])],
  )

  async def collect(request):
    return [r async for r in model.generate_content_async(request)]

  (first_response,) = asyncio.run(collect(first))
  call = first_response.content.parts[0].function_call
  assert call.name == "roll_dice"
  assert call.args == {"sides": 6}

  second = LlmRequest(
      model="fake-dice-model",
      contents=[
          types.Content(role="user", parts=[types.Part(text="Roll a d6.")]),
          types.Content(
              role="user",
              parts=[
                  types.Part(
                      function_response=types.FunctionResponse(
                          name="roll_dice", response={"sides": 6, "result": 4}
                      )
                  )
              ],
          ),
      ],
  )
  (second_response,) = asyncio.run(collect(second))
  assert second_response.content.parts[0].text == "Rolled a d6 and got 4."


def test_roll_dice_stays_in_range():
  for _ in range(100):
    result = roll_dice(sides=6)
    assert 1 <= result["result"] <= 6
    assert result["sides"] == 6


def test_one_invocation_emits_adk_spans(span_exporter):
  names = _run(span_exporter, runs=1)
  assert "invoke_agent dice_agent" in names
  assert "execute_tool roll_dice" in names
  assert "generate_content fake-dice-model" in names
  assert names.count("generate_content fake-dice-model") == 2


def test_span_attributes_use_genai_conventions(span_exporter):
  _run(span_exporter, runs=1)
  spans = {s.name: s for s in span_exporter.get_finished_spans()}
  tool = spans["execute_tool roll_dice"]
  assert tool.attributes["gen_ai.operation.name"] == "execute_tool"
  assert tool.attributes["gen_ai.tool.name"] == "roll_dice"
  agent = spans["invoke_agent dice_agent"]
  assert agent.attributes["gen_ai.operation.name"] == "invoke_agent"
  assert agent.attributes["gen_ai.agent.name"] == "dice_agent"
