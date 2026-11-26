"""Offline tests: the agent runs with the scripted model and emits OpenInference spans.

No Collector, no network, no LLM key. Spans go to an in-memory exporter.
"""

import sys
from pathlib import Path

import pytest
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent import ask, build_agent  # noqa: E402
from telemetry import instrument  # noqa: E402


@pytest.fixture(scope="module")
def answer_and_spans():
    memory = InMemorySpanExporter()
    provider = trace_sdk.TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory))
    instrument(provider)
    answer = ask(build_agent(), "What is the weather in Oslo?")
    provider.force_flush()
    yield answer, memory.get_finished_spans()
    provider.shutdown()


def test_agent_answers_with_tool_result(answer_and_spans):
    answer, _ = answer_and_spans
    assert "Oslo" in answer and "sunny" in answer


def test_every_span_has_an_openinference_kind(answer_and_spans):
    _, spans = answer_and_spans
    assert spans, "no spans were exported"
    kinds = {s.attributes.get("openinference.span.kind") for s in spans}
    assert None not in kinds
    assert {"LLM", "TOOL"} <= kinds


def test_tool_span_records_the_tool_name_and_input(answer_and_spans):
    _, spans = answer_and_spans
    tools = [s for s in spans if s.attributes.get("openinference.span.kind") == "TOOL"]
    assert any(s.name == "get_weather" for s in tools)
    assert any("Oslo" in str(s.attributes.get("input.value", "")) for s in tools)
