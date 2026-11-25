"""Shared test setup: one in-memory span exporter for the whole test session.

The OpenTelemetry global tracer provider can be set only once per process, so
it is set here, before any test runs.
"""

import sys
from pathlib import Path

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

# Let the tests import the example's modules from src/.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

_exporter = InMemorySpanExporter()
_provider = TracerProvider()
_provider.add_span_processor(SimpleSpanProcessor(_exporter))
trace.set_tracer_provider(_provider)


@pytest.fixture
def span_exporter():
  _exporter.clear()
  yield _exporter
