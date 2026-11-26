"""Offline tests: the RAG query produces a LlamaIndex span tree, with no network and no model keys."""
import sys
from pathlib import Path

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import app  # noqa: E402


@pytest.fixture(scope="module")
def exporter():
    exp = InMemorySpanExporter()
    app.configure_tracing(span_exporter=exp)
    yield exp
    app.flush_tracing()


def test_query_emits_span_tree(exporter):
    engine = app.build_query_engine()
    exporter.clear()
    engine.query(app.QUESTIONS[0])
    app.flush_tracing()
    spans = exporter.get_finished_spans()
    names = {s.name for s in spans}
    assert "RetrieverQueryEngine.query" in names
    assert "MockLLM.complete" in names
    assert "MockEmbedding.get_query_embedding" in names
    root = next(s for s in spans if s.name == "RetrieverQueryEngine.query")
    assert root.parent is None
    llm = next(s for s in spans if s.name == "MockLLM.complete")
    assert llm.parent is not None
    assert llm.context.trace_id == root.context.trace_id


def test_every_span_has_service_name(exporter):
    spans = exporter.get_finished_spans()
    assert spans, "expected spans from the query above"
    for s in spans:
        assert s.resource.attributes.get("service.name") == "llamaindex-rag-demo"
