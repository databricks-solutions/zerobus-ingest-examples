"""LlamaIndex RAG app that sends its OpenTelemetry spans to an OTLP endpoint.

The app builds a small vector index and runs query-engine calls. It uses LlamaIndex's
MockLLM and MockEmbedding, so it needs no model API key. The spans come from
llama-index-observability-otel and go out over OTLP/HTTP to the OpenTelemetry Collector,
which forwards them to Zerobus Ingest.
"""
import argparse
import os
import time

from llama_index.core import Document, Settings, VectorStoreIndex
from llama_index.core.embeddings import MockEmbedding
from llama_index.core.llms import MockLLM
from llama_index.observability.otel import LlamaIndexOpenTelemetry
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

DEFAULT_ENDPOINT = "http://127.0.0.1:4318/v1/traces"
DOCS = [
    "Zerobus Ingest accepts records over a streaming API and writes them to Delta tables in Unity Catalog.",
    "The OpenTelemetry Collector can forward traces, logs, and metrics to Zerobus Ingest with the oauth2client extension.",
    "LlamaIndex builds a vector index from documents, then answers questions with a query engine.",
    "A query engine embeds the question, retrieves the closest nodes, and sends them to the LLM.",
]
QUESTIONS = [
    "What does Zerobus Ingest write to?",
    "How does the Collector reach Zerobus Ingest?",
    "How does a query engine answer a question?",
]


def configure_tracing(span_exporter=None, service_name="llamaindex-rag-demo"):
    """Register LlamaIndex's OpenTelemetry handlers. Call once per process."""
    if span_exporter is None:
        span_exporter = OTLPSpanExporter(endpoint=os.environ.get("OTLP_TRACES_ENDPOINT", DEFAULT_ENDPOINT))
    instrumentor = LlamaIndexOpenTelemetry(
        service_name_or_resource=os.environ.get("OTEL_SERVICE_NAME", service_name),
        span_exporter=span_exporter,
    )
    instrumentor.start_registering()
    return instrumentor


def build_query_engine():
    """Mock LLM and mock embedding model: no keys, no network calls to a model provider."""
    Settings.llm = MockLLM(max_tokens=64)
    Settings.embed_model = MockEmbedding(embed_dim=256)
    index = VectorStoreIndex.from_documents([Document(text=t) for t in DOCS])
    return index.as_query_engine(similarity_top_k=2)


def flush_tracing():
    provider = trace.get_tracer_provider()
    if hasattr(provider, "force_flush"):
        provider.force_flush()
        provider.shutdown()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--queries", type=int, default=3, help="query rounds to run")
    parser.add_argument("--pause", type=float, default=0.0, help="seconds between rounds")
    args = parser.parse_args()

    configure_tracing()
    engine = build_query_engine()
    for i in range(args.queries):
        question = QUESTIONS[i % len(QUESTIONS)]
        response = engine.query(question)
        print(f"round={i + 1} question={question!r} answer_chars={len(str(response))}", flush=True)
        if args.pause and i + 1 < args.queries:
            time.sleep(args.pause)
    flush_tracing()
    print(f"done rounds={args.queries}", flush=True)


if __name__ == "__main__":
    main()
