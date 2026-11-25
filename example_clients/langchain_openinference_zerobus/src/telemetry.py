"""Send OpenInference spans from LangChain to an OpenTelemetry Collector over OTLP/HTTP.

The exporter reads the standard OpenTelemetry environment variables. Set
OTEL_EXPORTER_OTLP_ENDPOINT to the Collector's HTTP base URL, for example
http://127.0.0.1:4318. The exporter adds /v1/traces. Without it, the default
http://localhost:4318 is used.
"""

import os

from openinference.instrumentation.langchain import LangChainInstrumentor
from opentelemetry import trace as trace_api
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def build_provider(service_name: str | None = None) -> trace_sdk.TracerProvider:
    """Return a tracer provider that exports every span to the Collector."""
    name = service_name or os.environ.get("OTEL_SERVICE_NAME", "langchain-agent")
    provider = trace_sdk.TracerProvider(resource=Resource.create({"service.name": name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    return provider


def instrument(provider: trace_sdk.TracerProvider) -> None:
    """Turn on OpenInference tracing for LangChain and LangGraph runs."""
    LangChainInstrumentor().instrument(tracer_provider=provider)


def setup_tracing(service_name: str | None = None) -> trace_sdk.TracerProvider:
    provider = build_provider(service_name)
    trace_api.set_tracer_provider(provider)
    instrument(provider)
    return provider
