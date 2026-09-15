"""
langchain_agent — a worked example: a LangChain agent whose OpenTelemetry spans export
directly to Databricks ZeroBus via the embedded `zerobus_otel` component (no collector).

The point: an open-source client (LangChain) ships its telemetry to Unity Catalog with NO
ZeroBus-specific code in the app. The only ZeroBus-aware lines are `build_providers(...)`
and attaching the instrumentor to that provider — everything else is ordinary LangChain.

How the spans get out:
  • `opentelemetry-instrumentation-langchain` (Traceloop / OpenLLMetry) hooks LangChain's
    callback manager, so every chain/LLM step becomes an OTel span carrying `gen_ai.*`
    semantic-convention attributes (model, prompts/completions, finish reason).
  • That instrumentor is handed OUR TracerProvider — the one `zerobus_otel.build_providers`
    wired to ZeroBus — so the spans land in `<prefix>_otel_spans` in Unity Catalog.

Config comes from environment variables (see .env.example / README). Run: `python langchain_agent.py`.
"""
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.langchain import LangchainInstrumentor

from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from zerobus_otel import ZerobusConfig, build_providers

SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "genai-agent")
QUESTION = "What's the weather in Paris, and should I bring an umbrella?"


def build_chain():
    """An ordinary LangChain chain: prompt | model | parser. No ZeroBus awareness here."""
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a helpful weather assistant. Answer in one sentence."),
        ("human", "{question}"),
    ])
    # Default: a fake model so the example runs with NO model API key — the instrumentor still
    # emits gen_ai.* spans because it hooks LangChain's callbacks, not a provider SDK.
    #
    # Swap in your real model — the export path is unchanged. Two common options:
    #   • Databricks Model Serving (no external key):
    #       from databricks_langchain import ChatDatabricks
    #       model = ChatDatabricks(endpoint="databricks-claude-sonnet-5")
    #   • Any provider, provider-agnostic (needs that provider's key):
    #       from langchain.chat_models import init_chat_model
    #       model = init_chat_model("anthropic:claude-3-5-haiku-latest")
    model = FakeListChatModel(
        responses=["It's 14°C with light rain in Paris right now — yes, bring an umbrella!"],
    )
    return prompt | model | StrOutputParser()


def main():
    cfg = ZerobusConfig.from_env()
    resource = Resource.create({
        "service.name": SERVICE_NAME,           # the value you filter traces on
        "service.namespace": "genai-agents",
    })

    # The ONLY ZeroBus-specific wiring: build providers pointed at ZeroBus, then hand the
    # tracer provider to the LangChain instrumentor. LangChain itself stays untouched.
    tracer_provider, logger_provider, meter_provider = build_providers(cfg, resource)
    trace.set_tracer_provider(tracer_provider)
    LangchainInstrumentor().instrument(tracer_provider=tracer_provider)

    print(f"langchain_agent -> ZeroBus ({cfg.endpoint}); service.name={SERVICE_NAME}")
    answer = build_chain().invoke({"question": QUESTION})
    print("answer:", answer)

    # Flush the batch exporters so spans reach ZeroBus before the process exits.
    print("Flushing to ZeroBus...")
    tracer_provider.force_flush(); logger_provider.force_flush(); meter_provider.force_flush()
    tracer_provider.shutdown(); logger_provider.shutdown(); meter_provider.shutdown()
    print("Done.")


if __name__ == "__main__":
    main()
