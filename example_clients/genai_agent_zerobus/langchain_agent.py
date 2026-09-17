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

from databricks_langchain import ChatDatabricks
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

from zerobus_otel import ZerobusConfig, build_providers # custom module - Will handle authentication and exporting the spans to ZeroBus.

SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "genai-agent")
# Model Serving / AI Gateway endpoint NAME (not a URL). Any chat endpoint works; find yours under
# "Serving" in the workspace. Governed by Databricks AI Gateway (rate limits, usage tracking, guardrails).
MODEL_ENDPOINT = os.environ.get("MODEL_ENDPOINT", "databricks-claude-sonnet-5")
QUESTION = "What's the weather in Paris, and should I bring an umbrella?"


def build_chain():
    """An ordinary LangChain chain: prompt | model | parser. No ZeroBus awareness here."""
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a helpful weather assistant. Answer in one sentence."),
        ("human", "{question}"),
    ])
    # This example calls a Databricks AI Gateway (Model Serving) endpoint, so it runs with no external
    # API key. The model provider does NOT matter to ZeroBus — the instrumentor emits gen_ai.* spans by
    # hooking LangChain's callbacks, not a provider SDK, so the export path below is identical whatever
    # you use here. To point at your own provider instead, swap just this one line, e.g.:
    #       from langchain.chat_models import init_chat_model
    #       model = init_chat_model("anthropic:claude-3-5-haiku-latest")   # needs that provider's key
    model = ChatDatabricks(endpoint=MODEL_ENDPOINT)
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
