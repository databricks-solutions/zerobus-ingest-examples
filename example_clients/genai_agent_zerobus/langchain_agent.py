"""
langchain_agent — a reasoning + tool-calling LangChain agent whose OpenTelemetry spans
export directly to Databricks ZeroBus via the embedded `zerobus_otel` component (no collector).

The agent has one tool (`get_weather`, backed by the free Open-Meteo API — no key). The model
*reasons*, decides to call the tool, reads the result, and answers — producing a genuine agentic
trace: AGENT -> CHAT_MODEL (decide) -> TOOL (get_weather) -> CHAT_MODEL (answer), with the model's
summarized reasoning captured.

The point: shipping this to Unity Catalog is a **LangChain-only** concern. The only ZeroBus-aware
lines are `build_providers(...)` and attaching the instrumentor to that provider — everything else
is ordinary LangChain. Make the agent as simple or complex as you like; the export path never changes.

How the spans get out:
  • `opentelemetry-instrumentation-langchain` (Traceloop / OpenLLMetry) hooks LangChain's callback
    manager, so every model/tool step becomes an OTel span with `gen_ai.*` attributes.
  • That instrumentor is handed the TracerProvider `zerobus_otel.build_providers` wired to ZeroBus,
    so the spans land in `<prefix>_otel_spans` in Unity Catalog.

Config comes from environment variables (see .env.example / README). Run: `python langchain_agent.py`.
"""
import json
import os
import urllib.parse
import urllib.request

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.langchain import LangchainInstrumentor

from databricks_langchain import ChatDatabricks
from langchain_core.tools import tool
from langchain.agents import create_agent

from zerobus_otel import ZerobusConfig, build_providers  # custom module — handles auth + export to ZeroBus

SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "genai-agent")
# Model Serving / AI Gateway endpoint NAME (not a URL). Any chat endpoint works; find yours under
# "Serving" in the workspace. Governed by Databricks AI Gateway (rate limits, usage tracking, guardrails).
MODEL_ENDPOINT = os.environ.get("MODEL_ENDPOINT", "databricks-claude-sonnet-5")
QUESTION = "What's the weather in Paris right now, and should I bring an umbrella?"

# WMO weather codes -> human text (https://open-meteo.com/en/docs)
_WMO = {0: "clear sky", 1: "mainly clear", 2: "partly cloudy", 3: "overcast", 45: "fog", 48: "rime fog",
        51: "light drizzle", 53: "drizzle", 55: "dense drizzle", 61: "slight rain", 63: "rain",
        65: "heavy rain", 71: "slight snow", 73: "snow", 75: "heavy snow", 80: "rain showers",
        81: "rain showers", 82: "violent rain showers", 95: "thunderstorm"}


@tool
def get_weather(city: str) -> dict:
    """Get the CURRENT weather for a city name. Returns temperature (°C), conditions, precipitation, wind."""
    geo = json.load(urllib.request.urlopen(
        "https://geocoding-api.open-meteo.com/v1/search?" +
        urllib.parse.urlencode({"name": city, "count": 1, "language": "en", "format": "json"}), timeout=15))
    if not geo.get("results"):
        return {"city": city, "error": "location not found"}
    loc = geo["results"][0]
    fc = json.load(urllib.request.urlopen(
        "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode({
            "latitude": loc["latitude"], "longitude": loc["longitude"],
            "current": "temperature_2m,precipitation,weather_code,wind_speed_10m"}), timeout=15))
    cur = fc["current"]
    return {
        "city": f"{loc['name']}, {loc.get('country_code', '')}",
        "temp_c": cur["temperature_2m"],
        "conditions": _WMO.get(cur["weather_code"], f"code {cur['weather_code']}"),
        "precipitation_mm": cur["precipitation"],
        "wind_kmh": cur["wind_speed_10m"],
    }


def build_agent():
    """An ordinary LangChain agent: a model + a tool. No ZeroBus awareness here.

    This example calls a Databricks AI Gateway (Model Serving) endpoint, so it runs with no external
    API key. The model provider does NOT matter to ZeroBus — the instrumentor emits gen_ai.* spans by
    hooking LangChain's callbacks, not a provider SDK, so the export path is identical whatever you use.
    To point at your own provider instead, swap just this `model = ...` line, e.g.:
          from langchain.chat_models import init_chat_model
          model = init_chat_model("anthropic:claude-3-5-haiku-latest")   # needs that provider's key
    """
    # We use Databricks (ChatDatabricks -> an AI Gateway / Model Serving endpoint) as the model provider
    # here, but this is still plain LangChain — no special modification. Use whatever provider you like:
    # swap this one line for ChatOpenAI, ChatAnthropic, init_chat_model("<provider>:<model>"), etc. The
    # agent, the instrumentation, and the ZeroBus export path are all identical regardless of provider.
    #
    # (The extra_params below just enable SUMMARIZED extended thinking so the model's reasoning shows up
    #  in the trace — Sonnet-class models omit raw reasoning by default. It's a model param, not a
    #  ZeroBus concern, and is Databricks/Anthropic-specific — drop it for other providers.)
    model = ChatDatabricks(
        endpoint=MODEL_ENDPOINT,
        extra_params={"extra_body": {"thinking": {"type": "adaptive", "display": "summarized"}}},
    )
    return create_agent(model, [get_weather])


def main():
    cfg = ZerobusConfig.from_env()
    resource = Resource.create({"service.name": SERVICE_NAME, "service.namespace": "genai-agents"})

    # The ONLY ZeroBus-specific wiring: build providers pointed at ZeroBus, then hand the tracer
    # provider to the LangChain instrumentor. LangChain (and the agent above) stays untouched.
    tracer_provider, logger_provider, meter_provider = build_providers(cfg, resource)
    trace.set_tracer_provider(tracer_provider)
    LangchainInstrumentor().instrument(tracer_provider=tracer_provider)

    print(f"langchain_agent -> ZeroBus ({cfg.endpoint}); model={MODEL_ENDPOINT}; service.name={SERVICE_NAME}")
    result = build_agent().invoke({"messages": [("user", QUESTION)]})
    print("answer:", result["messages"][-1].content)

    # Flush the batch exporters so spans reach ZeroBus before the process exits.
    print("Flushing to ZeroBus...")
    tracer_provider.force_flush(); logger_provider.force_flush(); meter_provider.force_flush()
    tracer_provider.shutdown(); logger_provider.shutdown(); meter_provider.shutdown()
    print("Done.")


if __name__ == "__main__":
    main()
