"""
weather_agent — a REASONING + TOOL-CALLING LangChain agent, exporting to ZeroBus.

Same story as langchain_agent.py, one step richer: instead of a single prompt->model->parser
chain, this is a ReAct agent (langgraph) with a real tool. The model *reasons*, decides to call
`get_weather`, the tool hits a free weather API (Open-Meteo — no API key), the model reads the
result and answers. That produces a genuine agentic trace: AGENT -> CHAT_MODEL (decide) ->
TOOL (get_weather) -> CHAT_MODEL (answer), with the model's summarized reasoning captured.

The point for the demo: this is a **LangChain-only** change. `zerobus_otel`, the auth flow, and the
export wiring are IDENTICAL to the simple example — a more capable agent just emits a richer trace
through the exact same pipe.

Config comes from environment variables (see .env.example / README). Run: `python weather_agent.py`.
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

from zerobus_otel import ZerobusConfig, build_providers  # unchanged: handles auth + export to ZeroBus

SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "genai-weather-agent")
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
        "city": f"{loc['name']}, {loc.get('country_code','')}",
        "temp_c": cur["temperature_2m"],
        "conditions": _WMO.get(cur["weather_code"], f"code {cur['weather_code']}"),
        "precipitation_mm": cur["precipitation"],
        "wind_kmh": cur["wind_speed_10m"],
    }


def main():
    cfg = ZerobusConfig.from_env()
    resource = Resource.create({"service.name": SERVICE_NAME, "service.namespace": "genai-agents"})

    # The ONLY ZeroBus-aware lines — identical to the simple example.
    tracer_provider, logger_provider, meter_provider = build_providers(cfg, resource)
    trace.set_tracer_provider(tracer_provider)
    LangchainInstrumentor().instrument(tracer_provider=tracer_provider)

    # Enable SUMMARIZED extended thinking so the model's reasoning is captured in the trace
    # (Sonnet-class models omit raw reasoning by default). This is a model param, not a ZeroBus concern.
    model = ChatDatabricks(
        endpoint=MODEL_ENDPOINT,
        extra_params={"extra_body": {"thinking": {"type": "adaptive", "display": "summarized"}}},
    )
    agent = create_agent(model, [get_weather])

    print(f"weather_agent -> ZeroBus ({cfg.endpoint}); model={MODEL_ENDPOINT}; service.name={SERVICE_NAME}")
    result = agent.invoke({"messages": [("user", QUESTION)]})
    print("answer:", result["messages"][-1].content)

    print("Flushing to ZeroBus...")
    tracer_provider.force_flush(); logger_provider.force_flush(); meter_provider.force_flush()
    tracer_provider.shutdown(); logger_provider.shutdown(); meter_provider.shutdown()
    print("Done.")


if __name__ == "__main__":
    main()
