"""Dice-game LLM app, instrumented by OpenLIT and sent to a local OpenTelemetry Collector.

The chat model is LangChain's FakeListChatModel, so the app runs offline and needs no model API key.
OpenLIT instruments the LangChain chain and exports traces and metrics over OTLP/HTTP to the Collector.
"""

import argparse
import os
import time

import openlit
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

DEFAULT_OTLP_ENDPOINT = "http://127.0.0.1:4318"

CANNED_REPLIES = [
    "The host rolls a six-sided die: 5. Lucky roll!",
    "A roll of 2 on a d6. Better luck next time.",
    "Rolled a 6! The crowd goes wild.",
]


def build_chain():
    """Prompt | fake chat model | string parser. The model replies from CANNED_REPLIES in turn."""
    model = FakeListChatModel(responses=CANNED_REPLIES)
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", "You are the host of a dice game."),
            ("user", "Roll a {sides}-sided die for {player}."),
        ]
    )
    return prompt | model | StrOutputParser()


def init_telemetry(endpoint=None):
    """Start OpenLIT. Traces and metrics go to the Collector; events (logs) stay off (see README)."""
    openlit.init(
        otlp_endpoint=endpoint or os.environ.get("OPENLIT_OTLP_ENDPOINT", DEFAULT_OTLP_ENDPOINT),
        application_name="openlit-dice-app",
        environment=os.environ.get("OPENLIT_ENVIRONMENT", "demo"),
        disable_events=True,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the dice-game chain in a loop.")
    parser.add_argument("--calls", type=int, default=10, help="number of chain calls")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between calls")
    parser.add_argument("--player", default="demo", help="player name in the prompt")
    args = parser.parse_args(argv)

    init_telemetry()
    chain = build_chain()
    for i in range(args.calls):
        reply = chain.invoke({"sides": 6, "player": args.player})
        print(f"call {i + 1}: {reply}", flush=True)
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
