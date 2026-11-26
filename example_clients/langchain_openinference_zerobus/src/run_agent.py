"""Run the weather agent in a loop and send its traces to the Collector.

Usage (from the example folder, with PYTHONPATH=src):
    python src/run_agent.py --runs 60 --interval 1
"""

import argparse
import itertools
import time

from agent import ask, build_agent
from telemetry import setup_tracing

CITIES = ["Paris", "Tokyo", "Lima", "Oslo"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=10, help="number of agent runs")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between runs")
    args = parser.parse_args()

    provider = setup_tracing()
    agent = build_agent()
    try:
        for i, city in zip(range(1, args.runs + 1), itertools.cycle(CITIES)):
            answer = ask(agent, f"What is the weather in {city}?")
            print(f"run {i}: {answer}", flush=True)
            time.sleep(args.interval)
    finally:
        provider.force_flush()
        provider.shutdown()


if __name__ == "__main__":
    main()
