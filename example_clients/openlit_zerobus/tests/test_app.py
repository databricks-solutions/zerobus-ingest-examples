"""Offline tests: no Collector, no network, no model key."""

import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import app  # noqa: E402


def test_chain_returns_canned_reply_without_network():
    chain = app.build_chain()
    reply = chain.invoke({"sides": 6, "player": "demo"})
    assert reply in app.CANNED_REPLIES


def test_chain_cycles_through_replies():
    chain = app.build_chain()
    replies = [chain.invoke({"sides": 6, "player": "demo"}) for _ in range(len(app.CANNED_REPLIES))]
    assert replies == app.CANNED_REPLIES


def test_collector_example_has_both_signals_and_no_literal_secrets():
    text = (ROOT / "config.example.yaml").read_text(encoding="utf-8")
    config = yaml.safe_load(text)
    assert set(config["service"]["pipelines"]) == {"traces", "metrics"}
    assert "${env:DATABRICKS_CLIENT_SECRET}" in text
    assert "dapi" not in text
