"""Tests for the dice-roll app and the example Telegraf configuration. Run from the example folder: pytest tests."""

import pathlib
import re
import sys
import tomllib

from fastapi.testclient import TestClient

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import dice_app  # noqa: E402


def test_line_protocol_shape():
    assert dice_app.line_protocol("demo", 4, host="laptop") == "dice_roll,host=laptop,player=demo value=4i"


def test_tag_values_are_escaped():
    assert dice_app.line_protocol("a b,c=d", 1, host="h") == r"dice_roll,host=h,player=a\ b\,c\=d value=1i"


def test_roll_returns_a_die_value_and_posts_one_point(monkeypatch):
    sent = []

    async def fake_post(body):
        sent.append(body)
        return 204

    monkeypatch.setattr(dice_app, "post_to_telegraf", fake_post)
    response = TestClient(dice_app.app).get("/roll", params={"player": "demo"})
    assert response.status_code == 200
    value = response.json()["value"]
    assert 1 <= value <= 6
    assert sent == [f"dice_roll,host={dice_app.APP_HOST},player=demo value={value}i"]


def test_roll_reports_a_telegraf_error_as_502(monkeypatch):
    async def fake_post(body):
        return 400

    monkeypatch.setattr(dice_app, "post_to_telegraf", fake_post)
    assert TestClient(dice_app.app).get("/roll").status_code == 502


def test_player_name_is_validated():
    assert TestClient(dice_app.app).get("/roll", params={"player": "a b"}).status_code == 422


def test_example_config_has_the_output_contract():
    cfg = tomllib.loads((ROOT / "config.example.toml").read_text(encoding="utf-8"))
    out = cfg["outputs"]["opentelemetry"][0]
    assert out["token"] == "@{zbauth:zbtoken}"
    assert out["service_address"].endswith("/v1/metrics")
    assert out["headers"]["x-databricks-zerobus-table-name"] == "<catalog>.<schema>.<table>"
    assert cfg["inputs"]["http_listener_v2"][0]["paths"] == ["/telegraf"]
    assert cfg["secretstores"]["oauth2"][0]["id"] == "zbauth"


def test_example_config_has_placeholders_only():
    text = (ROOT / "config.example.toml").read_text(encoding="utf-8")
    assert not re.search(r"\b\d{10,}\b", text), "looks like a real workspace ID"
    assert not re.search(r"\bdapi[0-9a-f]{32}\b|\bdose[0-9a-f]{20,}\b", text), "looks like a real token"
    assert 'client_secret = "${DATABRICKS_CLIENT_SECRET}"' in text
    assert 'client_id = "${DATABRICKS_CLIENT_ID}"' in text
