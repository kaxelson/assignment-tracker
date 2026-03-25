import asyncio
import json
from io import BytesIO
from unittest.mock import patch

from urllib.error import HTTPError

from acc.ai.client import OpenAIChatClient
from acc.config import Settings


def build_settings(model: str, **kwargs: object) -> Settings:
    merged: dict[str, object] = {
        "openai_api_key": "test-key",
        "openai_model": model,
        "_env_file": None,
    }
    merged.update(kwargs)
    return Settings(**merged)


def test_openai_chat_client_keeps_temperature_for_pre_gpt5_models() -> None:
    client = OpenAIChatClient(build_settings("gpt-4o-mini"))

    payload = client._build_payload("Return JSON.")

    assert payload["temperature"] == 0


def test_openai_chat_client_omits_temperature_for_gpt5_models() -> None:
    client = OpenAIChatClient(build_settings(" GPT-5.1-chat-latest "))

    payload = client._build_payload("Return JSON.")

    assert "temperature" not in payload


def test_openai_chat_client_raises_floor_timeout_for_nano_models() -> None:
    client = OpenAIChatClient(
        build_settings("gpt-5-nano", openai_timeout_seconds=60),
    )
    assert client.timeout_seconds == 60
    assert client.request_timeout_seconds == 240


def test_openai_chat_client_respects_high_timeout_for_nano_models() -> None:
    client = OpenAIChatClient(
        build_settings("gpt-5-nano", openai_timeout_seconds=400),
    )
    assert client.request_timeout_seconds == 400


class _OkResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_OkResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_openai_client_retries_429_then_succeeds(monkeypatch) -> None:
    monkeypatch.setattr("acc.ai.client.time.sleep", lambda _s: None)
    calls: list[str] = []

    def fake_urlopen(_req, timeout=None):
        if len(calls) == 0:
            calls.append("429")
            raise HTTPError(
                "https://api.openai.com/v1/chat/completions",
                429,
                "Too Many Requests",
                {"Retry-After": "0"},
                BytesIO(b'{"error":"rate_limit"}'),
            )
        calls.append("ok")
        return _OkResponse({"choices": [{"message": {"content": "{}"}}]})

    client = OpenAIChatClient(
        build_settings("gpt-4o-mini", openai_retry_max_attempts=3),
    )
    with patch("acc.ai.client.request.urlopen", side_effect=fake_urlopen):
        out = asyncio.run(client.complete_json("x"))
    assert out == "{}"
    assert calls == ["429", "ok"]


def test_openai_client_retries_timeout_then_succeeds(monkeypatch) -> None:
    monkeypatch.setattr("acc.ai.client.time.sleep", lambda _s: None)
    n = {"c": 0}

    def fake_urlopen(_req, timeout=None):
        n["c"] += 1
        if n["c"] == 1:
            raise TimeoutError("The read operation timed out")
        return _OkResponse({"choices": [{"message": {"content": "{\"a\":1}"}}]})

    client = OpenAIChatClient(
        build_settings("gpt-4o-mini", openai_retry_max_attempts=3),
    )
    with patch("acc.ai.client.request.urlopen", side_effect=fake_urlopen):
        out = asyncio.run(client.complete_json("x"))
    assert json.loads(out) == {"a": 1}


def test_openai_client_stops_after_429_retries(monkeypatch) -> None:
    monkeypatch.setattr("acc.ai.client.time.sleep", lambda _s: None)

    def always_429(_req, timeout=None):
        raise HTTPError(
            "https://api.openai.com/v1/chat/completions",
            429,
            "Too Many Requests",
            {"Retry-After": "0"},
            BytesIO(b'{"error":"rate_limit"}'),
        )

    client = OpenAIChatClient(
        build_settings("gpt-4o-mini", openai_retry_max_attempts=2),
    )
    with patch("acc.ai.client.request.urlopen", side_effect=always_429):
        try:
            asyncio.run(client.complete_json("x"))
        except RuntimeError as exc:
            assert "rate_limit" in str(exc) or "failed" in str(exc).lower()
        else:
            raise AssertionError("expected RuntimeError")
