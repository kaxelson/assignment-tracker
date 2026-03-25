import asyncio
import json
import random
import time
from email.utils import parsedate_to_datetime
from typing import Protocol
from urllib import error, request

import structlog

from acc.ai.openai_queue import get_openai_gate
from acc.config import Settings

logger = structlog.get_logger(__name__)

# gpt-5-nano (and similar) often needs longer per-request time than default 90s for JSON + large prompts.
_NANO_MODEL_MIN_TIMEOUT_SECONDS = 240


class JsonModelClient(Protocol):
    async def complete_json(self, prompt: str) -> str:
        ...


class OpenAIChatClient:
    def __init__(
        self,
        settings: Settings,
        *,
        context: str = "request",
        use_global_concurrency: bool = True,
    ) -> None:
        if settings.openai_api_key is None:
            raise RuntimeError("ACC_OPENAI_API_KEY is required.")
        self.settings = settings
        self.api_key = settings.openai_api_key.get_secret_value()
        self.model = settings.openai_model
        self.timeout_seconds = max(1, settings.openai_timeout_seconds)
        self.request_timeout_seconds = _openai_request_timeout_seconds(
            self.model,
            self.timeout_seconds,
        )
        self.retry_max_attempts = max(1, settings.openai_retry_max_attempts)
        self.retry_base_delay_seconds = max(0.1, settings.openai_retry_base_delay_seconds)
        self.context = context
        self.use_global_concurrency = use_global_concurrency

    async def complete_json(self, prompt: str) -> str:
        if not self.use_global_concurrency:
            return await asyncio.to_thread(self._complete_json_sync, prompt)
        gate = get_openai_gate(self.settings)
        await gate.acquire()
        try:
            return await asyncio.to_thread(self._complete_json_sync, prompt)
        finally:
            gate.release()

    def _complete_json_sync(self, prompt: str) -> str:
        logger.info(
            "openai.request",
            context=self.context,
            model=self.model,
            prompt_chars=len(prompt),
        )
        payload = self._build_payload(prompt)
        body = json.dumps(payload).encode("utf-8")
        api_request = request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=body,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        for attempt in range(self.retry_max_attempts):
            try:
                with request.urlopen(api_request, timeout=self.request_timeout_seconds) as response:
                    response_data = json.loads(response.read().decode("utf-8"))
            except error.HTTPError as exc:
                details = exc.read().decode("utf-8", errors="replace")
                if exc.code == 429 and attempt + 1 < self.retry_max_attempts:
                    time.sleep(self._retry_delay_seconds(attempt, exc))
                    continue
                raise RuntimeError(f"OpenAI {self.context} failed: {details}") from exc
            except TimeoutError as exc:
                if attempt + 1 < self.retry_max_attempts:
                    time.sleep(self._retry_delay_seconds(attempt, None))
                    continue
                raise self._timeout_runtime_error() from exc
            except error.URLError as exc:
                if isinstance(exc.reason, TimeoutError):
                    if attempt + 1 < self.retry_max_attempts:
                        time.sleep(self._retry_delay_seconds(attempt, None))
                        continue
                    raise self._timeout_runtime_error() from exc
                raise RuntimeError(f"Could not reach OpenAI API: {exc.reason}") from exc
            else:
                return str(response_data["choices"][0]["message"]["content"])

    def _retry_delay_seconds(self, attempt_index: int, rate_exc: error.HTTPError | None) -> float:
        cap = 120.0
        if rate_exc is not None and rate_exc.code == 429:
            raw = rate_exc.headers.get("Retry-After")
            if raw:
                try:
                    return min(cap, float(raw))
                except ValueError:
                    try:
                        when = parsedate_to_datetime(raw)
                        if when is not None:
                            delay = (when.timestamp() - time.time()) + 0.5
                            return min(cap, max(0.5, delay))
                    except (TypeError, ValueError, OSError):
                        pass
        base = self.retry_base_delay_seconds
        jitter = random.uniform(0, 0.25 * base)
        return min(cap, base * (2**attempt_index)) + jitter

    def _timeout_runtime_error(self) -> RuntimeError:
        return RuntimeError(
            f"OpenAI {self.context} timed out after {self.request_timeout_seconds}s while waiting for the API "
            f"(often during response read on large crawl batches). "
            f"Increase ACC_OPENAI_TIMEOUT_SECONDS (configured value is {self.timeout_seconds}s)."
        )

    def _build_payload(self, prompt: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": "Return valid JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        }
        if self._supports_temperature_override():
            payload["temperature"] = 0
        return payload

    def _supports_temperature_override(self) -> bool:
        return not _is_gpt5_family_model(self.model)


def _is_gpt5_family_model(model: str) -> bool:
    return model.strip().lower().startswith("gpt-5")


def _openai_request_timeout_seconds(model: str, configured_seconds: int) -> int:
    m = model.strip().lower()
    if "nano" in m:
        return max(configured_seconds, _NANO_MODEL_MIN_TIMEOUT_SECONDS)
    return configured_seconds


def extract_json_text(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.startswith("json"):
            cleaned = cleaned[4:].strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1:
        raise ValueError("Model response did not contain a JSON object.")
    return cleaned[start : end + 1]
