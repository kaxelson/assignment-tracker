"""Process-wide OpenAI request concurrency (single bottleneck for all LLM calls)."""

from __future__ import annotations

import asyncio

import structlog

from acc.config import Settings

logger = structlog.get_logger(__name__)

_gate: OpenAIConcurrencyGate | None = None
_gate_limit: int | None = None


class OpenAIConcurrencyGate:
    __slots__ = ("_sem", "_limit")

    def __init__(self, limit: int) -> None:
        self._limit = max(1, limit)
        self._sem = asyncio.Semaphore(self._limit)

    @property
    def limit(self) -> int:
        return self._limit

    async def acquire(self) -> None:
        await self._sem.acquire()

    def release(self) -> None:
        self._sem.release()


def get_openai_gate(settings: Settings) -> OpenAIConcurrencyGate:
    global _gate, _gate_limit
    lim = max(1, settings.openai_max_concurrent_requests)
    if _gate is None or _gate_limit != lim:
        _gate = OpenAIConcurrencyGate(lim)
        _gate_limit = lim
        logger.info("openai_queue.configured", openai_max_concurrent_requests=lim)
    return _gate


def reset_openai_gate_for_tests() -> None:
    global _gate, _gate_limit
    _gate = None
    _gate_limit = None
