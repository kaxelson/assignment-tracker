"""Global + per-host concurrency limits for Playwright navigations (and similar fetches)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import structlog
from playwright.async_api import Page

from acc.config import Settings

logger = structlog.get_logger(__name__)

_limiter: FetchLimiter | None = None
_limiter_limits: tuple[int, int] | None = None


class FetchLimiter:
    """Nested limits: at most N navigations total, and at most M to the same host."""

    __slots__ = ("_global_sem", "_per_host_limit", "_host_lock", "_host_sems")

    def __init__(self, *, global_limit: int, per_host_limit: int) -> None:
        self._global_sem = asyncio.Semaphore(max(1, global_limit))
        self._per_host_limit = max(1, per_host_limit)
        self._host_lock = asyncio.Lock()
        self._host_sems: dict[str, asyncio.Semaphore] = {}

    def _netloc(self, url: str) -> str:
        parsed = urlparse(url)
        return (parsed.netloc or "local").lower() or "local"

    async def _host_sem(self, netloc: str) -> asyncio.Semaphore:
        async with self._host_lock:
            if netloc not in self._host_sems:
                self._host_sems[netloc] = asyncio.Semaphore(self._per_host_limit)
            return self._host_sems[netloc]

    @asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[None]:
        netloc = self._netloc(url)
        await self._global_sem.acquire()
        host_sem = await self._host_sem(netloc)
        await host_sem.acquire()
        try:
            yield
        finally:
            host_sem.release()
            self._global_sem.release()


def get_fetch_limiter(settings: Settings) -> FetchLimiter:
    global _limiter, _limiter_limits
    key = (max(1, settings.fetch_max_concurrent_global), max(1, settings.fetch_max_concurrent_per_host))
    if _limiter is None or _limiter_limits != key:
        _limiter = FetchLimiter(global_limit=key[0], per_host_limit=key[1])
        _limiter_limits = key
        logger.info(
            "fetch_limiter.configured",
            fetch_max_concurrent_global=key[0],
            fetch_max_concurrent_per_host=key[1],
        )
    return _limiter


def reset_fetch_limiter_for_tests() -> None:
    global _limiter, _limiter_limits
    _limiter = None
    _limiter_limits = None


async def goto_throttled(
    page: Page,
    url: str,
    settings: Settings,
    *,
    wait_until: str = "domcontentloaded",
) -> None:
    """page.goto under global + per-host fetch limits."""
    limiter = get_fetch_limiter(settings)
    async with limiter.acquire(url):
        await page.goto(url, wait_until=wait_until)
