import pytest

from acc.config import Settings
from acc.scrapers.fetch_limiter import FetchLimiter, get_fetch_limiter, reset_fetch_limiter_for_tests


@pytest.mark.asyncio
async def test_fetch_limiter_acquire_releases() -> None:
    lim = FetchLimiter(global_limit=2, per_host_limit=1)
    async with lim.acquire("https://example.com/path"):
        async with lim.acquire("https://other.test/"):
            pass


def test_get_fetch_limiter_singleton_resets_when_limits_change() -> None:
    reset_fetch_limiter_for_tests()
    a = get_fetch_limiter(
        Settings(
            fetch_max_concurrent_global=5,
            fetch_max_concurrent_per_host=2,
        )
    )
    b = get_fetch_limiter(
        Settings(
            fetch_max_concurrent_global=5,
            fetch_max_concurrent_per_host=2,
        )
    )
    assert a is b
    c = get_fetch_limiter(
        Settings(
            fetch_max_concurrent_global=6,
            fetch_max_concurrent_per_host=2,
        )
    )
    assert c is not a
    reset_fetch_limiter_for_tests()
