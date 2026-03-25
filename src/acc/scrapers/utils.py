from playwright.async_api import Locator, Page, TimeoutError as PlaywrightTimeoutError


async def wait_after_navigation(
    page: Page,
    *,
    timeout_ms: int = 15_000,
) -> None:
    """Prefer load + main landmark over a blind sleep after goto."""
    try:
        await page.wait_for_load_state("load", timeout=min(10_000, timeout_ms))
    except PlaywrightTimeoutError:
        pass
    try:
        await page.locator("div[role='main']").first.wait_for(state="attached", timeout=5_000)
    except PlaywrightTimeoutError:
        pass


async def wait_for_first_locator(
    page: Page,
    selector: str,
    *,
    state: str = "visible",
    timeout_ms: int = 12_000,
) -> bool:
    try:
        await page.locator(selector).first.wait_for(state=state, timeout=timeout_ms)
        return True
    except PlaywrightTimeoutError:
        return False


async def find_first_visible(page: Page, selectors: tuple[str, ...], timeout_ms: int) -> Locator | None:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.is_visible(timeout=timeout_ms):
                return locator
        except PlaywrightTimeoutError:
            continue
    return None


async def click_first(page: Page, selectors: tuple[str, ...], timeout_ms: int) -> bool:
    locator = await find_first_visible(page, selectors, timeout_ms)
    if locator is None:
        return False
    await locator.click()
    return True


async def fill_first(page: Page, selectors: tuple[str, ...], value: str, timeout_ms: int) -> bool:
    locator = await find_first_visible(page, selectors, timeout_ms)
    if locator is None:
        return False
    await locator.fill(value)
    return True

