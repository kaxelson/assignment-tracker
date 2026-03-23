from playwright.async_api import Locator, Page, TimeoutError as PlaywrightTimeoutError


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

