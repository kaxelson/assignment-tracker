from abc import ABC
from pathlib import Path

from playwright.async_api import Page

from acc.config import Settings


class ScraperError(RuntimeError):
    pass


class BaseScraper(ABC):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def save_screenshot(self, page: Page, name: str) -> Path:
        path = self.settings.screenshots_dir / name
        await page.screenshot(path=str(path), full_page=True)
        return path

