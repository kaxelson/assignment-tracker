from __future__ import annotations

import json
from typing import Literal

from pydantic import BaseModel, Field

from acc.ai.client import JsonModelClient, OpenAIChatClient, extract_json_text
from acc.ai.prompts import CRAWL_LINK_SELECTION_PROMPT
from acc.config import Settings
from acc.scrapers.crawl_navigation import d2l_tool_nav_should_be_crawled, nav_target_should_be_crawled

MAX_PAGE_TEXT_CHARS = 12_000

CrawlPlatform = Literal["d2l", "cengage", "pearson"]


class LinkSelectionResult(BaseModel):
    follow: list[int] = Field(default_factory=list)
    notes: str | None = None


class CrawlLinkPicker:
    def __init__(
        self,
        settings: Settings,
        *,
        client: JsonModelClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or OpenAIChatClient(settings, context="crawl link selection")

    def _build_prompt(
        self,
        *,
        platform: CrawlPlatform,
        page_url: str,
        page_text: str,
        course_code: str,
        course_name: str,
        links: list[tuple[str, str]],
    ) -> str:
        truncated = page_text[:MAX_PAGE_TEXT_CHARS]
        lines = [
            CRAWL_LINK_SELECTION_PROMPT,
            "",
            f"PLATFORM: {platform}",
            f"COURSE: {course_code} - {course_name}",
            f"PAGE_URL: {page_url}",
            "",
            "PAGE_TEXT:",
            truncated,
            "",
            "LINKS (index, href, visible text):",
        ]
        for index, (href, text) in enumerate(links):
            lines.append(f"{index}\t{href}\t{text}")
        return "\n".join(lines)

    async def pick_link_selection(
        self,
        *,
        platform: CrawlPlatform,
        page_url: str,
        page_text: str,
        course_code: str,
        course_name: str,
        links: list[tuple[str, str]],
    ) -> LinkSelectionResult:
        if not links:
            return LinkSelectionResult(follow=[], notes=None)
        prompt = self._build_prompt(
            platform=platform,
            page_url=page_url,
            page_text=page_text,
            course_code=course_code,
            course_name=course_name,
            links=links,
        )
        raw = await self.client.complete_json(prompt)
        parsed = LinkSelectionResult.model_validate(json.loads(extract_json_text(raw)))
        n = len(links)
        out: list[int] = []
        seen: set[int] = set()
        for index in parsed.follow:
            if not isinstance(index, int):
                continue
            if index < 0 or index >= n:
                continue
            if index in seen:
                continue
            seen.add(index)
            out.append(index)
        return LinkSelectionResult(follow=out, notes=parsed.notes)

    async def pick_follow_indices(
        self,
        *,
        platform: CrawlPlatform,
        page_url: str,
        page_text: str,
        course_code: str,
        course_name: str,
        links: list[tuple[str, str]],
    ) -> list[int]:
        selection = await self.pick_link_selection(
            platform=platform,
            page_url=page_url,
            page_text=page_text,
            course_code=course_code,
            course_name=course_name,
            links=links,
        )
        return selection.follow


def heuristic_follow_indices_d2l(links: list[tuple[str, str]]) -> list[int]:
    return [
        index
        for index, (href, text) in enumerate(links)
        if d2l_tool_nav_should_be_crawled(text or "", href)
    ]


def heuristic_follow_indices_external(links: list[tuple[str, str]]) -> list[int]:
    return [
        index
        for index, (href, text) in enumerate(links)
        if nav_target_should_be_crawled(text or "", href)
    ]


def heuristic_follow_indices_pearson_nav(entries: list[tuple[str, str]]) -> list[int]:
    """entries are (nav_label, absolute_href) as used by Pearson left nav."""
    return [
        index
        for index, (label, abs_url) in enumerate(entries)
        if nav_target_should_be_crawled(label or "", abs_url)
    ]
