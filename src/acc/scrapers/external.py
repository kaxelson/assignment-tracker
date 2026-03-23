import time
import hashlib
import re
from datetime import UTC, datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import structlog
from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from acc.engine.normalizer import (
    build_course_id,
    choose_primary_external_tool,
    detect_external_platform_topic,
    infer_assignment_type,
    slugify,
)
from acc.scrapers.base import BaseScraper, ScraperError
from acc.scrapers.d2l import AUTHENTICATED_SELECTORS
from acc.scrapers.snapshots import (
    D2LDashboardSnapshot,
    ExternalAssignmentSnapshot,
    ExternalCourseSnapshot,
    ExternalScrapeSnapshot,
)
from acc.scrapers.utils import find_first_visible

logger = structlog.get_logger(__name__)

CENGAGE_HOST_TOKENS = ("cengage.com",)
PEARSON_HOST_TOKENS = ("pearson.com",)
PEARSON_DUE_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M\b")
POINTS_RE = re.compile(
    r"(?P<earned>--|\d+(?:\.\d+)?)\s*/\s*(?P<possible>\d+(?:\.\d+)?)\s*points?",
    re.IGNORECASE,
)
MINUTES_RE = re.compile(r"about\s+(?P<low>\d+)(?:-(?P<high>\d+))?\s+minutes", re.IGNORECASE)


class ExternalScraper(BaseScraper):
    async def save_snapshot(
        self,
        dashboard_snapshot: D2LDashboardSnapshot | None = None,
    ) -> ExternalScrapeSnapshot:
        snapshot = await self.snapshot(dashboard_snapshot=dashboard_snapshot)
        self.settings.external_snapshot_path.write_text(
            snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )
        logger.info(
            "external.snapshot_saved",
            path=str(self.settings.external_snapshot_path),
            courses=len(snapshot.courses),
            assignments=len(snapshot.assignments),
        )
        return snapshot

    async def snapshot(
        self,
        dashboard_snapshot: D2LDashboardSnapshot | None = None,
    ) -> ExternalScrapeSnapshot:
        if dashboard_snapshot is None:
            if not self.settings.d2l_snapshot_path.exists():
                raise ScraperError(
                    "No D2L snapshot found. Run `acc d2l-snapshot` before scraping external platforms."
                )
            dashboard_snapshot = D2LDashboardSnapshot.model_validate_json(
                self.settings.d2l_snapshot_path.read_text(encoding="utf-8")
            )

        if not self.settings.d2l_storage_state_path.exists():
            raise ScraperError("No saved D2L session found. Run `acc d2l-login` first.")

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright)
            page: Page | None = None
            try:
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await page.goto(self.settings.d2l_base_url, wait_until="domcontentloaded")
                if not await self._is_authenticated(page):
                    raise ScraperError(
                        "Saved D2L session is no longer authenticated. Run `acc d2l-login`."
                    )

                course_snapshots: list[ExternalCourseSnapshot] = []
                assignment_snapshots: list[ExternalAssignmentSnapshot] = []

                for course in dashboard_snapshot.courses:
                    topic = choose_primary_external_tool(course.external_tools)
                    if topic is None:
                        continue

                    platform = detect_external_platform_topic(topic)
                    if platform not in {"cengage_mindtap", "pearson_mylab"}:
                        continue

                    course_id = build_course_id(course)
                    launch_url = topic.launch_url or topic.url
                    course_snapshots.append(
                        ExternalCourseSnapshot(
                            course_id=course_id,
                            source_platform=platform,
                            launch_url=launch_url,
                            title=course.name,
                        )
                    )

                    if platform == "cengage_mindtap":
                        assignments = await self._scrape_cengage_course(
                            page=page,
                            course_id=course_id,
                            wrapper_url=topic.url,
                            launch_url=launch_url,
                        )
                    else:
                        assignments = await self._scrape_pearson_course(
                            page=page,
                            course_id=course_id,
                            wrapper_url=topic.url,
                        )

                    assignment_snapshots.extend(assignments)
                    logger.info(
                        "external.course_scraped",
                        course_id=course_id,
                        platform=platform,
                        assignments=len(assignments),
                    )

                return ExternalScrapeSnapshot(
                    fetched_at=datetime.now(UTC),
                    courses=course_snapshots,
                    assignments=assignment_snapshots,
                )
            finally:
                await browser_context.close()
                await browser.close()

    async def _new_context(self, playwright: Playwright) -> tuple[Browser, BrowserContext]:
        browser = await playwright.chromium.launch(
            headless=self.settings.browser_headless,
            slow_mo=self.settings.browser_slow_mo_ms,
        )
        context = await browser.new_context(storage_state=str(self.settings.d2l_storage_state_path))
        return browser, context

    async def _is_authenticated(self, page: Page) -> bool:
        locator = await find_first_visible(page, AUTHENTICATED_SELECTORS, 2_000)
        return locator is not None

    async def _scrape_cengage_course(
        self,
        page: Page,
        course_id: str,
        wrapper_url: str,
        launch_url: str,
    ) -> list[ExternalAssignmentSnapshot]:
        course_page = await self._open_external_page_from_navigation(
            page=page,
            wrapper_url=wrapper_url,
            host_tokens=CENGAGE_HOST_TOKENS,
            direct_fallback_url=launch_url,
        )
        await self._wait_for_selector(course_page, "li.activities-wrapper")

        rows = course_page.locator("li.activities-wrapper")
        assignments: list[ExternalAssignmentSnapshot] = []
        seen_ids: set[str] = set()

        for index in range(await rows.count()):
            row = rows.nth(index)
            title = _clean_text(await row.locator(".title").first.text_content() or "")
            row_text = _clean_text(await row.inner_text())
            points_text = _clean_text(await _first_text(row, ".node-info__inner"))
            description = _clean_text(await _first_text(row, ".description__content"))
            activity_class = await _first_attribute(row, ".activity", "class")
            href = await _first_attribute(row, "a[href]", "href")
            assignment = parse_cengage_assignment(
                course_id=course_id,
                title=title or row_text,
                row_text=row_text,
                points_text=points_text or None,
                activity_class=activity_class or "",
                external_url=urljoin(course_page.url, href) if href else None,
                description=description or None,
            )
            if assignment is None or assignment.id in seen_ids:
                continue
            assignments.append(assignment)
            seen_ids.add(assignment.id)

        await self._close_popup_if_needed(page, course_page)
        return assignments

    async def _scrape_pearson_course(
        self,
        page: Page,
        course_id: str,
        wrapper_url: str,
    ) -> list[ExternalAssignmentSnapshot]:
        course_page = await self._open_pearson_course_page(page=page, wrapper_url=wrapper_url)
        await course_page.wait_for_timeout(6_000)
        await self._wait_for_pearson_rows(course_page)

        rows = course_page.locator("li.assignment-row")
        assignments: list[ExternalAssignmentSnapshot] = []
        seen_ids: set[str] = set()

        for index in range(await rows.count()):
            row = rows.nth(index)
            title = _clean_text(await row.locator("a.assignment-row--div--link").first.text_content() or "")
            row_text = _clean_text(await row.inner_text())
            href = await _first_attribute(row, "a.assignment-row--div--link", "href")
            assignment = parse_pearson_assignment(
                course_id=course_id,
                title=title or row_text,
                row_text=row_text,
                external_url=urljoin(course_page.url, href) if href else None,
                timezone=self.settings.timezone,
            )
            if assignment is None or assignment.id in seen_ids:
                continue
            assignments.append(assignment)
            seen_ids.add(assignment.id)

        await self._close_popup_if_needed(page, course_page)
        return assignments

    async def _open_external_page_from_navigation(
        self,
        page: Page,
        wrapper_url: str,
        host_tokens: tuple[str, ...],
        direct_fallback_url: str | None = None,
    ) -> Page:
        await page.goto(wrapper_url, wait_until="domcontentloaded")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            external_page = await self._find_open_page(page.context.pages, host_tokens)
            if external_page is not None:
                return external_page
            if any(token in page.url for token in host_tokens):
                return page
            await page.wait_for_timeout(500)

        if direct_fallback_url and any(token in direct_fallback_url for token in host_tokens):
            await page.goto(direct_fallback_url, wait_until="domcontentloaded")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                external_page = await self._find_open_page(page.context.pages, host_tokens)
                if external_page is not None:
                    return external_page
                if any(token in page.url for token in host_tokens):
                    return page
                await page.wait_for_timeout(500)

        raise ScraperError(f"Could not open external platform page from {wrapper_url}")

    async def _open_pearson_course_page(self, page: Page, wrapper_url: str) -> Page:
        await page.goto(wrapper_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2_000)
        if any(token in page.url for token in PEARSON_HOST_TOKENS):
            return page

        open_link = page.get_by_text("Open in New Window").first
        if await open_link.count() == 0:
            raise ScraperError(f"Pearson wrapper did not expose an Open in New Window link: {wrapper_url}")

        try:
            async with page.context.expect_page(timeout=self.settings.browser_timeout_ms) as popup_info:
                await open_link.click()
            popup = await popup_info.value
            await popup.wait_for_load_state("domcontentloaded")
        except PlaywrightTimeoutError as error:
            raise ScraperError(f"Pearson launch did not open a new window for {wrapper_url}") from error

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            external_page = await self._find_open_page(page.context.pages, PEARSON_HOST_TOKENS)
            if external_page is not None:
                return external_page
            await page.wait_for_timeout(500)

        raise ScraperError(f"Could not find Pearson course page after launching {wrapper_url}")

    async def _wait_for_pearson_rows(self, page: Page) -> None:
        rows = page.locator("li.assignment-row")
        best_count = 0
        stable_checks = 0
        previous_count = -1
        deadline = time.monotonic() + 10

        while time.monotonic() < deadline:
            count = await rows.count()
            if count > best_count:
                best_count = count
                stable_checks = 0
            elif count == previous_count and count > 0:
                stable_checks += 1

            if best_count > 0 and stable_checks >= 2:
                return

            previous_count = count
            await page.wait_for_timeout(1_000)

        if best_count == 0:
            await self._wait_for_selector(page, "li.assignment-row")

    async def _find_open_page(self, pages: list[Page], host_tokens: tuple[str, ...]) -> Page | None:
        for candidate in reversed(pages):
            if any(token in candidate.url for token in host_tokens):
                try:
                    await candidate.wait_for_load_state("domcontentloaded", timeout=5_000)
                except PlaywrightTimeoutError:
                    logger.info("external.page_still_loading", url=candidate.url)
                return candidate
        return None

    async def _wait_for_selector(self, page: Page, selector: str) -> None:
        try:
            await page.locator(selector).first.wait_for(timeout=self.settings.browser_timeout_ms)
        except PlaywrightTimeoutError as error:
            screenshot_name = f"external-{slugify(selector)}.png"
            try:
                await self.save_screenshot(page, screenshot_name)
            except Exception:
                pass
            raise ScraperError(f"Timed out waiting for selector {selector} on {page.url}") from error

    async def _close_popup_if_needed(self, source_page: Page, external_page: Page) -> None:
        if external_page != source_page and not external_page.is_closed():
            await external_page.close()


def parse_cengage_assignment(
    course_id: str,
    title: str,
    row_text: str,
    points_text: str | None,
    activity_class: str,
    external_url: str | None,
    description: str | None,
) -> ExternalAssignmentSnapshot | None:
    cleaned_title = clean_cengage_title(title or row_text)
    if not cleaned_title:
        return None

    points_earned, points_possible = parse_points(points_text)
    grade_pct = None
    if points_earned is not None and points_possible not in (None, 0):
        grade_pct = round((points_earned / points_possible) * 100, 2)

    return ExternalAssignmentSnapshot(
        id=build_external_assignment_id("cengage", course_id, external_url or cleaned_title),
        course_id=course_id,
        source_platform="cengage_mindtap",
        title=cleaned_title,
        type=infer_assignment_type(cleaned_title),
        status=parse_cengage_status(row_text=row_text, activity_class=activity_class),
        external_url=external_url,
        description=description,
        due_at=None,
        due_text=None,
        points_earned=points_earned,
        points_possible=points_possible,
        grade_pct=grade_pct,
        estimated_minutes=parse_estimated_minutes(row_text),
        raw_source={
            "row_text": row_text,
            "points_text": points_text,
            "activity_class": activity_class,
        },
    )


def parse_pearson_assignment(
    course_id: str,
    title: str,
    row_text: str,
    external_url: str | None,
    timezone: str,
) -> ExternalAssignmentSnapshot | None:
    cleaned_title = _clean_text(title)
    if not cleaned_title:
        return None

    due_match = PEARSON_DUE_RE.search(row_text)
    due_text = due_match.group(0) if due_match is not None else None
    due_at = parse_pearson_due_text(due_text, timezone=timezone)
    description = clean_pearson_description(row_text=row_text, title=cleaned_title, due_text=due_text)
    identifier = cleaned_title if due_text is None else f"{cleaned_title}-{due_text}"

    return ExternalAssignmentSnapshot(
        id=build_external_assignment_id("pearson", course_id, identifier),
        course_id=course_id,
        source_platform="pearson_mylab",
        title=cleaned_title,
        type=infer_assignment_type(cleaned_title),
        status=parse_pearson_status(row_text),
        external_url=external_url,
        description=description or None,
        due_at=due_at,
        due_text=due_text,
        points_earned=None,
        points_possible=None,
        grade_pct=None,
        estimated_minutes=parse_estimated_minutes(row_text),
        raw_source={
            "row_text": row_text,
            "due_text": due_text,
        },
    )


def parse_cengage_status(row_text: str, activity_class: str) -> str:
    lowered = f"{row_text} {activity_class}".lower()
    if "submitted" in lowered or " done " in f" {lowered} ":
        return "completed"
    if "in progress" in lowered or "inprogress" in lowered:
        return "in_progress"
    if "not started" in lowered:
        return "upcoming"
    return "available"


def parse_pearson_status(row_text: str) -> str:
    lowered = row_text.lower()
    if "complete" in lowered or "completed" in lowered:
        return "completed"
    if "past due" in lowered:
        return "overdue"
    return "upcoming"


def parse_points(value: str | None) -> tuple[float | None, float | None]:
    if not value:
        return None, None

    match = POINTS_RE.search(value)
    if match is None:
        return None, None

    possible = float(match.group("possible"))
    earned_text = match.group("earned")
    earned = None if earned_text == "--" else float(earned_text)
    return earned, possible


def parse_estimated_minutes(value: str) -> int | None:
    match = MINUTES_RE.search(value)
    if match is None:
        return None

    low = int(match.group("low"))
    high = int(match.group("high")) if match.group("high") else low
    return round((low + high) / 2)


def parse_pearson_due_text(value: str | None, timezone: str) -> datetime | None:
    if not value:
        return None

    local_due_at = datetime.strptime(value, "%m/%d/%Y %I:%M %p").replace(
        tzinfo=ZoneInfo(timezone)
    )
    return local_due_at.astimezone(UTC)


def clean_cengage_title(value: str) -> str:
    cleaned = _clean_text(value)
    markers = (" Submitted", " Not started", " In progress", " COUNTS TOWARDS GRADE")
    for marker in markers:
        if marker in cleaned:
            cleaned = cleaned.split(marker, 1)[0].strip()
    return cleaned


def clean_pearson_description(row_text: str, title: str, due_text: str | None) -> str:
    cleaned = _clean_text(row_text)
    if cleaned.startswith(title):
        cleaned = cleaned[len(title) :].strip()
    if due_text and cleaned.startswith(due_text):
        cleaned = cleaned[len(due_text) :].strip()
    return cleaned


def build_external_assignment_id(prefix: str, course_id: str, value: str) -> str:
    slug = slugify(value)[:20]
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}-{course_id}-{slug}-{digest}"


async def _first_text(row: Locator, selector: str) -> str:
    locator = row.locator(selector).first
    if await locator.count() == 0:
        return ""
    return await locator.text_content() or ""


async def _first_attribute(row: Locator, selector: str, attribute: str) -> str | None:
    locator = row.locator(selector).first
    if await locator.count() == 0:
        return None
    return await locator.get_attribute(attribute)


def _clean_text(value: str) -> str:
    return " ".join(value.split())
