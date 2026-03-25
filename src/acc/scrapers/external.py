import asyncio
import time
import hashlib
import re
from datetime import UTC, datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import structlog
from playwright.async_api import (
    Error as PlaywrightError,
    Frame,
    Locator,
    Page,
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
from acc.scrapers.fetch_limiter import goto_throttled
from acc.scrapers.d2l import AUTHENTICATED_SELECTORS
from acc.scrapers.snapshots import (
    D2LContentTopic,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
    ExternalAssignmentSnapshot,
    ExternalCourseSnapshot,
    ExternalScrapeSnapshot,
)
from acc.scrapers.utils import find_first_visible

logger = structlog.get_logger(__name__)

CENGAGE_HOST_TOKENS = ("cengage.com",)
PEARSON_HOST_TOKENS = ("pearson.com", "pearsoned.com", "pearsoncmg.com")
PEARSON_DUE_RE = re.compile(r"\b\d{2}/\d{2}/(?:\d{2}|\d{4})\s+\d{1,2}:\d{2}\s*[APap][Mm]\b")
PEARSON_ASSIGNMENT_ID_RE = re.compile(r"\((?P<assignment_id>\d+)[,)]")
POINTS_RE = re.compile(
    r"(?P<earned>--|\d+(?:\.\d+)?)\s*/\s*(?P<possible>\d+(?:\.\d+)?)\s*points?",
    re.IGNORECASE,
)
SCORE_PERCENT_RE = re.compile(r"(?P<percent>\d+(?:\.\d+)?)%")
MINUTES_RE = re.compile(r"about\s+(?P<low>\d+)(?:-(?P<high>\d+))?\s+minutes", re.IGNORECASE)
PEARSON_ASSIGNMENT_HEADER_SELECTOR = "th.assignmentlink, th.assignmentNameColumn"
CENGAGE_INLINE_DUE_RE = re.compile(
    r"\b(\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})\s+\d{1,2}:\d{2}\s*[AP]M)\b",
    re.IGNORECASE,
)


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
            browser = await playwright.chromium.launch(
                headless=self.settings.browser_headless,
                slow_mo=self.settings.browser_slow_mo_ms,
            )
            storage_state_path = str(self.settings.d2l_storage_state_path)
            auth_context = await browser.new_context(storage_state=storage_state_path)
            try:
                page = await auth_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await goto_throttled(page, self.settings.d2l_base_url, self.settings)
                if not await self._is_authenticated(page):
                    raise ScraperError(
                        "Saved D2L session is no longer authenticated. Run `acc d2l-login`."
                    )
            finally:
                await auth_context.close()

            work_items: list[tuple[int, D2LCourseSnapshot, D2LContentTopic, str]] = []
            for index, course in enumerate(dashboard_snapshot.courses):
                topic = choose_primary_external_tool(course.external_tools)
                if topic is None:
                    continue
                platform = detect_external_platform_topic(topic)
                if platform not in {"cengage_mindtap", "pearson_mylab"}:
                    continue
                work_items.append((index, course, topic, platform))

            course_snapshots: list[ExternalCourseSnapshot] = []
            assignment_snapshots: list[ExternalAssignmentSnapshot] = []

            if work_items:
                concurrency = max(1, min(len(work_items), self.settings.browser_course_concurrency))
                sem = asyncio.Semaphore(concurrency)

                async def scrape_one(
                    item: tuple[int, D2LCourseSnapshot, D2LContentTopic, str],
                ) -> tuple[int, ExternalCourseSnapshot, list[ExternalAssignmentSnapshot]]:
                    index, course, topic, platform = item
                    async with sem:
                        ctx = await browser.new_context(storage_state=storage_state_path)
                        try:
                            worker = await ctx.new_page()
                            worker.set_default_timeout(self.settings.browser_timeout_ms)
                            course_id = build_course_id(course)
                            launch_url = topic.launch_url or topic.url
                            snapshot_row = ExternalCourseSnapshot(
                                course_id=course_id,
                                source_platform=platform,
                                launch_url=launch_url,
                                title=course.name,
                            )
                            if platform == "cengage_mindtap":
                                assignments = await self._scrape_cengage_course(
                                    page=worker,
                                    course_id=course_id,
                                    wrapper_url=topic.url,
                                    launch_url=launch_url,
                                )
                            else:
                                assignments = await self._scrape_pearson_course(
                                    page=worker,
                                    course_id=course_id,
                                    wrapper_url=topic.url,
                                )
                            logger.info(
                                "external.course_scraped",
                                course_id=course_id,
                                platform=platform,
                                assignments=len(assignments),
                            )
                            return index, snapshot_row, assignments
                        finally:
                            await ctx.close()

                indexed = await asyncio.gather(*[scrape_one(item) for item in work_items])
                for _, snapshot_row, assignments in sorted(indexed, key=lambda row: row[0]):
                    course_snapshots.append(snapshot_row)
                    assignment_snapshots.extend(assignments)

            await browser.close()

            return ExternalScrapeSnapshot(
                fetched_at=datetime.now(UTC),
                courses=course_snapshots,
                assignments=assignment_snapshots,
            )

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
        rows = await self._wait_for_cengage_activity_rows(course_page)
        assignments: list[ExternalAssignmentSnapshot] = []
        seen_ids: set[str] = set()

        for row in await rows.all():
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
                timezone=self.settings.timezone,
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
        if await self._maybe_open_pearson_assignments(course_page):
            await course_page.wait_for_timeout(4_000)

        if await self._has_pearson_rows(course_page):
            await self._wait_for_pearson_rows(course_page)
        else:
            frame = await self._find_pearson_assignments_frame(course_page)
            if frame is not None:
                assignments = await self._parse_pearson_assignment_table(
                    frame=frame,
                    course_id=course_id,
                )
                await self._close_popup_if_needed(page, course_page)
                return assignments
            if await self._is_pearson_course_shell(course_page):
                await self._close_popup_if_needed(page, course_page)
                return []
            await self._wait_for_pearson_rows(course_page)

        assignments = await self._parse_pearson_assignment_list(
            page=course_page,
            course_id=course_id,
        )

        await self._close_popup_if_needed(page, course_page)
        return assignments

    async def _open_external_page_from_navigation(
        self,
        page: Page,
        wrapper_url: str,
        host_tokens: tuple[str, ...],
        direct_fallback_url: str | None = None,
    ) -> Page:
        await goto_throttled(page, wrapper_url, self.settings)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            external_page = await self._find_open_page(page.context.pages, host_tokens)
            if external_page is not None:
                return external_page
            if any(token in page.url for token in host_tokens):
                return page
            await page.wait_for_timeout(500)

        if direct_fallback_url and any(token in direct_fallback_url for token in host_tokens):
            await goto_throttled(page, direct_fallback_url, self.settings)
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
        await goto_throttled(page, wrapper_url, self.settings)
        await page.wait_for_timeout(2_000)
        if is_pearson_url(page.url):
            return await self._complete_pearson_launch(page, wrapper_url=wrapper_url)

        open_link = page.get_by_text("Open in New Window").first
        if await open_link.count() == 0:
            raise ScraperError(f"Pearson wrapper did not expose an Open in New Window link: {wrapper_url}")

        launch_error: Exception | None = None
        try:
            async with page.context.expect_page(timeout=self.settings.browser_timeout_ms) as popup_info:
                await open_link.click()
            popup = await popup_info.value
            await popup.wait_for_load_state("domcontentloaded")
            launched_page = popup
        except PlaywrightTimeoutError as error:
            launch_error = error
            logger.info(
                "external.pearson_launch_reused_page",
                wrapper_url=wrapper_url,
                current_url=page.url,
            )
            launched_page = page

        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            external_page = await self._find_open_page(page.context.pages, PEARSON_HOST_TOKENS)
            if external_page is not None:
                return await self._complete_pearson_launch(external_page, wrapper_url=wrapper_url)
            if is_pearson_url(launched_page.url):
                return await self._complete_pearson_launch(launched_page, wrapper_url=wrapper_url)
            await page.wait_for_timeout(500)

        raise ScraperError(f"Could not find Pearson course page after launching {wrapper_url}") from launch_error

    async def _complete_pearson_launch(self, page: Page, *, wrapper_url: str) -> Page:
        deadline = time.monotonic() + 45
        current_page = page

        while time.monotonic() < deadline:
            external_page = await self._find_open_page(current_page.context.pages, PEARSON_HOST_TOKENS)
            if external_page is not None:
                current_page = external_page

            if await self._maybe_accept_pearson_cookies(current_page):
                await current_page.wait_for_timeout(1_000)
                continue

            if await self._maybe_click_pearson_access_now(current_page):
                await current_page.wait_for_timeout(4_000)
                continue

            if await self._maybe_open_pearson_mylab(current_page):
                await current_page.wait_for_timeout(4_000)
                continue

            if await self._maybe_open_pearson_assignments(current_page):
                await current_page.wait_for_timeout(4_000)
                continue

            if await self._has_pearson_rows(current_page):
                return current_page

            if await self._is_pearson_course_shell(current_page):
                return current_page

            await current_page.wait_for_timeout(1_000)

        raise ScraperError(f"Could not find Pearson course page after launching {wrapper_url}")

    async def _maybe_accept_pearson_cookies(self, page: Page) -> bool:
        for selector in ("Allow and Continue", "Allow All"):
            if await self._click_first_visible_text(page, selector):
                return True
        return False

    async def _maybe_click_pearson_access_now(self, page: Page) -> bool:
        if await self._click_first_visible_text(page, "Access Now"):
            return True
        return False

    async def _maybe_open_pearson_mylab(self, page: Page) -> bool:
        buttons = page.locator("button")
        for index in range(await buttons.count()):
            button = buttons.nth(index)
            label = _clean_text(await button.inner_text())
            if not label.startswith("Open MyLab & Mastering"):
                continue
            try:
                if not await button.is_visible():
                    continue
                try:
                    async with page.context.expect_page(timeout=5_000):
                        await button.click(force=True)
                except PlaywrightTimeoutError:
                    await button.click(force=True)
                return True
            except (PlaywrightTimeoutError, PlaywrightError):
                continue
        return False

    async def _has_pearson_rows(self, page: Page) -> bool:
        for selector in ("li.assignment-row", "div.assignment-row", "tr.assignment-row"):
            if await page.locator(selector).count() > 0:
                return True
        return False

    async def _find_pearson_assignments_frame(self, page: Page) -> Frame | None:
        matched_frame: Frame | None = None
        deadline = time.monotonic() + 12

        while time.monotonic() < deadline:
            for frame in reversed(page.frames):
                if not is_pearson_assignments_frame_url(frame.url):
                    continue
                matched_frame = frame
                if await frame.locator("tr").count() > 0:
                    return frame
            if matched_frame is not None:
                return matched_frame
            await page.wait_for_timeout(1_000)

        return matched_frame

    async def _maybe_open_pearson_assignments(self, page: Page) -> bool:
        if "mylabmastering.pearson.com" not in page.url:
            return False
        title = await self._page_title(page)
        if title == "Assignments":
            return False
        return await self._click_first_visible_text(page, "Assignments")

    async def _click_first_visible_text(self, page: Page, text: str) -> bool:
        locator = page.get_by_text(text)
        count = await locator.count()
        for index in range(count):
            candidate = locator.nth(index)
            try:
                if not await candidate.is_visible():
                    continue
                await candidate.click(force=True)
                return True
            except (PlaywrightTimeoutError, PlaywrightError):
                continue
        return False

    async def _is_pearson_course_shell(self, page: Page) -> bool:
        if "mylabmastering.pearson.com" not in page.url:
            return False
        title = await self._page_title(page)
        return title in {"Course Home", "Assignments"}

    async def _page_title(self, page: Page) -> str:
        try:
            return await page.title()
        except PlaywrightError:
            return ""

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

    async def _parse_pearson_assignment_list(
        self,
        page: Page,
        course_id: str,
    ) -> list[ExternalAssignmentSnapshot]:
        rows = page.locator("li.assignment-row")
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
                external_url=urljoin(page.url, href) if href else None,
                timezone=self.settings.timezone,
            )
            if assignment is None or assignment.id in seen_ids:
                continue
            assignments.append(assignment)
            seen_ids.add(assignment.id)

        return assignments

    async def _parse_pearson_assignment_table(
        self,
        frame: Frame,
        course_id: str,
    ) -> list[ExternalAssignmentSnapshot]:
        rows = frame.locator("tr")
        assignments: list[ExternalAssignmentSnapshot] = []
        seen_ids: set[str] = set()

        for index in range(await rows.count()):
            row = rows.nth(index)
            link_cell = row.locator(PEARSON_ASSIGNMENT_HEADER_SELECTOR).first
            if await link_cell.count() == 0:
                continue

            title = _clean_text(await link_cell.locator("a").first.text_content() or "")
            assignment_kind = _clean_text(await _first_text(link_cell, ".readableButHidden"))
            due_text = _clean_text(await row.locator("td").first.inner_text() or "")
            due_class = await _first_attribute(row, "td div, td", "class")
            href = await _first_attribute(link_cell, "a", "href")
            score_text = await self._resolve_pearson_score_text(row, timeout_parent=frame)
            row_text = _clean_text(await row.inner_text())
            assignment = parse_pearson_assignment_table_row(
                course_id=course_id,
                title=title or row_text,
                assignment_kind=assignment_kind,
                row_text=row_text,
                due_text=due_text or None,
                due_class=due_class,
                score_text=score_text,
                href=href,
                timezone=self.settings.timezone,
            )
            if assignment is None or assignment.id in seen_ids:
                continue
            assignments.append(assignment)
            seen_ids.add(assignment.id)

        return assignments

    async def _resolve_pearson_score_text(self, row: Locator, *, timeout_parent: Frame | Page) -> str | None:
        td_last = row.locator("td").last
        if await td_last.count() > 0:
            score_cell = td_last
        else:
            score_cell = row

        score_text = _clean_text(await score_cell.inner_text() or "")
        if "see score" not in score_text.lower():
            return score_text or None

        anchor = score_cell.locator("a").first
        if await anchor.count() == 0:
            anchor = row.locator("a").filter(has_text=re.compile(r"see\s+score", re.IGNORECASE)).first
        if await anchor.count() == 0:
            return score_text or None

        try:
            await anchor.click(force=True)
        except (PlaywrightTimeoutError, PlaywrightError):
            return score_text or None

        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            updated_text = _clean_text(await score_cell.inner_text() or "")
            if updated_text and "see score" not in updated_text.lower():
                return updated_text
            await timeout_parent.wait_for_timeout(250)

        return score_text or None

    async def _find_open_page(self, pages: list[Page], host_tokens: tuple[str, ...]) -> Page | None:
        for candidate in reversed(pages):
            if any(token in candidate.url for token in host_tokens):
                try:
                    await candidate.wait_for_load_state("domcontentloaded", timeout=5_000)
                except PlaywrightTimeoutError:
                    logger.info("external.page_still_loading", url=candidate.url)
                return candidate
        return None

    async def _wait_for_cengage_activity_rows(self, page: Page) -> Locator:
        """MindTap often renders `li.activities-wrapper` in the main shell or inside an iframe."""
        selector = "li.activities-wrapper"
        budget_ms = max(
            self.settings.browser_timeout_ms,
            self.settings.cengage_activity_timeout_ms,
        )
        deadline = time.monotonic() + budget_ms / 1000.0
        last_exc: PlaywrightTimeoutError | None = None

        try:
            await page.wait_for_load_state("load", timeout=min(25_000, budget_ms))
        except PlaywrightTimeoutError:
            pass

        while time.monotonic() < deadline:
            remaining_ms = max(500, int((deadline - time.monotonic()) * 1000))
            per_try = min(10_000, remaining_ms)
            for frame in page.frames:
                loc = frame.locator(selector)
                try:
                    await loc.first.wait_for(state="attached", timeout=per_try)
                    if await loc.count() > 0:
                        return loc
                except PlaywrightTimeoutError as exc:
                    last_exc = exc
                    continue
            await asyncio.sleep(0.4)

        try:
            await self.save_screenshot(page, "external-cengage-activities-timeout.png")
        except Exception:
            pass
        err = ScraperError(
            f"Timed out waiting for MindTap activity list ({selector}) on {page.url}"
        )
        if last_exc is not None:
            raise err from last_exc
        raise err

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


def is_pearson_url(url: str) -> bool:
    return any(token in url for token in PEARSON_HOST_TOKENS)


def is_pearson_assignments_frame_url(url: str) -> bool:
    return "mylab.pearson.com/courses/" in url and "/assignments" in url


def parse_cengage_due_from_row(row_text: str | None, *, timezone: str) -> tuple[datetime | None, str | None]:
    if not row_text:
        return None, None
    match = CENGAGE_INLINE_DUE_RE.search(row_text)
    if match is None:
        return None, None
    due_text = match.group(1)
    due_at = parse_pearson_due_text(due_text, timezone)
    return due_at, due_text


def parse_cengage_assignment(
    course_id: str,
    title: str,
    row_text: str,
    points_text: str | None,
    activity_class: str,
    external_url: str | None,
    description: str | None,
    *,
    timezone: str = "America/Chicago",
) -> ExternalAssignmentSnapshot | None:
    cleaned_title = clean_cengage_title(title or row_text)
    if not cleaned_title:
        return None

    points_earned, points_possible = parse_points(points_text)
    grade_pct = None
    if points_earned is not None and points_possible not in (None, 0):
        grade_pct = round((points_earned / points_possible) * 100, 2)

    due_at, due_text = parse_cengage_due_from_row(row_text, timezone=timezone)

    return ExternalAssignmentSnapshot(
        id=build_external_assignment_id("cengage", course_id, external_url or cleaned_title),
        course_id=course_id,
        source_platform="cengage_mindtap",
        title=cleaned_title,
        type=infer_assignment_type(cleaned_title),
        status=parse_cengage_status(
            row_text=row_text,
            activity_class=activity_class,
            points_earned=points_earned,
        ),
        external_url=external_url,
        description=description,
        due_at=due_at,
        due_text=due_text,
        points_earned=points_earned,
        points_possible=points_possible,
        grade_pct=grade_pct,
        estimated_minutes=parse_estimated_minutes(row_text),
        raw_source={
            "row_text": row_text,
            "points_text": points_text,
            "activity_class": activity_class,
            "due_text": due_text,
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
    points_earned, points_possible = parse_points(row_text)
    grade_pct = assignment_grade_pct_from_points(points_earned, points_possible)

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
        points_earned=points_earned,
        points_possible=points_possible,
        grade_pct=grade_pct,
        estimated_minutes=parse_estimated_minutes(row_text),
        raw_source={
            "row_text": row_text,
            "due_text": due_text,
        },
    )


def parse_pearson_assignment_table_row(
    course_id: str,
    title: str,
    assignment_kind: str,
    row_text: str,
    due_text: str | None,
    due_class: str | None,
    score_text: str | None,
    href: str | None,
    timezone: str,
) -> ExternalAssignmentSnapshot | None:
    cleaned_title = _clean_text(title)
    if not cleaned_title:
        return None

    cleaned_due_text = _clean_text(due_text or "")
    due_at = parse_pearson_due_text(cleaned_due_text, timezone=timezone)
    identifier = extract_pearson_assignment_identifier(href)
    if not identifier:
        identifier = cleaned_title if not cleaned_due_text else f"{cleaned_title}-{cleaned_due_text}"
    points_earned, points_possible, grade_pct = parse_pearson_score_text(score_text or row_text)

    return ExternalAssignmentSnapshot(
        id=build_external_assignment_id("pearson", course_id, identifier),
        course_id=course_id,
        source_platform="pearson_mylab",
        title=cleaned_title,
        type=infer_assignment_type(f"{assignment_kind} {cleaned_title}".strip()),
        status=parse_pearson_table_status(
            row_text=row_text,
            due_class=due_class,
            due_at=due_at,
            score_text=score_text,
        ),
        external_url=None,
        description=clean_pearson_description(
            row_text=row_text,
            title=cleaned_title,
            due_text=cleaned_due_text or None,
        )
        or None,
        due_at=due_at,
        due_text=cleaned_due_text or None,
        points_earned=points_earned,
        points_possible=points_possible,
        grade_pct=grade_pct,
        estimated_minutes=parse_estimated_minutes(row_text),
        raw_source={
            "row_text": row_text,
            "due_text": cleaned_due_text or None,
            "due_class": due_class,
            "score_text": score_text,
            "href": href,
            "assignment_kind": assignment_kind or None,
        },
    )


def parse_cengage_status(
    row_text: str,
    activity_class: str,
    points_earned: float | None = None,
) -> str:
    lowered = f"{row_text} {activity_class}".lower()
    if points_earned is not None and points_earned > 0:
        return "completed"
    if "submitted" in lowered or " done " in f" {lowered} ":
        return "completed"
    if "in progress" in lowered or "inprogress" in lowered:
        return "in_progress"
    if points_earned is not None and "not started" not in lowered:
        return "completed"
    if "not started" in lowered:
        return "upcoming"
    return "available"


def parse_pearson_status(row_text: str) -> str:
    if parse_pearson_score_text(row_text)[2] is not None:
        return "completed"
    lowered = row_text.lower()
    if "incomplete" in lowered:
        return "in_progress"
    if "complete" in lowered or "completed" in lowered:
        return "completed"
    if "past due" in lowered:
        return "overdue"
    return "upcoming"


def parse_pearson_table_status(
    row_text: str,
    due_class: str | None,
    due_at: datetime | None,
    score_text: str | None = None,
) -> str:
    _, _, grade_pct = parse_pearson_score_text(score_text or row_text)
    if grade_pct is not None:
        if grade_pct == 0 and due_at is not None and due_at >= datetime.now(UTC):
            return "upcoming"
        return "completed"
    lowered_due_class = (due_class or "").lower()
    if "past due" in lowered_due_class:
        return "overdue"
    status = parse_pearson_status(row_text)
    if status != "upcoming":
        return status
    if due_at is not None and due_at < datetime.now(UTC):
        return "overdue"
    return status


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


def assignment_grade_pct_from_points(
    points_earned: float | None,
    points_possible: float | None,
) -> float | None:
    if points_earned is None or points_possible in (None, 0):
        return None
    return round((points_earned / points_possible) * 100, 2)


def parse_pearson_score_text(value: str | None) -> tuple[float | None, float | None, float | None]:
    if not value:
        return None, None, None

    points_earned, points_possible = parse_points(value)
    if points_possible is not None:
        return points_earned, points_possible, assignment_grade_pct_from_points(
            points_earned,
            points_possible,
        )

    percent_match = SCORE_PERCENT_RE.search(value)
    if percent_match is not None:
        grade_pct = float(percent_match.group("percent"))
        return None, None, grade_pct

    return None, None, None


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

    normalized = _clean_text(value).upper()
    for date_format in (
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %I:%M%p",
        "%m/%d/%y %I:%M %p",
        "%m/%d/%y %I:%M%p",
    ):
        try:
            local_due_at = datetime.strptime(normalized, date_format).replace(
                tzinfo=ZoneInfo(timezone)
            )
            return local_due_at.astimezone(UTC)
        except ValueError:
            continue
    return None


def extract_pearson_assignment_identifier(value: str | None) -> str | None:
    if not value:
        return None
    match = PEARSON_ASSIGNMENT_ID_RE.search(value)
    if match is None:
        return None
    return match.group("assignment_id")


def clean_cengage_title(value: str) -> str:
    cleaned = _clean_text(value)
    cleaned = re.sub(
        r"(?<=[\d.])(?=In progress\b|Not started\b|Submitted\b|COUNTS TOWARDS GRADE\b)",
        " ",
        cleaned,
        flags=re.IGNORECASE,
    )
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
