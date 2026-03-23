import time
from dataclasses import dataclass
from datetime import UTC, datetime
import re
from urllib.parse import urljoin, urlparse

import structlog
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from acc.scrapers.base import BaseScraper, ScraperError
from acc.scrapers.snapshots import (
    D2LContentTopic,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
    D2LGradeRow,
    D2LGradeSummary,
    D2LToolLink,
    D2LUpcomingEvent,
)
from acc.scrapers.utils import click_first, fill_first, find_first_visible

logger = structlog.get_logger(__name__)

AUTHENTICATED_SELECTORS = (
    "text=/my courses/i",
    "text=/course home/i",
    "nav[role='navigation'] a[href*='/d2l/home']",
    "[data-testid='course-selector']",
    "a[href*='/d2l/lms/dropbox']",
)

USERNAME_SELECTORS = (
    "input[name='username']",
    "input[name='UserName']",
    "input[name='identifier']",
    "input[name='loginfmt']",
    "input[type='email']",
    "input#i0116",
    "input#username",
    "input#userNameInput",
)

PASSWORD_SELECTORS = (
    "input[name='password']",
    "input[name='passwd']",
    "input[type='password']",
    "input#i0118",
    "input#password",
    "input#passwordInput",
)

SUBMIT_SELECTORS = (
    "button[type='submit']",
    "input[type='submit']",
    "button#idSIButton9",
    "input#idSIButton9",
    "button:has-text('Sign in')",
    "button:has-text('Log in')",
    "button:has-text('Login')",
    "button:has-text('Next')",
)

LOGIN_LINK_SELECTORS = (
    "a:has-text('Log in')",
    "a:has-text('Login')",
    "button:has-text('Log in')",
    "button:has-text('Login')",
)

MFA_SELECTORS = (
    "text=/multi-factor/i",
    "text=/verification code/i",
    "text=/check your phone/i",
    "text=/duo/i",
    "iframe[title*='Duo']",
)

COURSE_LINK_RE = re.compile(r"/d2l/home/(?P<course_id>\d+)")
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\s?(?:AM|PM)\b", re.IGNORECASE)
CONTENT_TYPE_RE = re.compile(r"^'.*' - (?P<content_type>.+)$")
TOOL_LINK_NAMES = (
    "Content",
    "Discussions",
    "Assignments",
    "Quizzes / Exams",
    "Grades",
    "Checklist",
    "Classlist",
    "Groups",
)
CONTENT_MODULE_SELECTOR = "a.d2l-le-TreeAccordionItem-anchor"
CONTENT_TOPIC_SELECTOR = "a[href*='/viewContent/']"
SYLLABUS_KEYWORDS = ("syll", "course outline")
EXTERNAL_MODULE_KEYWORDS = ("cengage", "mindtap", "mastering", "pearson", "mylab")
IRRELEVANT_FRAME_HOSTS = {
    "s.brightspace.com",
    "service.force.com",
    "solve-widget.forethought.ai",
    "d2l.my.site.com",
}
GENERIC_FRAME_PREFIXES = (
    "Toggle Sidebar Find Previous Next of",
    "Thumbnails Document Outline Attachments",
)
DOCUMENT_TEXT_MARKERS = (
    "OAKTON",
    "Oakton",
    "COURSE SYLLABUS",
    "Course Syllabus",
    "SYLLABUS",
    "Syllabus",
)
MAX_EXTRACTED_TEXT_CHARS = 20_000


@dataclass(slots=True)
class AuthResult:
    authenticated: bool
    used_saved_session: bool = False


class D2LScraper(BaseScraper):
    async def login(self, force: bool = False) -> AuthResult:
        self.settings.ensure_runtime_dirs()

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright, force=force)
            page: Page | None = None
            try:
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await page.goto(self.settings.d2l_base_url, wait_until="domcontentloaded")

                if await self._is_authenticated(page):
                    logger.info("d2l.saved_session_valid")
                    await self._persist_state(browser_context)
                    return AuthResult(authenticated=True, used_saved_session=True)

                await click_first(page, LOGIN_LINK_SELECTORS, 1_000)
                await self._try_autofill_login(page)
                await self._wait_for_authenticated(page)

                await self._persist_state(browser_context)
                logger.info("d2l.login_success")
                return AuthResult(authenticated=True, used_saved_session=False)
            except Exception:
                screenshot = None
                if page is not None:
                    try:
                        screenshot = await self.save_screenshot(page, "d2l-login-failure.png")
                    except Exception:
                        screenshot = None
                logger.exception("d2l.login_failed", screenshot=str(screenshot) if screenshot else None)
                raise
            finally:
                await browser_context.close()
                await browser.close()

    async def check_saved_session(self) -> bool:
        if not self.settings.d2l_storage_state_path.exists():
            logger.info("d2l.no_saved_session")
            return False

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright, force=False)
            try:
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await page.goto(self.settings.d2l_base_url, wait_until="domcontentloaded")
                authenticated = await self._is_authenticated(page)
                logger.info("d2l.saved_session_checked", authenticated=authenticated)
                return authenticated
            finally:
                await browser_context.close()
                await browser.close()

    async def save_snapshot(self, limit_courses: int | None = None) -> D2LDashboardSnapshot:
        snapshot = await self.snapshot(limit_courses=limit_courses)
        output_path = self.settings.d2l_snapshot_path
        output_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
        logger.info("d2l.snapshot_saved", path=str(output_path), courses=len(snapshot.courses))
        return snapshot

    async def snapshot(self, limit_courses: int | None = None) -> D2LDashboardSnapshot:
        if not self.settings.d2l_storage_state_path.exists():
            raise ScraperError("No saved D2L session found. Run `acc d2l-login` first.")

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright, force=False)
            try:
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await page.goto(self.settings.d2l_base_url, wait_until="domcontentloaded")
                await self._wait_for_course_cards(page)

                if not await self._is_authenticated(page):
                    raise ScraperError("Saved D2L session is no longer authenticated. Run `acc d2l-login`.")

                source_url = page.url
                courses = await self._scrape_courses(page)
                if limit_courses is not None:
                    courses = courses[:limit_courses]

                for course in courses:
                    await self._scrape_course_home(page, course)

                return D2LDashboardSnapshot(
                    fetched_at=datetime.now(UTC),
                    source_url=source_url,
                    courses=courses,
                )
            finally:
                await browser_context.close()
                await browser.close()

    async def _new_context(self, playwright: Playwright, force: bool) -> tuple[Browser, BrowserContext]:
        storage_state = None
        if not force and self.settings.d2l_storage_state_path.exists():
            storage_state = str(self.settings.d2l_storage_state_path)

        browser = await playwright.chromium.launch(
            headless=self.settings.browser_headless,
            slow_mo=self.settings.browser_slow_mo_ms,
        )
        context = await browser.new_context(storage_state=storage_state)
        return browser, context

    async def _scrape_courses(self, page: Page) -> list[D2LCourseSnapshot]:
        course_links = page.locator("a[href*='/d2l/home/']")
        seen_ids: set[str] = set()
        courses: list[D2LCourseSnapshot] = []

        for index in range(await course_links.count()):
            link = course_links.nth(index)
            href = await link.get_attribute("href")
            text = self._clean_text(await link.inner_text())
            if not href or " - " not in text:
                continue

            course_id = extract_course_id(href)
            if course_id is None or course_id in seen_ids:
                continue

            course = parse_course_link_text(text, course_id, self._absolute_url(href))
            courses.append(course)
            seen_ids.add(course_id)

        logger.info("d2l.courses_scraped", count=len(courses))
        return courses

    async def _wait_for_course_cards(self, page: Page) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if await page.locator("a[href*='/d2l/home/']").count() > 0:
                return
            await page.wait_for_timeout(500)

    async def _scrape_course_home(self, page: Page, course: D2LCourseSnapshot) -> None:
        await page.goto(course.home_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1_500)
        course.tool_links = await self._scrape_tool_links(page)
        course.upcoming_events = await self._scrape_upcoming_events(page)
        content_url = next((tool.url for tool in course.tool_links if tool.name == "Content"), None)
        if content_url is not None:
            syllabus_topics, external_tools = await self._scrape_content_resources(page, content_url)
            course.syllabus_topics = syllabus_topics
            course.external_tools = external_tools
        grades_url = next((tool.url for tool in course.tool_links if tool.name == "Grades"), None)
        if grades_url is not None:
            grade_summary, grade_rows = await self._scrape_grades(page, grades_url)
            course.final_calculated_grade = grade_summary
            course.grade_rows = grade_rows

    async def _scrape_tool_links(self, page: Page) -> list[D2LToolLink]:
        links = page.locator("a")
        tool_links: list[D2LToolLink] = []
        seen: set[tuple[str, str]] = set()

        for index in range(await links.count()):
            link = links.nth(index)
            name = self._clean_text(await link.inner_text())
            href = await link.get_attribute("href")
            if name not in TOOL_LINK_NAMES or not href:
                continue

            absolute_url = self._absolute_url(href)
            key = (name, absolute_url)
            if key in seen:
                continue

            tool_links.append(D2LToolLink(name=name, url=absolute_url))
            seen.add(key)

        return tool_links

    async def _scrape_upcoming_events(self, page: Page) -> list[D2LUpcomingEvent]:
        items = page.locator("li.d2l-datalist-item-actionable")
        events: list[D2LUpcomingEvent] = []

        for index in range(await items.count()):
            item = items.nth(index)
            action_link = item.locator("a.d2l-datalist-item-actioncontrol").first
            href = await action_link.get_attribute("href")
            title = await action_link.get_attribute("title")
            if not href or not title or not title.startswith("View Event - "):
                continue

            textblocks = item.locator(".d2l-textblock")
            text_values = [
                self._clean_text(await textblocks.nth(position).inner_text())
                for position in range(await textblocks.count())
            ]
            visible_title = next((value for value in reversed(text_values) if value), None)
            due_text = build_due_text(text_values)
            event_title = normalize_event_title(title.removeprefix("View Event - ").strip())
            if visible_title:
                event_title = normalize_event_title(visible_title)

            events.append(
                D2LUpcomingEvent(
                    title=event_title,
                    due_text=due_text,
                    details_url=self._absolute_url(href),
                )
            )

        logger.info("d2l.events_scraped", count=len(events), url=page.url)
        return events

    async def _scrape_grades(
        self,
        page: Page,
        grades_url: str,
    ) -> tuple[D2LGradeSummary | None, list[D2LGradeRow]]:
        await page.goto(grades_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(1_500)

        grade_summary = await self._scrape_grade_summary(page)
        grade_rows = await self._scrape_grade_rows(page)
        logger.info("d2l.grades_scraped", rows=len(grade_rows), url=page.url)
        return grade_summary, grade_rows

    async def _scrape_grade_summary(self, page: Page) -> D2LGradeSummary | None:
        summary_tables = page.locator("table.d_t")
        count = await summary_tables.count()
        if count < 2:
            return None

        weight_achieved_text = self._clean_text(await summary_tables.nth(0).inner_text())
        grade_text = self._clean_text(await summary_tables.nth(1).inner_text())
        return D2LGradeSummary(
            weight_achieved_text=weight_achieved_text or None,
            grade_text=grade_text or None,
        )

    async def _scrape_grade_rows(self, page: Page) -> list[D2LGradeRow]:
        table = page.locator("table.d2l-table.d2l-grid").first
        if await table.count() == 0:
            return []

        rows = table.locator("tr")
        grade_rows: list[D2LGradeRow] = []
        current_category: str | None = None

        for index in range(1, await rows.count()):
            row = rows.nth(index)
            ths = row.locator("th")
            if await ths.count() == 0:
                continue

            title = self._clean_text(await ths.last.inner_text())
            if not title:
                continue

            cells = [
                self._clean_text(await row.locator("td").nth(cell_index).inner_text())
                for cell_index in range(await row.locator("td").count())
            ]
            if len(cells) == 5:
                row_data = D2LGradeRow(
                    title=title,
                    is_category=False,
                    category_title=current_category,
                    points_text=cells[1] or None,
                    weight_achieved_text=cells[2] or None,
                    grade_text=cells[3] or None,
                )
            elif len(cells) == 4:
                current_category = title
                row_data = D2LGradeRow(
                    title=title,
                    is_category=True,
                    category_title=None,
                    points_text=None,
                    weight_achieved_text=cells[1] or None,
                    grade_text=cells[2] or None,
                )
            else:
                continue

            grade_rows.append(row_data)

        return grade_rows

    async def _scrape_content_resources(
        self,
        page: Page,
        content_url: str,
    ) -> tuple[list[D2LContentTopic], list[D2LContentTopic]]:
        await page.goto(content_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2_000)

        module_titles = await self._list_content_module_titles(page)
        syllabus_topics: dict[str, D2LContentTopic] = {}
        external_tools: dict[str, D2LContentTopic] = {}

        for module_title in module_titles:
            lowered = module_title.lower()
            if any(keyword in lowered for keyword in SYLLABUS_KEYWORDS):
                await self._select_content_module(page, module_title)
                for topic in await self._scrape_visible_content_topics(page, module_title):
                    syllabus_topics.setdefault(topic.url, topic)
            elif any(keyword in lowered for keyword in EXTERNAL_MODULE_KEYWORDS):
                await self._select_content_module(page, module_title)
                for topic in await self._scrape_visible_content_topics(page, module_title):
                    if topic.content_type == "External Learning Tool" or detect_external_platform(
                        topic.title,
                        topic.url,
                    ):
                        external_tools.setdefault(topic.url, topic)

        for topic in syllabus_topics.values():
            topic.extracted_text = await self._extract_document_text(page, topic.url)

        for topic in external_tools.values():
            topic.launch_url = await self._resolve_external_launch_url(page, topic.url)

        logger.info(
            "d2l.content_resources_scraped",
            url=content_url,
            syllabus_topics=len(syllabus_topics),
            external_tools=len(external_tools),
        )
        return list(syllabus_topics.values()), list(external_tools.values())

    async def _list_content_module_titles(self, page: Page) -> list[str]:
        modules = page.locator(CONTENT_MODULE_SELECTOR)
        titles: list[str] = []
        seen: set[str] = set()

        for index in range(await modules.count()):
            module = modules.nth(index)
            title = self._clean_text(await module.inner_text())
            if not title:
                continue
            module_name = title.split(" module:", 1)[0].strip()
            key = module_name.casefold()
            if key in seen:
                continue
            titles.append(module_name)
            seen.add(key)

        return titles

    async def _select_content_module(self, page: Page, module_title: str) -> None:
        module = page.locator(CONTENT_MODULE_SELECTOR, has_text=module_title).first
        if await module.count() == 0:
            raise ScraperError(f"Could not find D2L content module: {module_title}")
        await module.click()
        await page.wait_for_timeout(1_500)

    async def _scrape_visible_content_topics(
        self,
        page: Page,
        module_title: str,
    ) -> list[D2LContentTopic]:
        topics = page.locator(CONTENT_TOPIC_SELECTOR)
        visible_topics: list[D2LContentTopic] = []
        seen: set[str] = set()

        for index in range(await topics.count()):
            topic = topics.nth(index)
            href = await topic.get_attribute("href")
            text = self._clean_text(await topic.inner_text())
            title = await topic.get_attribute("title")
            if not href or not text:
                continue

            absolute_url = self._absolute_url(href)
            if absolute_url in seen:
                continue

            visible_topics.append(
                D2LContentTopic(
                    title=text,
                    url=absolute_url,
                    module_title=module_title,
                    content_type=parse_content_type(title),
                )
            )
            seen.add(absolute_url)

        return visible_topics

    async def _extract_document_text(self, page: Page, topic_url: str) -> str | None:
        await page.goto(topic_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2_000)

        best_text: str | None = None
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            if should_ignore_frame_url(frame.url):
                continue

            try:
                if await frame.locator("body").count() == 0:
                    continue
                text = self._clean_text(await frame.locator("body").inner_text())
            except Exception:
                continue

            if len(text) < 200:
                continue

            for prefix in GENERIC_FRAME_PREFIXES:
                if text.startswith(prefix):
                    text = text[len(prefix) :].strip()
                    break

            text = trim_document_preamble(text)
            if best_text is None or len(text) > len(best_text):
                best_text = text

        if best_text is None:
            return None
        return best_text[:MAX_EXTRACTED_TEXT_CHARS]

    async def _resolve_external_launch_url(self, page: Page, topic_url: str) -> str | None:
        await page.goto(topic_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2_000)

        for frame in page.frames:
            if frame == page.main_frame:
                continue
            if is_external_launch_url(frame.url):
                return frame.url

        links = page.locator("a")
        for index in range(await links.count()):
            link = links.nth(index)
            href = await link.get_attribute("href")
            if not href:
                continue
            absolute_url = self._absolute_url(href)
            if is_external_launch_url(absolute_url):
                return absolute_url

        return topic_url

    async def _is_authenticated(self, page: Page) -> bool:
        locator = await find_first_visible(page, AUTHENTICATED_SELECTORS, 2_000)
        return locator is not None

    async def _try_autofill_login(self, page: Page) -> None:
        username = self.settings.d2l_username
        password = self.settings.d2l_password.get_secret_value() if self.settings.d2l_password else None

        filled_username = False
        filled_password = False
        deadline = time.monotonic() + 15

        while time.monotonic() < deadline:
            if username and not filled_username:
                filled_username = await fill_first(page, USERNAME_SELECTORS, username, 1_000)
                if filled_username:
                    logger.info("d2l.username_filled")
                    await click_first(page, SUBMIT_SELECTORS, 1_000)
                    await page.wait_for_timeout(750)
                    continue

            if password and not filled_password:
                filled_password = await fill_first(page, PASSWORD_SELECTORS, password, 1_000)
                if filled_password:
                    logger.info("d2l.password_filled")
                    await click_first(page, SUBMIT_SELECTORS, 1_000)
                    await page.wait_for_timeout(750)
                    continue

            if await self._is_authenticated(page):
                return

            await page.wait_for_timeout(500)

        if not filled_username or not filled_password:
            logger.info("d2l.manual_login_required")

    async def _wait_for_authenticated(self, page: Page) -> None:
        deadline = time.monotonic() + self.settings.d2l_login_timeout_seconds
        mfa_logged = False

        while time.monotonic() < deadline:
            if await self._is_authenticated(page):
                return

            if not mfa_logged:
                mfa_visible = await find_first_visible(page, MFA_SELECTORS, 500)
                if mfa_visible is not None:
                    logger.info("d2l.mfa_detected")
                    mfa_logged = True

            await page.wait_for_timeout(1_000)

        raise ScraperError(
            "D2L login did not complete before timeout. "
            "Finish any SSO or MFA steps in the browser and try again."
        )

    async def _persist_state(self, browser_context: BrowserContext) -> None:
        await browser_context.storage_state(path=str(self.settings.d2l_storage_state_path))

    def _absolute_url(self, href: str) -> str:
        return urljoin(self.settings.d2l_base_url, href)

    @staticmethod
    def _clean_text(text: str) -> str:
        return " ".join(text.split())


def extract_course_id(href: str) -> str | None:
    match = COURSE_LINK_RE.search(href)
    if match is None:
        return None
    return match.group("course_id")


def parse_course_link_text(text: str, course_id: str, home_url: str) -> D2LCourseSnapshot:
    parts = [part.strip() for part in text.split(",")]
    primary = parts[0]
    code, _, name = primary.partition(" - ")
    return D2LCourseSnapshot(
        course_id=course_id,
        code=code.strip(),
        name=name.strip() or primary.strip(),
        offering_code=parts[1] if len(parts) > 1 else None,
        semester=parts[2] if len(parts) > 2 else None,
        end_date_text=", ".join(parts[3:]) if len(parts) > 3 else None,
        home_url=home_url,
    )


def build_due_text(text_values: list[str]) -> str | None:
    month = next((value for value in text_values if value.isalpha() and len(value) <= 4), None)
    day = next((value for value in text_values if value.isdigit() and len(value) <= 2), None)
    time_value = next((value for value in text_values if TIME_RE.search(value)), None)

    parts = [part for part in (month, day, time_value) if part]
    return " ".join(parts) if parts else None


def normalize_event_title(title: str) -> str:
    cleaned = title.replace("View Event - ", "").strip()
    return " ".join(cleaned.split())


def parse_content_type(title: str | None) -> str | None:
    if not title:
        return None
    match = CONTENT_TYPE_RE.match(title)
    if match is None:
        return None
    return match.group("content_type")


def detect_external_platform(*values: str | None) -> str | None:
    haystack = " ".join(value for value in values if value).lower()
    if any(token in haystack for token in ("cengage", "mindtap")):
        return "cengage_mindtap"
    if any(token in haystack for token in ("pearson", "mastering", "mylab")):
        return "pearson_mylab"
    return None


def should_ignore_frame_url(url: str) -> bool:
    if not url or url == "about:blank":
        return True
    hostname = urlparse(url).hostname or ""
    return hostname in IRRELEVANT_FRAME_HOSTS


def is_external_launch_url(url: str) -> bool:
    if not url or url == "about:blank":
        return False

    hostname = urlparse(url).hostname or ""
    if not hostname or hostname in IRRELEVANT_FRAME_HOSTS:
        return False
    if hostname.endswith("oakton.edu"):
        return False
    return detect_external_platform(url) is not None


def trim_document_preamble(text: str) -> str:
    indexes = [text.find(marker) for marker in DOCUMENT_TEXT_MARKERS if text.find(marker) != -1]
    if not indexes:
        return text

    first_marker = min(indexes)
    if first_marker <= 500:
        return text[first_marker:].strip()
    return text
