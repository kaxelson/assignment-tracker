import asyncio
import time
from dataclasses import dataclass
from datetime import UTC, datetime
import re
from urllib.parse import quote, urljoin, urlparse

import structlog
from playwright.async_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from acc.progress import ProgressCallback
from acc.scrapers.base import BaseScraper, ScraperError
from acc.scrapers.fetch_limiter import goto_throttled
from acc.scrapers.snapshots import (
    D2LAnnouncement,
    D2LAnnouncementItem,
    D2LContentTopic,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
    D2LGradeRow,
    D2LGradeSummary,
    D2LToolLink,
    D2LUpcomingEvent,
)
from acc.scrapers.utils import (
    click_first,
    fill_first,
    find_first_visible,
    wait_after_navigation,
    wait_for_first_locator,
)

logger = structlog.get_logger(__name__)

AUTHENTICATED_SELECTORS = (
    "text=/my courses/i",
    "text=/course home/i",
    "nav[role='navigation'] a[href*='/d2l/home']",
    "[data-testid='course-selector']",
    "a[href*='/d2l/lms/dropbox']",
)

USERNAME_SELECTORS = (
    "input#usernameUserInput",
    "input[name='usernameUserInput']",
    "input[placeholder='Username']",
    "input[name='username']",
    "input[name='UserName']",
    "input[name='pf.username']",
    "input[name='user']",
    "input[name='identifier']",
    "input[name='loginfmt']",
    "input[type='email']",
    "input#i0116",
    "input#okta-signin-username",
    "input#username",
    "input#userNameInput",
)

PASSWORD_SELECTORS = (
    "input[name='password']",
    "input[name='pf.pass']",
    "input[name='passwd']",
    "input[type='password']",
    "input#i0118",
    "input#okta-signin-password",
    "input#password",
    "input#passwordInput",
)

SUBMIT_SELECTORS = (
    "button[type='submit']",
    "input[type='submit']",
    "button#idSIButton9",
    "input#idSIButton9",
    "button:has-text('Yes')",
    "button:has-text('No')",
    "input[value='Yes']",
    "input[value='No']",
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
ANNOUNCEMENT_POSTED_RE = re.compile(
    r"posted on (?P<posted>[A-Z][a-z]{2} \d{1,2}, \d{4} \d{1,2}:\d{2} [AP]M)",
    re.IGNORECASE,
)
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
# Module titles under Content that usually hold syllabus / course-wide policy (not weekly task lists).
SYLLABUS_KEYWORDS = (
    "syll",
    "course outline",
    "course information",
    "course info",
    "getting started",
    "start here",
    "welcome",
    "orientation",
    "general information",
    "instructor information",
    "about this course",
    "course summary",
    "grading policy",
    "grade policy",
    "grade breakdown",
    "grade components",
    "assessment plan",
    "course expectations",
    "class expectations",
    "important information",
)
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
MAX_CONTENT_OUTLINE_TEXT_CHARS = 24_000
MAX_CONTENT_TOPIC_EXTRACTS_PER_COURSE = 40

WEEK_MODULE_TITLE_RE = re.compile(r"^week\s+\d+\b", re.IGNORECASE)
CONTENT_SCHEDULE_MODULE_RE = re.compile(
    r"graded?\s*tasks?|grades?\s*tasks?|grade\s+tasks?|extra\s+credit|"
    r"programming\s+exercise|lab\s+assignment|weekly\s+tasks?|reading\s+tasks?",
    re.IGNORECASE,
)
_ASSIGNMENT_CONTENT_TOPIC_RE = re.compile(
    r"graded?\s*tasks?|grades?\s*tasks?|extra\s+credit|programming\s+exercise|"
    r"\bquiz\b|\bexam\b|lab\s+\d|assignment|\bhw\b|\bhomework\b|due\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)",
    re.IGNORECASE,
)


def content_module_has_schedule_hints(title: str) -> bool:
    t = title.strip()
    if not t:
        return False
    if WEEK_MODULE_TITLE_RE.match(t):
        return True
    return CONTENT_SCHEDULE_MODULE_RE.search(t) is not None


def content_topic_suggests_assignment_list(title: str | None, content_type: str | None) -> bool:
    ct = (content_type or "").lower()
    if ct in {"html document", "html"}:
        return True
    if title and _ASSIGNMENT_CONTENT_TOPIC_RE.search(title):
        return True
    return False


@dataclass(slots=True)
class AuthResult:
    authenticated: bool
    used_saved_session: bool = False


class D2LScraper(BaseScraper):
    async def login(self, force: bool = False, on_progress: ProgressCallback | None = None) -> AuthResult:
        self.settings.ensure_runtime_dirs()

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright, force=force)
            page: Page | None = None
            try:
                if on_progress is not None:
                    on_progress("D2L session", "Launching browser and opening D2L...")
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                await goto_throttled(page, self.settings.d2l_base_url, self.settings)

                if await self._is_authenticated(page):
                    logger.info("d2l.saved_session_valid")
                    if on_progress is not None:
                        on_progress(
                            "D2L session",
                            "Saved session is still valid.",
                            fraction=1.0,
                        )
                    await self._persist_state(browser_context)
                    return AuthResult(authenticated=True, used_saved_session=True)

                if on_progress is not None:
                    on_progress("D2L session", "Opening login or SSO page...")
                page = await self._open_login_surface(page, browser_context)
                await self._try_autofill_login(page)
                if on_progress is not None:
                    on_progress(
                        "D2L session",
                        "Finish sign-in in the browser (MFA if prompted)...",
                    )
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
                await goto_throttled(page, self.settings.d2l_base_url, self.settings)
                authenticated = await self._is_authenticated(page)
                logger.info("d2l.saved_session_checked", authenticated=authenticated)
                return authenticated
            finally:
                await browser_context.close()
                await browser.close()

    async def save_snapshot(
        self,
        limit_courses: int | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> D2LDashboardSnapshot:
        snapshot = await self.snapshot(limit_courses=limit_courses, on_progress=on_progress)
        output_path = self.settings.d2l_snapshot_path
        output_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
        logger.info("d2l.snapshot_saved", path=str(output_path), courses=len(snapshot.courses))
        return snapshot

    async def snapshot(
        self,
        limit_courses: int | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> D2LDashboardSnapshot:
        if not self.settings.d2l_storage_state_path.exists():
            raise ScraperError("No saved D2L session found. Run `acc d2l-login` first.")

        async with async_playwright() as playwright:
            browser, browser_context = await self._new_context(playwright, force=False)
            try:
                page = await browser_context.new_page()
                page.set_default_timeout(self.settings.browser_timeout_ms)
                if on_progress is not None:
                    on_progress("D2L snapshot", "Opening My Courses...", fraction=0.0)
                await goto_throttled(page, self.settings.d2l_base_url, self.settings)
                await self._wait_for_course_cards(page)

                if not await self._is_authenticated(page):
                    raise ScraperError("Saved D2L session is no longer authenticated. Run `acc d2l-login`.")

                source_url = page.url
                courses = await self._scrape_courses(page)
                if limit_courses is not None:
                    courses = courses[:limit_courses]

                await page.close()

                if courses:
                    if on_progress is not None:
                        on_progress(
                            "D2L snapshot",
                            f"Found {len(courses)} course(s); loading grades, announcements, and content...",
                            fraction=0.05,
                        )
                    concurrency = max(
                        1,
                        min(len(courses), self.settings.browser_course_concurrency),
                    )
                    sem = asyncio.Semaphore(concurrency)
                    done_lock = asyncio.Lock()
                    done_count = 0
                    total_courses = len(courses)

                    async def scrape_course_home(course: D2LCourseSnapshot) -> None:
                        nonlocal done_count
                        async with sem:
                            worker = await browser_context.new_page()
                            worker.set_default_timeout(self.settings.browser_timeout_ms)
                            try:
                                logger.info(
                                    "d2l.course_home_started",
                                    course_id=course.course_id,
                                    code=course.code,
                                )
                                await self._scrape_course_home(worker, course)
                                logger.info(
                                    "d2l.course_home_completed",
                                    course_id=course.course_id,
                                    code=course.code,
                                )
                            finally:
                                await worker.close()
                            if on_progress is not None:
                                async with done_lock:
                                    done_count += 1
                                    on_progress(
                                        "D2L snapshot",
                                        f"{done_count}/{total_courses} course pages — {course.code}",
                                        fraction=done_count / total_courses,
                                    )

                    await asyncio.gather(*[scrape_course_home(c) for c in courses])
                elif on_progress is not None:
                    on_progress("D2L snapshot", "No courses found on the dashboard.")

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
        await goto_throttled(page, course.home_url, self.settings)
        await wait_after_navigation(page)
        course.tool_links = await self._scrape_tool_links(page)
        course.upcoming_events = await self._scrape_upcoming_events(page)
        course.announcements = await self._scrape_announcements(page, course.course_id)
        content_url = next((tool.url for tool in course.tool_links if tool.name == "Content"), None)
        if content_url is not None:
            syllabus_topics, external_tools, content_outline_topics = await self._scrape_content_resources(
                page, content_url
            )
            course.syllabus_topics = syllabus_topics
            course.external_tools = external_tools
            course.content_outline_topics = content_outline_topics
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

    async def _scrape_announcements(self, page: Page, course_id: str) -> list[D2LAnnouncement]:
        announcements_url = self._absolute_url(f"/d2l/lms/news/main.d2l?ou={course_id}")
        await goto_throttled(page, announcements_url, self.settings)
        if not await wait_for_first_locator(
            page,
            "a[href*='/d2l/le/news/']",
            timeout_ms=12_000,
        ):
            await wait_after_navigation(page)

        announcement_links = page.locator("a[href*='/d2l/le/news/']")
        announcements: list[D2LAnnouncement] = []
        seen_urls: set[str] = set()

        for index in range(await announcement_links.count()):
            link = announcement_links.nth(index)
            href = await link.get_attribute("href")
            title = self._clean_text(await link.inner_text())
            if not href or not title:
                continue

            absolute_url = self._absolute_url(href)
            if absolute_url in seen_urls:
                continue

            announcements.append(
                D2LAnnouncement(
                    title=title,
                    url=absolute_url,
                )
            )
            seen_urls.add(absolute_url)

        detailed_announcements: list[D2LAnnouncement] = []
        for announcement in announcements:
            detailed_announcements.append(
                await self._scrape_announcement_detail(
                    page,
                    title=announcement.title,
                    announcement_url=announcement.url,
                )
            )

        logger.info("d2l.announcements_scraped", count=len(detailed_announcements), url=announcements_url)
        return detailed_announcements

    async def _scrape_announcement_detail(
        self,
        page: Page,
        *,
        title: str,
        announcement_url: str,
    ) -> D2LAnnouncement:
        await goto_throttled(page, announcement_url, self.settings)
        await wait_after_navigation(page)

        main = page.locator("div[role='main']").first
        main_text = ""
        if await main.count() > 0:
            try:
                main_text = self._clean_text(await main.inner_text())
            except Exception:
                main_text = ""

        posted_at_text = extract_announcement_posted_at_text(main_text)
        item_links = page.locator("div[role='main'] a[href*='quickLink.d2l']")
        items: list[D2LAnnouncementItem] = []
        seen_urls: set[str] = set()
        seen_titles: set[str] = set()

        for index in range(await item_links.count()):
            link = item_links.nth(index)
            href = await link.get_attribute("href")
            item_title = self._clean_text(await link.inner_text())
            if not item_title:
                continue

            absolute_url = self._absolute_url(href) if href else None
            key = absolute_url or item_title.casefold()
            if key in seen_urls or item_title.casefold() in seen_titles:
                continue

            items.append(D2LAnnouncementItem(title=item_title, url=absolute_url))
            seen_urls.add(key)
            seen_titles.add(item_title.casefold())

        return D2LAnnouncement(
            title=title,
            url=announcement_url,
            posted_at_text=posted_at_text,
            items=items,
        )

    async def _scrape_grades(
        self,
        page: Page,
        grades_url: str,
    ) -> tuple[D2LGradeSummary | None, list[D2LGradeRow]]:
        await goto_throttled(page, grades_url, self.settings)
        if not await wait_for_first_locator(
            page,
            "table.d2l-table.d2l-grid",
            timeout_ms=12_000,
        ):
            await wait_after_navigation(page)

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
    ) -> tuple[list[D2LContentTopic], list[D2LContentTopic], list[D2LContentTopic]]:
        await goto_throttled(page, content_url, self.settings)
        if not await wait_for_first_locator(
            page,
            CONTENT_MODULE_SELECTOR,
            state="attached",
            timeout_ms=12_000,
        ):
            await wait_after_navigation(page)

        module_titles = await self._list_content_module_titles(page)
        syllabus_topics: dict[str, D2LContentTopic] = {}
        external_tools: dict[str, D2LContentTopic] = {}
        content_outline_topics: dict[str, D2LContentTopic] = {}

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

        await goto_throttled(page, content_url, self.settings)
        await wait_after_navigation(page)
        await wait_for_first_locator(
            page,
            CONTENT_MODULE_SELECTOR,
            state="attached",
            timeout_ms=8_000,
        )

        schedule_modules = [title for title in module_titles if content_module_has_schedule_hints(title)]
        extractions_left = MAX_CONTENT_TOPIC_EXTRACTS_PER_COURSE

        for module_title in schedule_modules:
            await goto_throttled(page, content_url, self.settings)
            await wait_after_navigation(page)
            try:
                await self._select_content_module(page, module_title)
            except ScraperError:
                logger.warning("d2l.content_module_skipped", module_title=module_title)
                continue

            main = page.locator("div[role='main']").first
            main_text = ""
            if await main.count() > 0:
                try:
                    main_text = self._clean_text(await main.inner_text())
                except Exception:
                    main_text = ""

            if len(main_text) >= 120:
                synthetic_url = f"{content_url}#outline-module={quote(module_title, safe='')}"
                content_outline_topics[synthetic_url] = D2LContentTopic(
                    title=f"{module_title} (module view)",
                    url=synthetic_url,
                    module_title=module_title,
                    content_type="ModuleOverview",
                    extracted_text=main_text[:MAX_CONTENT_OUTLINE_TEXT_CHARS],
                )

            topics = await self._scrape_visible_content_topics(page, module_title)
            for topic in topics:
                if extractions_left <= 0:
                    break
                if not content_topic_suggests_assignment_list(topic.title, topic.content_type):
                    continue
                if topic.url in syllabus_topics or topic.url in external_tools:
                    continue
                if topic.url in content_outline_topics:
                    continue
                extracted = await self._extract_document_text(page, topic.url)
                extractions_left -= 1
                if extracted:
                    content_outline_topics[topic.url] = D2LContentTopic(
                        title=topic.title,
                        url=topic.url,
                        module_title=module_title,
                        content_type=topic.content_type,
                        extracted_text=extracted[:MAX_CONTENT_OUTLINE_TEXT_CHARS],
                    )
                await goto_throttled(page, content_url, self.settings)
                await wait_after_navigation(page)
                try:
                    await self._select_content_module(page, module_title)
                except ScraperError:
                    break

        logger.info(
            "d2l.content_resources_scraped",
            url=content_url,
            syllabus_topics=len(syllabus_topics),
            external_tools=len(external_tools),
            content_outline_topics=len(content_outline_topics),
        )
        return (
            list(syllabus_topics.values()),
            list(external_tools.values()),
            list(content_outline_topics.values()),
        )

    async def _list_content_module_titles(self, page: Page) -> list[str]:
        titles: list[str] = []
        seen: set[str] = set()

        for module in await page.locator(CONTENT_MODULE_SELECTOR).all():
            try:
                title = self._clean_text(await module.inner_text(timeout=8_000))
            except PlaywrightTimeoutError:
                continue
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
        candidates = page.locator(CONTENT_MODULE_SELECTOR, has_text=module_title)
        if await candidates.count() == 0:
            raise ScraperError(f"Could not find D2L content module: {module_title}")
        module = candidates.first
        try:
            await module.wait_for(state="attached", timeout=15_000)
        except PlaywrightTimeoutError as error:
            raise ScraperError(f"Could not find D2L content module: {module_title}") from error

        # Nested week/chapter rows often stay in the DOM but are not "visible" until a
        # parent accordion is expanded. A normal click raises PlaywrightError ("not visible"),
        # not only TimeoutError, so we skip strict clicks and use force/JS.
        try:
            await module.scroll_into_view_if_needed(timeout=8_000)
        except PlaywrightError:
            pass
        try:
            await module.click(force=True, timeout=15_000)
        except PlaywrightError:
            await module.evaluate("el => el.click()")

        if not await wait_for_first_locator(page, CONTENT_TOPIC_SELECTOR, timeout_ms=8_000):
            await wait_after_navigation(page, timeout_ms=6_000)

    async def _scrape_visible_content_topics(
        self,
        page: Page,
        module_title: str,
    ) -> list[D2LContentTopic]:
        visible_topics: list[D2LContentTopic] = []
        seen: set[str] = set()
        topic_timeout_ms = 8_000

        # Resolve each match once. Using count()+nth(i) can hang when the DOM updates
        # and an index no longer exists (e.g. nth(11) after topics collapse).
        for topic in await page.locator(CONTENT_TOPIC_SELECTOR).all():
            try:
                href = await topic.get_attribute("href", timeout=topic_timeout_ms)
                text = self._clean_text(await topic.inner_text(timeout=topic_timeout_ms))
                title_attr = await topic.get_attribute("title", timeout=topic_timeout_ms)
            except PlaywrightTimeoutError:
                continue
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
                    content_type=parse_content_type(title_attr),
                )
            )
            seen.add(absolute_url)

        return visible_topics

    async def _extract_document_text(self, page: Page, topic_url: str) -> str | None:
        await goto_throttled(page, topic_url, self.settings)
        await wait_after_navigation(page, timeout_ms=12_000)
        try:
            await page.wait_for_function(
                "() => document.querySelectorAll('iframe').length > 0",
                timeout=6_000,
            )
        except PlaywrightTimeoutError:
            pass

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
        await goto_throttled(page, topic_url, self.settings)
        await wait_after_navigation(page, timeout_ms=12_000)
        try:
            await page.wait_for_function(
                "() => document.querySelectorAll('iframe').length > 0",
                timeout=6_000,
            )
        except PlaywrightTimeoutError:
            pass

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
        deadline = time.monotonic() + min(self.settings.d2l_login_timeout_seconds, 60)

        while time.monotonic() < deadline:
            await self._wait_for_login_page_progress(page)
            username_locator = await find_first_visible(page, USERNAME_SELECTORS, 500)
            password_locator = await find_first_visible(page, PASSWORD_SELECTORS, 500)

            if not filled_username and password_locator is not None and username_locator is None:
                filled_username = True
                logger.info("d2l.username_step_skipped")

            if not filled_username and username_locator is not None:
                try:
                    filled_username = bool((await username_locator.input_value()).strip())
                except Exception:
                    filled_username = False
                if filled_username:
                    logger.info("d2l.username_already_present")

            if username and not filled_username:
                filled_username = await fill_first(page, USERNAME_SELECTORS, username, 1_000)
                if filled_username:
                    logger.info("d2l.username_filled")
                    if password_locator is None:
                        await self._submit_login_step(page)
                    continue

            if password and not filled_password:
                filled_password = await fill_first(page, PASSWORD_SELECTORS, password, 1_000)
                if filled_password:
                    logger.info("d2l.password_filled")
                    await self._submit_login_step(page)
                    continue

            if await self._is_authenticated(page):
                return

            if (
                (filled_username or filled_password)
                and username_locator is None
                and password_locator is None
                and await click_first(page, SUBMIT_SELECTORS, 750)
            ):
                logger.info("d2l.submit_clicked")
                await self._wait_for_login_page_progress(page)
                continue

            await asyncio.sleep(0.25)

        if not filled_username or not filled_password:
            logger.info("d2l.manual_login_required")

    async def _open_login_surface(self, page: Page, browser_context: BrowserContext) -> Page:
        try:
            async with browser_context.expect_page(timeout=2_000) as new_page_info:
                await click_first(page, LOGIN_LINK_SELECTORS, 1_500)
            login_page = await new_page_info.value
            login_page.set_default_timeout(self.settings.browser_timeout_ms)
            await self._wait_for_login_page_progress(login_page)
            return login_page
        except PlaywrightTimeoutError:
            await click_first(page, LOGIN_LINK_SELECTORS, 1_500)
            await self._wait_for_login_page_progress(page)
            return page

    async def _submit_login_step(self, page: Page) -> None:
        await click_first(page, SUBMIT_SELECTORS, 1_500)
        await self._wait_for_login_page_progress(page)

    async def _wait_for_login_page_progress(self, page: Page) -> None:
        for state in ("domcontentloaded", "networkidle"):
            try:
                await page.wait_for_load_state(state, timeout=2_000)
            except PlaywrightTimeoutError:
                continue
        await asyncio.sleep(0.15)

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

            await asyncio.sleep(1.0)

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


def extract_announcement_posted_at_text(text: str) -> str | None:
    match = ANNOUNCEMENT_POSTED_RE.search(text)
    if match is None:
        return None
    return match.group("posted")


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
