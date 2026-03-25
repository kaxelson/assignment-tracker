from __future__ import annotations

import asyncio
import hashlib
import re
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import structlog

from acc.ai.crawl_link_picker import (
    CrawlLinkPicker,
    LinkSelectionResult,
    heuristic_follow_indices_d2l,
    heuristic_follow_indices_external,
    heuristic_follow_indices_pearson_nav,
)
from playwright.async_api import (
    BrowserContext,
    Frame,
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from acc.config import Settings
from acc.progress import ProgressCallback
from acc.scrapers.fetch_limiter import goto_throttled
from acc.engine.normalizer import build_course_id, choose_primary_external_tool, slugify
from acc.scrapers.base import BaseScraper, ScraperError
from acc.scrapers.d2l import D2LScraper
from acc.scrapers.utils import wait_after_navigation, wait_for_first_locator
from acc.scrapers.crawl_navigation import (
    cengage_url_same_course,
    d2l_calendar_url,
    d2l_href_allowed_for_course,
    is_pearson_mylab_course_tool_frame_url,
    nav_target_should_be_crawled,
    normalize_crawl_url,
    pearson_href_in_course_scope,
)
from acc.scrapers.external import CENGAGE_HOST_TOKENS, ExternalScraper, is_pearson_assignments_frame_url
from acc.scrapers.snapshots import (
    CrawlArtifact,
    CrawlCourseSnapshot,
    CrawlSnapshot,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
)

logger = structlog.get_logger(__name__)


def _crawl_root_artifact_with_llm_link_meta(
    root: CrawlArtifact,
    *,
    platform: str,
    page_url: str,
    follow_indices: list[int],
    notes: str | None = None,
    fallback: str | None = None,
    error: str | None = None,
) -> CrawlArtifact:
    meta = dict(root.metadata)
    body: dict[str, object] = {
        "platform": platform,
        "page_url": page_url,
        "follow_indices": list(follow_indices),
    }
    if notes is not None:
        body["notes"] = notes
    if fallback is not None:
        body["fallback"] = fallback
    if error is not None:
        body["error"] = error
    meta["llm_link_selection"] = body
    return root.model_copy(update={"metadata": meta})


@dataclass(slots=True)
class CrawlTarget:
    source_platform: str
    page_kind: str
    url: str
    title: str | None = None


class CrawlScraper(BaseScraper):
    async def save_snapshot(
        self,
        dashboard_snapshot: D2LDashboardSnapshot | None = None,
        *,
        course_id: str | None = None,
        limit_courses: int | None = None,
        max_external_details: int | None = None,
        capture_screenshots: bool = True,
        on_progress: ProgressCallback | None = None,
    ) -> CrawlSnapshot:
        snapshot = await self.snapshot(
            dashboard_snapshot=dashboard_snapshot,
            course_id=course_id,
            limit_courses=limit_courses,
            max_external_details=max_external_details,
            capture_screenshots=capture_screenshots,
            on_progress=on_progress,
        )
        self.settings.crawl_snapshot_path.write_text(
            snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )
        logger.info(
            "crawl.snapshot_saved",
            path=str(self.settings.crawl_snapshot_path),
            courses=len(snapshot.courses),
            artifacts=len(snapshot.artifacts),
        )
        return snapshot

    async def snapshot(
        self,
        dashboard_snapshot: D2LDashboardSnapshot | None = None,
        *,
        course_id: str | None = None,
        limit_courses: int | None = None,
        max_external_details: int | None = None,
        capture_screenshots: bool = True,
        on_progress: ProgressCallback | None = None,
    ) -> CrawlSnapshot:
        if dashboard_snapshot is None:
            if not self.settings.d2l_snapshot_path.exists():
                raise ScraperError(
                    "No D2L snapshot found. Run `acc d2l-snapshot` before crawling artifacts."
                )
            dashboard_snapshot = D2LDashboardSnapshot.model_validate_json(
                self.settings.d2l_snapshot_path.read_text(encoding="utf-8")
            )

        if course_id is not None:
            filtered_courses = [
                course for course in dashboard_snapshot.courses if build_course_id(course) == course_id
            ]
            dashboard_snapshot = dashboard_snapshot.model_copy(update={"courses": filtered_courses})
        if limit_courses is not None:
            courses = dashboard_snapshot.courses[:limit_courses]
            dashboard_snapshot = dashboard_snapshot.model_copy(update={"courses": courses})

        timestamp = datetime.now(UTC)
        run_dir = self.settings.crawl_artifacts_dir / timestamp.strftime("%Y%m%dT%H%M%SZ")
        run_dir.mkdir(parents=True, exist_ok=True)
        if on_progress is not None:
            on_progress("Crawl", "Launching browser...", fraction=0.0)

        d2l_scraper = D2LScraper(self.settings)
        external_scraper = ExternalScraper(self.settings)
        course_manifests: list[CrawlCourseSnapshot] = []
        artifacts: list[CrawlArtifact] = []

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
                if not await d2l_scraper._is_authenticated(page):
                    raise ScraperError("Saved D2L session is no longer authenticated. Run `acc d2l-login`.")
            finally:
                await auth_context.close()

            courses = list(dashboard_snapshot.courses)
            if on_progress is not None:
                if courses:
                    on_progress(
                        "Crawl",
                        f"Capturing {len(courses)} course(s) in parallel...",
                        fraction=0.05,
                    )
                else:
                    on_progress("Crawl", "No courses in this snapshot.")
            if courses:
                concurrency = max(1, min(len(courses), self.settings.browser_course_concurrency))
                sem = asyncio.Semaphore(concurrency)
                done_lock = asyncio.Lock()
                done_count = 0
                total_courses = len(courses)

                async def crawl_one_course(
                    index: int,
                    course: D2LCourseSnapshot,
                ) -> tuple[int, CrawlCourseSnapshot, list[CrawlArtifact]]:
                    nonlocal done_count
                    async with sem:
                        course_id = build_course_id(course)
                        logger.info("crawl.course_started", course_id=course_id, code=course.code)
                        ctx = await browser.new_context(storage_state=storage_state_path)
                        try:
                            worker_page = await ctx.new_page()
                            worker_page.set_default_timeout(self.settings.browser_timeout_ms)
                            course_dir = run_dir / course_id
                            course_dir.mkdir(parents=True, exist_ok=True)
                            course_artifacts = await self._crawl_course(
                                course,
                                page=worker_page,
                                context=ctx,
                                d2l_scraper=d2l_scraper,
                                external_scraper=external_scraper,
                                course_dir=course_dir,
                                fetched_at=timestamp,
                                max_external_details=max_external_details,
                                capture_screenshots=capture_screenshots,
                                on_progress=on_progress,
                            )
                            manifest = CrawlCourseSnapshot(
                                course_id=course_id,
                                code=course.code,
                                name=course.name,
                                artifact_count=len(course_artifacts),
                            )
                            logger.info(
                                "crawl.course_completed",
                                course_id=course_id,
                                artifacts=len(course_artifacts),
                            )
                            if on_progress is not None:
                                async with done_lock:
                                    done_count += 1
                                    on_progress(
                                        "Crawl",
                                        f"{done_count}/{total_courses} done — {course.code} "
                                        f"({len(course_artifacts)} artifacts)",
                                        fraction=done_count / total_courses,
                                    )
                            return index, manifest, course_artifacts
                        finally:
                            await ctx.close()

                indexed_results = await asyncio.gather(
                    *[crawl_one_course(i, c) for i, c in enumerate(courses)],
                )
                for _, manifest, course_artifacts in sorted(
                    indexed_results,
                    key=lambda item: item[0],
                ):
                    course_manifests.append(manifest)
                    artifacts.extend(course_artifacts)

            await browser.close()

        return CrawlSnapshot(
            fetched_at=timestamp,
            artifacts_dir=str(run_dir),
            courses=course_manifests,
            artifacts=artifacts,
        )

    async def _crawl_course(
        self,
        course: D2LCourseSnapshot,
        *,
        page: Page,
        context: BrowserContext,
        d2l_scraper: D2LScraper,
        external_scraper: ExternalScraper,
        course_dir: Path,
        fetched_at: datetime,
        max_external_details: int | None,
        capture_screenshots: bool,
        on_progress: ProgressCallback | None = None,
    ) -> list[CrawlArtifact]:
        artifacts: list[CrawlArtifact] = []
        course_id = build_course_id(course)

        if self.settings.crawl_ai_navigation and self.settings.openai_api_key is None:
            logger.warning(
                "crawl.ai_navigation_missing_openai_key",
                hint="Set ACC_OPENAI_API_KEY or disable ACC_CRAWL_AI_NAVIGATION.",
            )

        d2l_targets: list[CrawlTarget] = []
        parallel_d2l_seed_crawl = False

        if self._use_ai_link_navigation():
            artifacts.extend(
                await self._crawl_d2l_course_ai_bfs(
                    course=course,
                    page=page,
                    context=context,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                    on_progress=on_progress,
                )
            )
        else:
            seen_urls: set[str] = set()
            for target in build_d2l_crawl_targets(course):
                if target.url in seen_urls:
                    continue
                seen_urls.add(target.url)
                d2l_targets.append(target)
            page_concurrency = max(1, self.settings.crawl_page_concurrency)
            total_targets = len(d2l_targets)
            if page_concurrency == 1:
                for index, target in enumerate(d2l_targets, start=1):
                    if on_progress is not None and total_targets:
                        label = (target.title or target.page_kind or "page").strip()
                        on_progress(
                            "Crawl",
                            f"{course.code}: D2L page {index}/{total_targets} — {label}",
                        )
                    extra_artifacts: list[CrawlArtifact] = []
                    artifact = await self._capture_page_artifact(
                        page=page,
                        course_id=course_id,
                        course_code=course.code,
                        source_platform=target.source_platform,
                        page_kind=target.page_kind,
                        url=target.url,
                        title=target.title,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        capture_screenshots=capture_screenshots,
                        extra_artifacts=extra_artifacts,
                    )
                    artifacts.append(artifact)
                    artifacts.extend(extra_artifacts)
            else:
                parallel_d2l_seed_crawl = total_targets > 0
                artifacts.extend(
                    await self._capture_d2l_seed_targets_parallel(
                        d2l_targets,
                        context=context,
                        course_id=course_id,
                        course_code=course.code,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        capture_screenshots=capture_screenshots,
                        on_progress=on_progress,
                        concurrency=page_concurrency,
                    )
                )

        primary_external_tool = choose_primary_external_tool(course.external_tools)
        if primary_external_tool is None:
            return artifacts

        external_platform = infer_external_platform(course)
        if parallel_d2l_seed_crawl and external_platform in (
            "cengage_mindtap",
            "pearson_mylab",
        ):
            await goto_throttled(page, course.home_url, self.settings)
            await wait_after_navigation(page)

        if external_platform == "cengage_mindtap":
            if on_progress is not None:
                on_progress("Crawl", f"{course.code}: opening Cengage MindTap...")
            artifacts.extend(
                await self._crawl_cengage_course(
                    course=course,
                    page=page,
                    context=context,
                    external_scraper=external_scraper,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    max_external_details=max_external_details,
                    capture_screenshots=capture_screenshots,
                )
            )
        elif external_platform == "pearson_mylab":
            if on_progress is not None:
                on_progress("Crawl", f"{course.code}: opening Pearson MyLab...")
            artifacts.extend(
                await self._crawl_pearson_course(
                    course=course,
                    page=page,
                    external_scraper=external_scraper,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                )
            )

        return artifacts

    def _use_ai_link_navigation(self) -> bool:
        return bool(
            self.settings.crawl_ai_navigation and self.settings.openai_api_key is not None
        )

    async def _expand_d2l_ai_bfs_node(
        self,
        target: CrawlTarget,
        *,
        worker: Page,
        course: D2LCourseSnapshot,
        internal_id: str,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        d2l_host: str,
        picker: CrawlLinkPicker,
    ) -> tuple[list[CrawlArtifact], list[CrawlTarget]]:
        extra_artifacts: list[CrawlArtifact] = []
        try:
            artifact = await self._capture_page_artifact(
                page=worker,
                course_id=internal_id,
                course_code=course.code,
                source_platform=target.source_platform,
                page_kind=target.page_kind,
                url=target.url,
                title=target.title,
                course_dir=course_dir,
                fetched_at=fetched_at,
                capture_screenshots=capture_screenshots,
                extra_artifacts=extra_artifacts,
            )
        except Exception as exc:
            logger.info(
                "crawl.d2l_ai_page_failed",
                course_id=internal_id,
                url=target.url,
                error=str(exc),
            )
            return [], []

        branch_artifacts = [artifact, *extra_artifacts]
        page_text = await self._extract_page_text(worker)
        raw_candidates = await collect_d2l_page_link_candidates(
            worker,
            page_url=worker.url,
            course_id=course.course_id,
            d2l_host=d2l_host,
            max_links=self.settings.crawl_ai_max_links_per_page,
        )
        link_tuples: list[tuple[str, str]] = [(href, text) for href, text in raw_candidates]
        try:
            selection = await picker.pick_link_selection(
                platform="d2l",
                page_url=worker.url,
                page_text=page_text,
                course_code=course.code,
                course_name=course.name,
                links=link_tuples,
            )
            follow = selection.follow
            meta = dict(artifact.metadata)
            meta["llm_link_selection"] = {
                "notes": selection.notes,
                "follow_indices": list(follow),
                "page_url": worker.url,
            }
            artifact.metadata = meta
        except Exception as exc:
            logger.warning("crawl.d2l_ai_link_pick_failed", url=worker.url, error=str(exc))
            follow = heuristic_follow_indices_d2l(link_tuples)
            meta = dict(artifact.metadata)
            meta["llm_link_selection"] = {
                "fallback": "heuristic_d2l",
                "error": str(exc),
                "follow_indices": list(follow),
                "page_url": worker.url,
            }
            artifact.metadata = meta

        children: list[CrawlTarget] = []
        for index in follow:
            if index < 0 or index >= len(link_tuples):
                continue
            href, text = link_tuples[index]
            if not d2l_href_allowed_for_course(href, course.course_id, d2l_host):
                continue
            children.append(
                CrawlTarget(
                    source_platform="d2l",
                    page_kind="d2l_nav_page",
                    url=href,
                    title=text or None,
                )
            )
        return branch_artifacts, children

    async def _crawl_d2l_course_ai_bfs(
        self,
        course: D2LCourseSnapshot,
        *,
        page: Page,
        context: BrowserContext,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        on_progress: ProgressCallback | None = None,
    ) -> list[CrawlArtifact]:
        page_concurrency = max(1, self.settings.crawl_page_concurrency)
        if page_concurrency == 1:
            return await self._crawl_d2l_course_ai_bfs_sequential(
                course=course,
                page=page,
                course_dir=course_dir,
                fetched_at=fetched_at,
                capture_screenshots=capture_screenshots,
                on_progress=on_progress,
            )
        return await self._crawl_d2l_course_ai_bfs_parallel(
            course=course,
            context=context,
            course_dir=course_dir,
            fetched_at=fetched_at,
            capture_screenshots=capture_screenshots,
            on_progress=on_progress,
            page_concurrency=page_concurrency,
        )

    async def _crawl_d2l_course_ai_bfs_sequential(
        self,
        course: D2LCourseSnapshot,
        *,
        page: Page,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        on_progress: ProgressCallback | None = None,
    ) -> list[CrawlArtifact]:
        artifacts: list[CrawlArtifact] = []
        queue: deque[CrawlTarget] = deque()
        seen_queued: set[str] = set()
        d2l_host = urlparse(self.settings.d2l_base_url).netloc
        internal_id = build_course_id(course)

        def enqueue(target: CrawlTarget) -> None:
            norm = normalize_crawl_url(target.url)
            if norm in seen_queued:
                return
            seen_queued.add(norm)
            queue.append(target)

        for seed in build_d2l_crawl_targets(course):
            enqueue(seed)

        picker = CrawlLinkPicker(self.settings)
        pages_remaining = max(1, self.settings.crawl_ai_max_d2l_pages)
        visit_index = 0

        while queue and pages_remaining > 0:
            target = queue.popleft()
            pages_remaining -= 1
            visit_index += 1
            if on_progress is not None:
                label = (target.title or target.page_kind or "page").strip()
                on_progress(
                    "Crawl",
                    f"{course.code}: AI-guided D2L visit {visit_index} — {label} "
                    f"({pages_remaining} page budget left)",
                )
            branch_artifacts, children = await self._expand_d2l_ai_bfs_node(
                target,
                worker=page,
                course=course,
                internal_id=internal_id,
                course_dir=course_dir,
                fetched_at=fetched_at,
                capture_screenshots=capture_screenshots,
                d2l_host=d2l_host,
                picker=picker,
            )
            artifacts.extend(branch_artifacts)
            for child in children:
                enqueue(child)

        return artifacts

    async def _crawl_d2l_course_ai_bfs_parallel(
        self,
        course: D2LCourseSnapshot,
        *,
        context: BrowserContext,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        on_progress: ProgressCallback | None,
        page_concurrency: int,
    ) -> list[CrawlArtifact]:
        artifacts: list[CrawlArtifact] = []
        queue: deque[CrawlTarget] = deque()
        seen_queued: set[str] = set()
        d2l_host = urlparse(self.settings.d2l_base_url).netloc
        internal_id = build_course_id(course)
        queue_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()

        async def enqueue_many(targets: Iterable[CrawlTarget]) -> None:
            async with queue_lock:
                for target in targets:
                    norm = normalize_crawl_url(target.url)
                    if norm in seen_queued:
                        continue
                    seen_queued.add(norm)
                    queue.append(target)

        await enqueue_many(build_d2l_crawl_targets(course))

        picker = CrawlLinkPicker(self.settings)
        pages_remaining = max(1, self.settings.crawl_ai_max_d2l_pages)
        visit_index = 0

        async def visit_one(ordinal: int, target: CrawlTarget, budget_after_batch: int) -> tuple[int, list[CrawlArtifact]]:
            worker = await context.new_page()
            worker.set_default_timeout(self.settings.browser_timeout_ms)
            try:
                if on_progress is not None:
                    label = (target.title or target.page_kind or "page").strip()
                    async with progress_lock:
                        on_progress(
                            "Crawl",
                            f"{course.code}: AI-guided D2L visit {ordinal} — {label} "
                            f"({budget_after_batch} page budget left)",
                        )
                branch_artifacts, children = await self._expand_d2l_ai_bfs_node(
                    target,
                    worker=worker,
                    course=course,
                    internal_id=internal_id,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                    d2l_host=d2l_host,
                    picker=picker,
                )
                await enqueue_many(children)
                return (ordinal, branch_artifacts)
            finally:
                await worker.close()

        while True:
            async with queue_lock:
                if not queue or pages_remaining <= 0:
                    break
                batch: list[tuple[int, CrawlTarget, int]] = []
                while queue and pages_remaining > 0 and len(batch) < page_concurrency:
                    tgt = queue.popleft()
                    pages_remaining -= 1
                    visit_index += 1
                    budget_snapshot = pages_remaining
                    batch.append((visit_index, tgt, budget_snapshot))

            results = await asyncio.gather(
                *[visit_one(ord_, tgt, budget_snapshot) for ord_, tgt, budget_snapshot in batch],
                return_exceptions=True,
            )
            ordered: list[tuple[int, list[CrawlArtifact]]] = []
            for item in results:
                if isinstance(item, Exception):
                    logger.warning("crawl.d2l_ai_parallel_branch_failed", error=str(item))
                    continue
                ordered.append(item)
            ordered.sort(key=lambda entry: entry[0])
            for _, branch_arts in ordered:
                artifacts.extend(branch_arts)

        return artifacts

    async def _crawl_cengage_course(
        self,
        *,
        course: D2LCourseSnapshot,
        page: Page,
        context: BrowserContext,
        external_scraper: ExternalScraper,
        course_dir: Path,
        fetched_at: datetime,
        max_external_details: int | None,
        capture_screenshots: bool,
    ) -> list[CrawlArtifact]:
        course_id = build_course_id(course)
        primary_external_tool = choose_primary_external_tool(course.external_tools)
        if primary_external_tool is None:
            return []

        await self._close_stale_external_pages(context, host_tokens=CENGAGE_HOST_TOKENS)
        course_page = await external_scraper._open_external_page_from_navigation(
            page=page,
            wrapper_url=primary_external_tool.url,
            host_tokens=CENGAGE_HOST_TOKENS,
            direct_fallback_url=primary_external_tool.launch_url or primary_external_tool.url,
        )
        await self._maybe_accept_cengage_cookies(course_page)
        rows = await external_scraper._wait_for_cengage_activity_rows(course_page)
        await wait_after_navigation(course_page)

        nav_candidates = await collect_cengage_nav_link_candidates(course_page)

        page_concurrency = max(1, self.settings.crawl_page_concurrency)

        artifacts = [
            await self._capture_current_page_artifact(
                page=course_page,
                course_id=course_id,
                course_code=course.code,
                source_platform="cengage_mindtap",
                page_kind="external_course_page",
                title=course.name,
                course_dir=course_dir,
                fetched_at=fetched_at,
                capture_screenshots=capture_screenshots,
            )
        ]

        row_fragments: list[CrawlArtifact] = []
        detail_specs: list[tuple[int, str, str, str]] = []
        detail_count = 0
        for index, row in enumerate(await rows.all()):
            row_artifact = await self._capture_locator_fragment(
                locator=row,
                course_id=course_id,
                course_code=course.code,
                source_platform="cengage_mindtap",
                page_kind="external_assignment_row",
                title=await self._cengage_row_title(row),
                parent_url=course_page.url,
                course_dir=course_dir,
                fetched_at=fetched_at,
                metadata={"row_index": index},
            )
            row_fragments.append(row_artifact)

            if max_external_details is not None and detail_count >= max_external_details:
                continue

            activity_id = await extract_cengage_activity_id(row)
            if activity_id is None:
                continue
            detail_url = build_cengage_detail_url(course_page.url, activity_id)
            if detail_url is None:
                continue

            detail_specs.append(
                (len(row_fragments) - 1, detail_url, row_artifact.title, activity_id),
            )
            detail_count += 1

        detail_by_row: dict[int, CrawlArtifact] = {}
        if detail_specs:
            if page_concurrency == 1:
                detail_page = await context.new_page()
                detail_page.set_default_timeout(self.settings.browser_timeout_ms)
                try:
                    for pair_idx, detail_url, title, activity_id in detail_specs:
                        detail_by_row[pair_idx] = await self._capture_page_artifact(
                            page=detail_page,
                            course_id=course_id,
                            course_code=course.code,
                            source_platform="cengage_mindtap",
                            page_kind="external_assignment_page",
                            url=detail_url,
                            title=title,
                            course_dir=course_dir,
                            fetched_at=fetched_at,
                            capture_screenshots=capture_screenshots,
                            metadata={"activity_id": activity_id, "parent_url": course_page.url},
                        )
                finally:
                    await detail_page.close()
            else:
                sem = asyncio.Semaphore(page_concurrency)

                async def capture_cengage_detail(
                    spec: tuple[int, str, str, str],
                ) -> tuple[int, CrawlArtifact]:
                    pair_idx, detail_url, title, activity_id = spec
                    async with sem:
                        worker = await context.new_page()
                        worker.set_default_timeout(self.settings.browser_timeout_ms)
                        try:
                            art = await self._capture_page_artifact(
                                page=worker,
                                course_id=course_id,
                                course_code=course.code,
                                source_platform="cengage_mindtap",
                                page_kind="external_assignment_page",
                                url=detail_url,
                                title=title,
                                course_dir=course_dir,
                                fetched_at=fetched_at,
                                capture_screenshots=capture_screenshots,
                                metadata={"activity_id": activity_id, "parent_url": course_page.url},
                            )
                            return (pair_idx, art)
                        finally:
                            await worker.close()

                for item in await asyncio.gather(
                    *[capture_cengage_detail(s) for s in detail_specs],
                    return_exceptions=True,
                ):
                    if isinstance(item, Exception):
                        logger.warning("crawl.cengage_detail_parallel_failed", error=str(item))
                        continue
                    pair_idx, det_art = item
                    detail_by_row[pair_idx] = det_art

        for ri, frag in enumerate(row_fragments):
            artifacts.append(frag)
            det = detail_by_row.get(ri)
            if det is not None:
                artifacts.append(det)

        nav_budget = self.settings.crawl_max_external_nav_pages
        nav_seen_urls: set[str] = {normalize_crawl_url(course_page.url)}
        try:
            scoped: list[tuple[str, str]] = []
            for href, text in nav_candidates:
                absolute = urljoin(course_page.url, href)
                if not cengage_url_same_course(absolute, course_page.url):
                    continue
                scoped.append((absolute, text))

            cap = self.settings.crawl_ai_max_links_per_page
            if len(scoped) > cap:
                scoped = scoped[:cap]

            selection: LinkSelectionResult | None = None
            link_pick_error: str | None = None
            if self._use_ai_link_navigation():
                page_text = await self._extract_page_text(course_page)
                picker = CrawlLinkPicker(self.settings)
                try:
                    selection = await picker.pick_link_selection(
                        platform="cengage",
                        page_url=course_page.url,
                        page_text=page_text,
                        course_code=course.code,
                        course_name=course.name,
                        links=scoped,
                    )
                    follow = selection.follow
                except Exception as exc:
                    link_pick_error = str(exc)
                    logger.warning(
                        "crawl.cengage_ai_link_pick_failed",
                        url=course_page.url,
                        error=link_pick_error,
                    )
                    follow = heuristic_follow_indices_external(scoped)
            else:
                follow = [
                    index
                    for index, (absolute, text) in enumerate(scoped)
                    if nav_target_should_be_crawled(text, absolute)
                ]

            artifacts[0] = _crawl_root_artifact_with_llm_link_meta(
                artifacts[0],
                platform="cengage_mindtap",
                page_url=course_page.url,
                follow_indices=list(follow),
                notes=selection.notes if selection is not None else None,
                fallback=(
                    "heuristic_cengage"
                    if not self._use_ai_link_navigation()
                    else ("heuristic_after_llm_error" if link_pick_error is not None else None)
                ),
                error=link_pick_error,
            )

            nav_jobs: list[tuple[str, str]] = []
            for index in follow:
                if nav_budget <= 0:
                    break
                if index < 0 or index >= len(scoped):
                    continue
                absolute, text = scoped[index]
                normalized = normalize_crawl_url(absolute)
                if normalized in nav_seen_urls:
                    continue
                nav_seen_urls.add(normalized)
                nav_jobs.append((absolute, text))
                nav_budget -= 1

            if page_concurrency == 1:
                nav_tab = await context.new_page()
                nav_tab.set_default_timeout(self.settings.browser_timeout_ms)
                try:
                    for absolute, text in nav_jobs:
                        try:
                            artifacts.append(
                                await self._capture_page_artifact(
                                    page=nav_tab,
                                    course_id=course_id,
                                    course_code=course.code,
                                    source_platform="cengage_mindtap",
                                    page_kind="external_course_nav_page",
                                    url=absolute,
                                    title=text or None,
                                    course_dir=course_dir,
                                    fetched_at=fetched_at,
                                    capture_screenshots=capture_screenshots,
                                    metadata={"nav_href": absolute, "nav_text": text},
                                )
                            )
                        except Exception as exc:
                            logger.info(
                                "crawl.cengage_nav_failed",
                                course_id=course_id,
                                url=absolute,
                                error=str(exc),
                            )
                finally:
                    await nav_tab.close()
            elif nav_jobs:
                sem = asyncio.Semaphore(page_concurrency)

                async def capture_cengage_nav(job: tuple[str, str]) -> CrawlArtifact:
                    absolute, text = job
                    async with sem:
                        worker = await context.new_page()
                        worker.set_default_timeout(self.settings.browser_timeout_ms)
                        try:
                            return await self._capture_page_artifact(
                                page=worker,
                                course_id=course_id,
                                course_code=course.code,
                                source_platform="cengage_mindtap",
                                page_kind="external_course_nav_page",
                                url=absolute,
                                title=text or None,
                                course_dir=course_dir,
                                fetched_at=fetched_at,
                                capture_screenshots=capture_screenshots,
                                metadata={"nav_href": absolute, "nav_text": text},
                            )
                        finally:
                            await worker.close()

                for item in await asyncio.gather(
                    *[capture_cengage_nav(j) for j in nav_jobs],
                    return_exceptions=True,
                ):
                    if isinstance(item, BaseException):
                        logger.info(
                            "crawl.cengage_nav_failed",
                            course_id=course_id,
                            error=str(item),
                        )
                        continue
                    artifacts.append(item)
        finally:
            await external_scraper._close_popup_if_needed(page, course_page)

        return artifacts

    async def _crawl_pearson_course(
        self,
        *,
        course: D2LCourseSnapshot,
        page: Page,
        external_scraper: ExternalScraper,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
    ) -> list[CrawlArtifact]:
        course_id = build_course_id(course)
        primary_external_tool = choose_primary_external_tool(course.external_tools)
        if primary_external_tool is None:
            return []

        await self._close_stale_external_pages(
            page.context,
            host_tokens=("pearson.com", "pearsoned.com", "pearsoncmg.com", "mylab.pearson.com"),
        )
        logger.info("crawl.pearson_launch_started", course_id=course_id, wrapper_url=primary_external_tool.url)
        pearson_page = await external_scraper._open_pearson_course_page(
            page=page,
            wrapper_url=primary_external_tool.url,
        )
        logger.info("crawl.pearson_course_opened", course_id=course_id, url=pearson_page.url)
        await wait_after_navigation(pearson_page, timeout_ms=12_000)
        for _ in range(2):
            if await external_scraper._maybe_accept_pearson_cookies(pearson_page):
                logger.info("crawl.pearson_cookies_accepted", url=pearson_page.url)
                try:
                    await pearson_page.wait_for_load_state("domcontentloaded", timeout=5_000)
                except PlaywrightTimeoutError:
                    await wait_after_navigation(pearson_page, timeout_ms=5_000)

        artifacts: list[CrawlArtifact] = []
        frame_seen_surfaces: set[tuple[str, str]] = set()
        nav_budget = self.settings.crawl_max_external_nav_pages

        nav_locator = pearson_page.locator("a.left-nav-item, a.left-nav-item-select")
        nav_count = await nav_locator.count()
        if nav_count == 0:
            logger.info("crawl.pearson_no_left_nav", course_id=course_id, url=pearson_page.url)
            await self._open_pearson_assignments_view(pearson_page, external_scraper=external_scraper)
            artifacts.append(
                await self._capture_current_page_artifact(
                    page=pearson_page,
                    course_id=course_id,
                    course_code=course.code,
                    source_platform="pearson_mylab",
                    page_kind="external_course_page",
                    title=course.name,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                    metadata={"pearson_fallback": "assignments_shell"},
                )
            )
            artifacts.extend(
                await self._capture_pearson_embedded_frames_and_rows(
                    pearson_page=pearson_page,
                    external_scraper=external_scraper,
                    course=course,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    frame_seen_surfaces=frame_seen_surfaces,
                    nav_label=None,
                )
            )
        else:
            artifacts.append(
                await self._capture_current_page_artifact(
                    page=pearson_page,
                    course_id=course_id,
                    course_code=course.code,
                    source_platform="pearson_mylab",
                    page_kind="external_course_page",
                    title=course.name,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                )
            )
            artifacts.extend(
                await self._capture_pearson_embedded_frames_and_rows(
                    pearson_page=pearson_page,
                    external_scraper=external_scraper,
                    course=course,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    frame_seen_surfaces=frame_seen_surfaces,
                    nav_label=None,
                )
            )
            nav_entries: list[tuple[str, str]] = []
            for index in range(nav_count):
                candidate = nav_locator.nth(index)
                try:
                    if not await candidate.is_visible():
                        continue
                    label = clean_text(await candidate.inner_text() or "")
                    if not label:
                        continue
                    href = await candidate.get_attribute("href") or ""
                    nav_entries.append((label, href))
                except Exception:
                    continue

            filtered_nav: list[tuple[str, str, str]] = []
            for label, href in nav_entries:
                absolute_href = urljoin(pearson_page.url, href) if href else pearson_page.url
                if href and not pearson_href_in_course_scope(absolute_href):
                    continue
                filtered_nav.append((label, href, absolute_href))

            cap = self.settings.crawl_ai_max_links_per_page
            if len(filtered_nav) > cap:
                filtered_nav = filtered_nav[:cap]

            links_for_ai = [(abs_url, label) for label, _href, abs_url in filtered_nav]

            pearson_selection: LinkSelectionResult | None = None
            pearson_link_err: str | None = None
            if self._use_ai_link_navigation():
                page_text = await self._extract_page_text(pearson_page)
                picker = CrawlLinkPicker(self.settings)
                try:
                    pearson_selection = await picker.pick_link_selection(
                        platform="pearson",
                        page_url=pearson_page.url,
                        page_text=page_text,
                        course_code=course.code,
                        course_name=course.name,
                        links=links_for_ai,
                    )
                    follow = pearson_selection.follow
                except Exception as exc:
                    pearson_link_err = str(exc)
                    logger.warning(
                        "crawl.pearson_ai_link_pick_failed",
                        url=pearson_page.url,
                        error=pearson_link_err,
                    )
                    follow = heuristic_follow_indices_pearson_nav(
                        [(label, abs_url) for label, _h, abs_url in filtered_nav]
                    )
            else:
                follow = heuristic_follow_indices_pearson_nav(
                    [(label, abs_url) for label, _h, abs_url in filtered_nav]
                )

            artifacts[0] = _crawl_root_artifact_with_llm_link_meta(
                artifacts[0],
                platform="pearson_mylab",
                page_url=pearson_page.url,
                follow_indices=list(follow),
                notes=pearson_selection.notes if pearson_selection is not None else None,
                fallback=(
                    "heuristic_pearson"
                    if not self._use_ai_link_navigation()
                    else ("heuristic_after_llm_error" if pearson_link_err is not None else None)
                ),
                error=pearson_link_err,
            )

            planned_nav_labels: set[str] = set()
            nav_plans: list[tuple[int, str, str, str]] = []
            step_key = 0
            for index in follow:
                if nav_budget <= 0:
                    break
                if index < 0 or index >= len(filtered_nav):
                    continue
                label, href, absolute_href = filtered_nav[index]
                key = label.strip().lower()
                if key in planned_nav_labels:
                    continue
                planned_nav_labels.add(key)
                nav_budget -= 1
                nav_plans.append((step_key, label, href, absolute_href))
                step_key += 1

            page_concurrency = max(1, self.settings.crawl_page_concurrency)
            pearson_context = page.context
            frame_lock = asyncio.Lock()

            async def pearson_nav_via_click(
                plan: tuple[int, str, str, str],
            ) -> tuple[int, list[CrawlArtifact]]:
                sk, label, href, _absolute_href = plan
                if not await self._click_pearson_left_nav_item(pearson_page, label):
                    logger.info("crawl.pearson_nav_click_failed", course_id=course_id, label=label)
                    return (sk, [])
                await wait_after_navigation(pearson_page, timeout_ms=10_000)
                await wait_for_first_locator(
                    pearson_page,
                    "a.left-nav-item-select",
                    timeout_ms=4_000,
                )
                main_art = await self._capture_current_page_artifact(
                    page=pearson_page,
                    course_id=course_id,
                    course_code=course.code,
                    source_platform="pearson_mylab",
                    page_kind="external_course_nav_page",
                    title=label,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    capture_screenshots=capture_screenshots,
                    metadata={"nav_label": label, "nav_href": href or None},
                )
                async with frame_lock:
                    embedded = await self._capture_pearson_embedded_frames_and_rows(
                        pearson_page=pearson_page,
                        external_scraper=external_scraper,
                        course=course,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        frame_seen_surfaces=frame_seen_surfaces,
                        nav_label=label,
                    )
                return (sk, [main_art, *embedded])

            async def pearson_nav_via_goto(
                plan: tuple[int, str, str, str],
            ) -> tuple[int, list[CrawlArtifact]]:
                sk, label, href, absolute_href = plan
                worker = await pearson_context.new_page()
                worker.set_default_timeout(self.settings.browser_timeout_ms)
                try:
                    await goto_throttled(worker, absolute_href, self.settings)
                    await wait_after_navigation(worker, timeout_ms=10_000)
                    await wait_for_first_locator(
                        worker,
                        "a.left-nav-item-select",
                        timeout_ms=4_000,
                    )
                    main_art = await self._capture_current_page_artifact(
                        page=worker,
                        course_id=course_id,
                        course_code=course.code,
                        source_platform="pearson_mylab",
                        page_kind="external_course_nav_page",
                        title=label,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        capture_screenshots=capture_screenshots,
                        metadata={"nav_label": label, "nav_href": href or None},
                    )
                    async with frame_lock:
                        embedded = await self._capture_pearson_embedded_frames_and_rows(
                            pearson_page=worker,
                            external_scraper=external_scraper,
                            course=course,
                            course_dir=course_dir,
                            fetched_at=fetched_at,
                            frame_seen_surfaces=frame_seen_surfaces,
                            nav_label=label,
                        )
                    return (sk, [main_art, *embedded])
                except Exception as exc:
                    logger.info(
                        "crawl.pearson_nav_goto_failed",
                        course_id=course_id,
                        url=absolute_href,
                        error=str(exc),
                    )
                    return (sk, [])
                finally:
                    await worker.close()

            def _pearson_href_allows_parallel_goto(h: str, abs_u: str) -> bool:
                if not (h or "").strip():
                    return False
                au = abs_u.strip().lower()
                return au.startswith("http://") or au.startswith("https://")

            if page_concurrency == 1:
                for plan in nav_plans:
                    artifacts.extend((await pearson_nav_via_click(plan))[1])
            else:
                plan_index = 0
                while plan_index < len(nav_plans):
                    plan = nav_plans[plan_index]
                    if not _pearson_href_allows_parallel_goto(plan[2], plan[3]):
                        artifacts.extend((await pearson_nav_via_click(plan))[1])
                        plan_index += 1
                        continue
                    batch: list[tuple[int, str, str, str]] = []
                    while (
                        plan_index < len(nav_plans)
                        and _pearson_href_allows_parallel_goto(
                            nav_plans[plan_index][2],
                            nav_plans[plan_index][3],
                        )
                        and len(batch) < page_concurrency
                    ):
                        batch.append(nav_plans[plan_index])
                        plan_index += 1
                    results = await asyncio.gather(
                        *[pearson_nav_via_goto(p) for p in batch],
                        return_exceptions=True,
                    )
                    ordered: list[tuple[int, list[CrawlArtifact]]] = []
                    for item in results:
                        if isinstance(item, Exception):
                            logger.warning("crawl.pearson_nav_parallel_failed", error=str(item))
                            continue
                        ordered.append(item)
                    ordered.sort(key=lambda entry: entry[0])
                    for _, arts in ordered:
                        artifacts.extend(arts)

        await external_scraper._close_popup_if_needed(page, pearson_page)
        return artifacts

    async def _capture_pearson_embedded_frames_and_rows(
        self,
        *,
        pearson_page: Page,
        external_scraper: ExternalScraper,
        course: D2LCourseSnapshot,
        course_dir: Path,
        fetched_at: datetime,
        frame_seen_surfaces: set[tuple[str, str]],
        nav_label: str | None,
    ) -> list[CrawlArtifact]:
        course_id = build_course_id(course)
        artifacts: list[CrawlArtifact] = []
        metadata_base: dict[str, object] = {}
        if nav_label:
            metadata_base["nav_label"] = nav_label

        surface_key = nav_label.strip().lower() if nav_label else ""
        for frame in pearson_page.frames:
            furl = frame.url or ""
            if not is_pearson_mylab_course_tool_frame_url(furl):
                continue
            normalized = normalize_crawl_url(furl)
            dedupe_key = (normalized, surface_key)
            if dedupe_key in frame_seen_surfaces:
                continue
            frame_seen_surfaces.add(dedupe_key)
            page_kind = (
                "external_assignments_frame"
                if is_pearson_assignments_frame_url(furl)
                else "external_course_frame"
            )
            try:
                artifacts.append(
                    await self._capture_frame_artifact(
                        frame=frame,
                        course_id=course_id,
                        course_code=course.code,
                        source_platform="pearson_mylab",
                        page_kind=page_kind,
                        title=course.name,
                        parent_url=pearson_page.url,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        metadata=dict(metadata_base),
                    )
                )
            except PlaywrightTimeoutError:
                logger.info("crawl.pearson_frame_capture_failed", url=furl)
                continue

            rows = frame.locator("tr")
            for index in range(await rows.count()):
                row = rows.nth(index)
                if await row.locator("a").count() == 0:
                    continue
                score_text = await external_scraper._resolve_pearson_score_text(
                    row, timeout_parent=frame
                )
                row_meta: dict[str, object] = {
                    "row_index": index,
                    "pearson_score_text": score_text,
                }
                row_meta.update(metadata_base)
                artifacts.append(
                    await self._capture_locator_fragment(
                        locator=row,
                        course_id=course_id,
                        course_code=course.code,
                        source_platform="pearson_mylab",
                        page_kind="external_assignment_row",
                        title=await self._first_locator_text(row.locator("a").first),
                        parent_url=furl or pearson_page.url,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        metadata=row_meta,
                    )
                )

        rows = pearson_page.locator("li.assignment-row, div.assignment-row, tr.assignment-row")
        if await rows.count() == 0:
            return artifacts

        for index in range(await rows.count()):
            row = rows.nth(index)
            if await row.locator("a").count() == 0:
                continue
            score_text = await external_scraper._resolve_pearson_score_text(
                row, timeout_parent=pearson_page
            )
            row_meta = {"row_index": index, "pearson_score_text": score_text}
            row_meta.update(metadata_base)
            artifacts.append(
                await self._capture_locator_fragment(
                    locator=row,
                    course_id=course_id,
                    course_code=course.code,
                    source_platform="pearson_mylab",
                    page_kind="external_assignment_row",
                    title=await self._first_locator_text(row.locator("a").first),
                    parent_url=pearson_page.url,
                    course_dir=course_dir,
                    fetched_at=fetched_at,
                    metadata=row_meta,
                )
            )
        return artifacts

    async def _open_pearson_assignments_view(
        self,
        page: Page,
        *,
        external_scraper: ExternalScraper,
    ) -> None:
        for _ in range(3):
            if await external_scraper._maybe_accept_pearson_cookies(page):
                logger.info("crawl.pearson_cookies_accepted", url=page.url)
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=5_000)
                except PlaywrightTimeoutError:
                    await asyncio.sleep(0.2)

            if await external_scraper._has_pearson_rows(page):
                logger.info("crawl.pearson_rows_visible", url=page.url)
                return

            frame = await external_scraper._find_pearson_assignments_frame(page)
            if frame is not None:
                logger.info("crawl.pearson_frame_visible", url=frame.url)
                return

            if await external_scraper._maybe_open_pearson_assignments(page):
                logger.info("crawl.pearson_assignments_clicked", url=page.url)
                await wait_after_navigation(page, timeout_ms=12_000)
                continue

            if await self._click_pearson_left_nav_item(page, "Assignments"):
                logger.info("crawl.pearson_assignments_nav_clicked", url=page.url)
                await wait_after_navigation(page, timeout_ms=12_000)
                continue

            await asyncio.sleep(0.35)

    async def _click_pearson_left_nav_item(self, page: Page, label: str) -> bool:
        containers = page.locator("a.left-nav-item, a.left-nav-item-select")
        count = await containers.count()
        for index in range(count):
            candidate = containers.nth(index)
            try:
                if not await candidate.is_visible():
                    continue
                text = clean_text(await candidate.inner_text())
                if label.lower() not in text.lower():
                    continue
                await candidate.click(force=True)
                return True
            except Exception:
                continue
        return False

    async def _capture_d2l_seed_targets_parallel(
        self,
        targets: list[CrawlTarget],
        *,
        context: BrowserContext,
        course_id: str,
        course_code: str,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        on_progress: ProgressCallback | None,
        concurrency: int,
    ) -> list[CrawlArtifact]:
        if not targets:
            return []

        limit = max(1, min(concurrency, len(targets)))
        sem = asyncio.Semaphore(limit)
        progress_lock = asyncio.Lock()
        total = len(targets)

        async def work(ordinal: int, target: CrawlTarget) -> tuple[int, CrawlArtifact, list[CrawlArtifact]]:
            async with sem:
                worker = await context.new_page()
                worker.set_default_timeout(self.settings.browser_timeout_ms)
                try:
                    if on_progress is not None:
                        label = (target.title or target.page_kind or "page").strip()
                        async with progress_lock:
                            on_progress(
                                "Crawl",
                                f"{course_code}: D2L page {ordinal}/{total} — {label}",
                            )
                    extra_artifacts: list[CrawlArtifact] = []
                    artifact = await self._capture_page_artifact(
                        page=worker,
                        course_id=course_id,
                        course_code=course_code,
                        source_platform=target.source_platform,
                        page_kind=target.page_kind,
                        url=target.url,
                        title=target.title,
                        course_dir=course_dir,
                        fetched_at=fetched_at,
                        capture_screenshots=capture_screenshots,
                        extra_artifacts=extra_artifacts,
                    )
                    return (ordinal, artifact, extra_artifacts)
                finally:
                    await worker.close()

        pairs = await asyncio.gather(
            *[work(i, t) for i, t in enumerate(targets, start=1)],
        )
        pairs.sort(key=lambda item: item[0])
        ordered: list[CrawlArtifact] = []
        for _, artifact, extras in pairs:
            ordered.append(artifact)
            ordered.extend(extras)
        return ordered

    async def _capture_page_artifact(
        self,
        *,
        page: Page,
        course_id: str,
        course_code: str,
        source_platform: str,
        page_kind: str,
        url: str,
        title: str | None,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        metadata: dict[str, object] | None = None,
        extra_artifacts: list[CrawlArtifact] | None = None,
    ) -> CrawlArtifact:
        await goto_throttled(page, url, self.settings)
        await wait_after_navigation(page)
        artifact = await self._capture_current_page_artifact(
            page=page,
            course_id=course_id,
            course_code=course_code,
            source_platform=source_platform,
            page_kind=page_kind,
            title=title,
            course_dir=course_dir,
            fetched_at=fetched_at,
            capture_screenshots=capture_screenshots,
            metadata=metadata,
        )
        if source_platform == "d2l" and page_kind == "tool_calendar":
            full_schedule = await self._capture_d2l_calendar_full_schedule_artifact(
                page=page,
                course_id=course_id,
                course_code=course_code,
                source_platform=source_platform,
                title=title,
                course_dir=course_dir,
                fetched_at=fetched_at,
                capture_screenshots=capture_screenshots,
                metadata=metadata,
            )
            if full_schedule is not None and extra_artifacts is not None:
                extra_artifacts.append(full_schedule)
        return artifact

    async def _capture_d2l_calendar_full_schedule_artifact(
        self,
        *,
        page: Page,
        course_id: str,
        course_code: str,
        source_platform: str,
        title: str | None,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        metadata: dict[str, object] | None,
    ) -> CrawlArtifact | None:
        if not await self._open_d2l_full_schedule(page):
            return None
        full_schedule_metadata = dict(metadata or {})
        full_schedule_metadata["calendar_view"] = "full_schedule"
        return await self._capture_current_page_artifact(
            page=page,
            course_id=course_id,
            course_code=course_code,
            source_platform=source_platform,
            page_kind="tool_calendar_full_schedule",
            title=f"{title or 'Calendar'} - Full Schedule",
            course_dir=course_dir,
            fetched_at=fetched_at,
            capture_screenshots=capture_screenshots,
            metadata=full_schedule_metadata,
        )

    async def _open_d2l_full_schedule(self, page: Page) -> bool:
        candidates = (
            page.get_by_role("button", name=re.compile(r"^full schedule$", re.IGNORECASE)).first,
            page.get_by_role("tab", name=re.compile(r"^full schedule$", re.IGNORECASE)).first,
            page.get_by_role("link", name=re.compile(r"^full schedule$", re.IGNORECASE)).first,
            page.get_by_text("Full Schedule", exact=True).first,
        )
        for candidate in candidates:
            try:
                if await candidate.count() == 0:
                    continue
                if not await candidate.is_visible():
                    continue
                await candidate.click(force=True, timeout=8_000)
                await wait_after_navigation(page, timeout_ms=10_000)
                return True
            except Exception:
                continue
        return False

    async def _capture_current_page_artifact(
        self,
        *,
        page: Page,
        course_id: str,
        course_code: str,
        source_platform: str,
        page_kind: str,
        title: str | None,
        course_dir: Path,
        fetched_at: datetime,
        capture_screenshots: bool,
        metadata: dict[str, object] | None = None,
    ) -> CrawlArtifact:
        html = await page.content()
        text = await self._extract_page_text(page)
        final_title = title or await safe_page_title(page)
        final_url = page.url
        artifact_id = build_artifact_id(course_id, page_kind, final_title or final_url)
        html_path = course_dir / f"{artifact_id}.html"
        text_path = course_dir / f"{artifact_id}.txt"
        html_path.write_text(html, encoding="utf-8")
        text_path.write_text(text, encoding="utf-8")
        screenshot_path = None
        if capture_screenshots:
            screenshot_path = course_dir / f"{artifact_id}.png"
            await page.screenshot(path=str(screenshot_path), full_page=True)

        return CrawlArtifact(
            id=artifact_id,
            course_id=course_id,
            course_code=course_code,
            source_platform=source_platform,
            artifact_type="page",
            page_kind=page_kind,
            title=final_title,
            url=final_url,
            fetched_at=fetched_at,
            html_path=str(html_path),
            text_path=str(text_path),
            screenshot_path=str(screenshot_path) if screenshot_path else None,
            metadata=metadata or {},
        )

    async def _capture_frame_artifact(
        self,
        *,
        frame: Frame,
        course_id: str,
        course_code: str,
        source_platform: str,
        page_kind: str,
        title: str | None,
        parent_url: str,
        course_dir: Path,
        fetched_at: datetime,
        metadata: dict[str, object] | None = None,
    ) -> CrawlArtifact:
        html = await frame.content()
        text = await self._extract_frame_text(frame)
        artifact_id = build_artifact_id(course_id, page_kind, title or frame.url or parent_url)
        html_path = course_dir / f"{artifact_id}.html"
        text_path = course_dir / f"{artifact_id}.txt"
        html_path.write_text(html, encoding="utf-8")
        text_path.write_text(text, encoding="utf-8")
        return CrawlArtifact(
            id=artifact_id,
            course_id=course_id,
            course_code=course_code,
            source_platform=source_platform,
            artifact_type="frame",
            page_kind=page_kind,
            title=title,
            url=frame.url or None,
            parent_url=parent_url,
            fetched_at=fetched_at,
            html_path=str(html_path),
            text_path=str(text_path),
            screenshot_path=None,
            metadata=metadata or {},
        )

    async def _capture_locator_fragment(
        self,
        *,
        locator: Locator,
        course_id: str,
        course_code: str,
        source_platform: str,
        page_kind: str,
        title: str | None,
        parent_url: str,
        course_dir: Path,
        fetched_at: datetime,
        metadata: dict[str, object],
    ) -> CrawlArtifact:
        html = await locator.inner_html()
        text = clean_text(await locator.inner_text())
        artifact_id = build_artifact_id(course_id, page_kind, title or text or parent_url)
        html_path = course_dir / f"{artifact_id}.html"
        text_path = course_dir / f"{artifact_id}.txt"
        html_path.write_text(html, encoding="utf-8")
        text_path.write_text(text, encoding="utf-8")
        return CrawlArtifact(
            id=artifact_id,
            course_id=course_id,
            course_code=course_code,
            source_platform=source_platform,
            artifact_type="fragment",
            page_kind=page_kind,
            title=title,
            url=None,
            parent_url=parent_url,
            fetched_at=fetched_at,
            html_path=str(html_path),
            text_path=str(text_path),
            screenshot_path=None,
            metadata=metadata,
        )

    async def _extract_page_text(self, page: Page) -> str:
        if await page.locator("body").count() == 0:
            return ""
        return clean_text(await page.locator("body").inner_text())

    async def _extract_frame_text(self, frame: Frame) -> str:
        try:
            if await frame.locator("body").count() == 0:
                return ""
            return clean_text(await frame.locator("body").inner_text())
        except PlaywrightTimeoutError:
            return ""

    async def _maybe_accept_cengage_cookies(self, page: Page) -> None:
        for label in ("Accept All Cookies", "Allow All"):
            locator = page.get_by_text(label)
            if await locator.count() == 0:
                continue
            try:
                await locator.first.click(force=True, timeout=2_000)
                await wait_after_navigation(page, timeout_ms=5_000)
                return
            except Exception:
                continue

    async def _close_stale_external_pages(
        self,
        context: BrowserContext,
        *,
        host_tokens: tuple[str, ...],
    ) -> None:
        for candidate in list(context.pages):
            if any(token in candidate.url for token in host_tokens):
                try:
                    await candidate.close()
                except Exception:
                    continue

    async def _cengage_row_title(self, row: Locator) -> str | None:
        title_locator = row.locator(".activity-name").first
        if await title_locator.count() == 0:
            return None
        return clean_text(await title_locator.inner_text())

    async def _first_locator_text(self, locator: Locator) -> str | None:
        if await locator.count() == 0:
            return None
        return clean_text(await locator.inner_text())


def _tool_match_score(name: str, url: str, needles: tuple[str, ...]) -> int:
    hay = f"{name} {url}".lower()
    return sum(1 for needle in needles if needle in hay)


def _resolve_standard_d2l_tool_targets(course: D2LCourseSnapshot) -> dict[str, CrawlTarget]:
    best: dict[str, tuple[int, CrawlTarget]] = {}
    slots: tuple[tuple[str, str, tuple[str, ...]], ...] = (
        ("content", "tool_content", (" content", "/content/", "/le/content")),
        ("assignments", "tool_assignments", ("assignment", "dropbox", "/dropbox/")),
        ("quizzes", "tool_quizzes-exams", ("quiz", "quizzes", "exam", "test", "/quizzing/")),
        ("grades", "tool_grades", ("grade", "/lms/grades/", "/grades/")),
        ("announcements", "announcements_index", ("announce", "news", "/lms/news/")),
        ("calendar", "tool_calendar", ("calendar", "/le/calendar/")),
    )
    for tool in course.tool_links:
        for slot, page_kind, needles in slots:
            score = _tool_match_score(tool.name, tool.url, needles)
            if score <= 0:
                continue
            candidate = CrawlTarget(
                source_platform="d2l",
                page_kind=page_kind,
                url=tool.url,
                title=tool.name,
            )
            existing = best.get(slot)
            if existing is None or score > existing[0]:
                best[slot] = (score, candidate)
    return {slot: target for slot, (_, target) in best.items()}


def build_d2l_crawl_targets(course: D2LCourseSnapshot) -> list[CrawlTarget]:
    base = course.home_url.rsplit("/d2l/home/", 1)[0]
    defaults: dict[str, CrawlTarget] = {
        "home": CrawlTarget(
            source_platform="d2l",
            page_kind="course_home",
            url=course.home_url,
            title=course.name,
        ),
        "content": CrawlTarget(
            source_platform="d2l",
            page_kind="tool_content",
            url=f"{base}/d2l/le/content/{course.course_id}/Home",
            title="Content",
        ),
        "assignments": CrawlTarget(
            source_platform="d2l",
            page_kind="tool_assignments",
            url=f"{base}/d2l/lms/dropbox/dropbox.d2l?ou={course.course_id}",
            title="Assignments",
        ),
        "quizzes": CrawlTarget(
            source_platform="d2l",
            page_kind="tool_quizzes-exams",
            url=f"{base}/d2l/lms/quizzing/user/quizzes_list.d2l?ou={course.course_id}",
            title="Quizzes / Exams",
        ),
        "grades": CrawlTarget(
            source_platform="d2l",
            page_kind="tool_grades",
            url=f"{base}/d2l/lms/grades/my_grades/main.d2l?ou={course.course_id}",
            title="Grades",
        ),
        "calendar": CrawlTarget(
            source_platform="d2l",
            page_kind="tool_calendar",
            url=d2l_calendar_url(course.home_url, course.course_id),
            title="Calendar",
        ),
        "announcements": CrawlTarget(
            source_platform="d2l",
            page_kind="announcements_index",
            url=f"{base}/d2l/lms/news/main.d2l?ou={course.course_id}",
            title="Announcements",
        ),
    }
    resolved = defaults | _resolve_standard_d2l_tool_targets(course)
    ordered = [
        resolved["home"],
        resolved["content"],
        resolved["assignments"],
        resolved["quizzes"],
        resolved["grades"],
        resolved["calendar"],
        resolved["announcements"],
    ]
    ordered.extend(
        CrawlTarget(
            source_platform="d2l",
            page_kind="announcement_detail",
            url=announcement.url,
            title=announcement.title,
        )
        for announcement in course.announcements
    )
    deduped: list[CrawlTarget] = []
    seen: set[str] = set()
    for target in ordered:
        if target.url in seen:
            continue
        seen.add(target.url)
        deduped.append(target)
    return deduped


def infer_external_platform(course: D2LCourseSnapshot) -> str | None:
    primary_external_tool = choose_primary_external_tool(course.external_tools)
    if primary_external_tool is None:
        return None
    haystack = " ".join(
        value
        for value in (
            primary_external_tool.title,
            primary_external_tool.module_title,
            primary_external_tool.url,
            primary_external_tool.launch_url,
        )
        if value
    ).lower()
    if "cengage" in haystack or "mindtap" in haystack:
        return "cengage_mindtap"
    if "pearson" in haystack or "mastering" in haystack or "mylab" in haystack:
        return "pearson_mylab"
    return None


def build_artifact_id(course_id: str, page_kind: str, identity: str) -> str:
    digest = hashlib.sha1(identity.encode("utf-8")).hexdigest()[:10]
    return f"{course_id}-{slugify(page_kind)}-{digest}"


async def safe_page_title(page: Page) -> str | None:
    try:
        return await page.title()
    except Exception:
        return None


def clean_text(value: str) -> str:
    return " ".join(value.split())


async def extract_cengage_activity_id(row: Locator) -> str | None:
    title = row.locator(".title").first
    if await title.count() > 0:
        focus_tag = await title.get_attribute("data-focus-tag")
        if focus_tag and "-" in focus_tag:
            return focus_tag.split("-", 1)[0]

    title_name = row.locator(".activity-name").first
    if await title_name.count() > 0:
        activity_heading_id = await title_name.get_attribute("id")
        if activity_heading_id and activity_heading_id.startswith("activity-heading-"):
            return activity_heading_id.removeprefix("activity-heading-")
    return None


def build_cengage_detail_url(course_url: str, activity_id: str) -> str | None:
    parsed = urlparse(course_url)
    query = parse_qs(parsed.query)
    snapshot_id = query.get("snapshotId", [None])[0]
    deployment_id = query.get("deploymentId", [None])[0]
    eisbn = query.get("eISBN", [None])[0]
    if not snapshot_id or not deployment_id or not eisbn:
        return None

    return urlunparse(
        parsed._replace(
            query=f"deploymentId={deployment_id}&eISBN={eisbn}&id={activity_id}&snapshotId={snapshot_id}&"
        )
    )


def is_pearson_frame(frame: Frame) -> bool:
    return bool(frame.url and is_pearson_assignments_frame_url(frame.url))


async def collect_d2l_page_link_candidates(
    page: Page,
    *,
    page_url: str,
    course_id: str,
    d2l_host: str,
    max_links: int,
    max_anchors_scan: int = 600,
) -> list[tuple[str, str]]:
    anchors = page.locator("a[href]")
    count = await anchors.count()
    seen_norm: set[str] = set()
    out: list[tuple[str, str]] = []
    for index in range(min(count, max_anchors_scan)):
        if len(out) >= max_links:
            break
        handle = anchors.nth(index)
        try:
            href = await handle.get_attribute("href")
            if not href:
                continue
            stripped = href.strip()
            lowered = stripped.lower()
            if lowered.startswith("#") or lowered.startswith("javascript:") or lowered.startswith("mailto:"):
                continue
            absolute = urljoin(page_url, stripped)
            if not d2l_href_allowed_for_course(absolute, course_id, d2l_host):
                continue
            norm = normalize_crawl_url(absolute)
            if norm in seen_norm:
                continue
            seen_norm.add(norm)
            text = clean_text(await handle.inner_text() or "")
            out.append((absolute, text))
        except Exception:
            continue
    return out


async def collect_cengage_nav_link_candidates(page: Page, *, max_anchors: int = 400) -> list[tuple[str, str]]:
    anchors = page.locator("a[href]")
    count = await anchors.count()
    seen_hrefs: set[str] = set()
    out: list[tuple[str, str]] = []
    for index in range(min(count, max_anchors)):
        handle = anchors.nth(index)
        try:
            href = await handle.get_attribute("href")
            if not href:
                continue
            stripped = href.strip()
            lowered = stripped.lower()
            if lowered.startswith("#") or lowered.startswith("javascript"):
                continue
            if stripped in seen_hrefs:
                continue
            seen_hrefs.add(stripped)
            text = clean_text(await handle.inner_text() or "")
            out.append((stripped, text))
        except Exception:
            continue
    return out
