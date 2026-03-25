from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from html import unescape
import json
from pathlib import Path
import re
from typing import Iterable
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator
import structlog

from acc.ai.client import JsonModelClient, OpenAIChatClient, extract_json_text
from acc.ai.prompts import CRAWL_ASSIGNMENT_EXTRACTION_PROMPT, CRAWL_RULE_EXTRACTION_PROMPT
from acc.config import Settings
from acc.grading_signals import zero_grade_means_not_turned_in
from acc.progress import ProgressCallback
from acc.scrapers.snapshots import CrawlArtifact, CrawlCourseSnapshot, CrawlSnapshot

MAX_ASSIGNMENT_CHUNK_CHARS = 12_000
MAX_RULE_CHUNK_CHARS = 20_000
MAX_ARTIFACT_TEXT_CHARS = 4_000
ASSIGNMENT_PAGE_KINDS = {
    "announcement_detail",
    "content_outline_topic",
    "d2l_nav_page",
    "tool_assignments",
    "tool_quizzes-exams",
    "tool_grades",
    "tool_calendar",
    "tool_calendar_full_schedule",
    "course_home",
    "external_assignment_row",
    "external_assignment_page",
    "external_assignments_frame",
    "external_assignments_page",
    "external_course_page",
    "external_course_nav_page",
    "external_course_frame",
}
RULE_PAGE_KINDS = {
    "tool_grades",
    "tool_calendar",
    "tool_calendar_full_schedule",
    "syllabus_topic",
    "content_outline_topic",
    "d2l_nav_page",
    "tool_content",
    "course_home",
    "announcement_detail",
}
HTML_AUGMENT_PAGE_KINDS = {
    "announcement_detail",
    "announcements_index",
    "content_outline_topic",
    "d2l_nav_page",
    "course_home",
    "tool_assignments",
    "tool_content",
    "tool_grades",
    "tool_calendar",
    "tool_calendar_full_schedule",
    "tool_quizzes-exams",
    "external_course_nav_page",
    "external_course_frame",
}
COMPLETED_STATUSES = {"completed", "graded"}
SUBMITTED_STATUSES = {"submitted"}

logger = structlog.get_logger(__name__)


class EvidenceSpan(BaseModel):
    artifact_id: str
    quote: str


class ExtractedAssignmentFact(BaseModel):
    title: str
    assignment_type: str | None = None
    source_platform: str | None = None
    grade_category: str | None = None
    due_at: str | None = None
    due_on: str | None = None
    due_text: str | None = None
    weight_pct: float | None = None
    points_possible: float | None = None
    points_earned: float | None = None
    grade_pct: float | None = None
    submitted: bool | None = None
    graded: bool | None = None
    optional: bool | None = None
    extra_credit: bool | None = None
    counts_toward_grade: bool | None = None
    status: str | None = None
    rationale: str | None = None
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    evidence_artifact_ids: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class AssignmentExtractionResult(BaseModel):
    assignments: list[ExtractedAssignmentFact] = Field(default_factory=list)

    @field_validator("assignments", mode="before")
    @classmethod
    def _coerce_null_assignments(cls, value: object) -> object:
        return [] if value is None else value


class ExtractedGradeCategory(BaseModel):
    name: str
    weight: float | None = None
    notes: str | None = None


class ExtractedLatePolicy(BaseModel):
    raw_text: str | None = None
    accepts_late: bool | None = None
    default_penalty_per_day: float | None = None
    max_late_days: int | None = None


class RuleExtractionResult(BaseModel):
    course_code: str | None = None
    course_name: str | None = None
    grade_categories: list[ExtractedGradeCategory] = Field(default_factory=list)
    grading_scale: dict[str, list[float]] = Field(default_factory=dict)
    late_policy: ExtractedLatePolicy | None = None
    notes: list[str] = Field(default_factory=list)

    @field_validator("grade_categories", "notes", mode="before")
    @classmethod
    def _coerce_null_lists(cls, value: object) -> object:
        return [] if value is None else value

    @field_validator("grading_scale", mode="before")
    @classmethod
    def _coerce_null_grading_scale(cls, value: object) -> object:
        return {} if value is None else value


class ExtractedAssignment(BaseModel):
    title: str
    assignment_type: str | None = None
    source_platforms: list[str] = Field(default_factory=list)
    grade_category: str | None = None
    due_at: str | None = None
    due_on: str | None = None
    due_text: str | None = None
    weight_pct: float | None = None
    points_possible: float | None = None
    points_earned: float | None = None
    grade_pct: float | None = None
    submitted: bool | None = None
    graded: bool | None = None
    optional: bool = False
    extra_credit: bool = False
    counts_toward_grade: bool = True
    status: str | None = None
    rationale: str | None = None
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    evidence_artifact_ids: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ExtractedCourseResult(BaseModel):
    course_id: str
    course_code: str
    course_name: str
    assignments: list[ExtractedAssignment] = Field(default_factory=list)
    grade_categories: list[ExtractedGradeCategory] = Field(default_factory=list)
    grading_scale: dict[str, list[float]] = Field(default_factory=dict)
    late_policy: ExtractedLatePolicy | None = None
    current_grade_pct: float | None = None
    calculation_notes: list[str] = Field(default_factory=list)
    artifact_count: int = 0
    # Serialized to crawl-extracted.json; persisted to SQL provenance_events on crawl-sync-db.
    provenance_events: list[dict[str, object]] = Field(default_factory=list)


class CrawlExtractionSnapshot(BaseModel):
    extracted_at: datetime
    source_snapshot_path: str
    source_artifacts_dir: str
    courses: list[ExtractedCourseResult] = Field(default_factory=list)


@dataclass(slots=True)
class CrawlExtractionSummary:
    courses_extracted: int = 0
    courses_failed: int = 0


class CrawlExtractor:
    def __init__(
        self,
        settings: Settings,
        *,
        client: JsonModelClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or OpenAIChatClient(settings, context="crawl extraction request")

    async def save_snapshot(
        self,
        *,
        course_id: str | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> CrawlExtractionSnapshot:
        snapshot = await self.extract(course_id=course_id, on_progress=on_progress)
        if course_id is not None and self.settings.crawl_extracted_path.exists():
            snapshot = merge_saved_snapshot(
                CrawlExtractionSnapshot.model_validate_json(
                    self.settings.crawl_extracted_path.read_text(encoding="utf-8")
                ),
                snapshot,
            )
        self.settings.crawl_extracted_path.write_text(
            snapshot.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return snapshot

    async def extract(
        self,
        *,
        course_id: str | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> CrawlExtractionSnapshot:
        if not self.settings.crawl_snapshot_path.exists():
            raise FileNotFoundError(
                f"No crawl snapshot found at {self.settings.crawl_snapshot_path}. Run `acc crawl-snapshot` first."
            )
        crawl_snapshot = CrawlSnapshot.model_validate_json(
            self.settings.crawl_snapshot_path.read_text(encoding="utf-8")
        )

        to_run = [
            course
            for course in crawl_snapshot.courses
            if course_id is None or course.course_id == course_id
        ]
        total = len(to_run)
        if on_progress is not None and total == 0:
            on_progress("AI extraction", "No courses in crawl snapshot.")

        courses = []
        if total:
            concurrency = max(1, min(total, self.settings.openai_max_concurrent_requests))
            sem = asyncio.Semaphore(concurrency)

            async def extract_one(index: int, course: CrawlCourseSnapshot) -> tuple[int, ExtractedCourseResult]:
                async with sem:
                    if on_progress is not None:
                        on_progress(
                            "AI extraction",
                            f"{index}/{total}: {course.code} ({course.name})",
                        )
                    artifacts = [
                        artifact for artifact in crawl_snapshot.artifacts if artifact.course_id == course.course_id
                    ]
                    result = await self.extract_course(
                        course.course_id,
                        course.code,
                        course.name,
                        artifacts,
                        on_progress=on_progress,
                        crawl_artifacts_dir=crawl_snapshot.artifacts_dir,
                    )
                    return index, result

            pairs = await asyncio.gather(
                *[extract_one(index, course) for index, course in enumerate(to_run, start=1)]
            )
            for _, extracted in sorted(pairs, key=lambda item: item[0]):
                courses.append(extracted)

        return CrawlExtractionSnapshot(
            extracted_at=datetime.now(UTC),
            source_snapshot_path=str(self.settings.crawl_snapshot_path),
            source_artifacts_dir=crawl_snapshot.artifacts_dir,
            courses=courses,
        )

    async def extract_course(
        self,
        course_id: str,
        course_code: str,
        course_name: str,
        artifacts: list[CrawlArtifact],
        *,
        on_progress: ProgressCallback | None = None,
        crawl_artifacts_dir: str | None = None,
    ) -> ExtractedCourseResult:
        assignment_candidates = extract_structured_assignment_facts(
            artifacts,
            timezone=self.settings.timezone,
        )
        assignment_chunks = build_assignment_chunks(artifacts)
        rule_artifacts = select_rule_artifacts(artifacts)
        rule_chunks: list[list[CrawlArtifact]] = []
        if rule_artifacts:
            rule_chunks = build_artifact_chunks(rule_artifacts, max_chunk_chars=MAX_RULE_CHUNK_CHARS)

        provenance_buffer: list[dict[str, object]] = []

        llm_lock = threading.Lock()
        llm_seq = [0]

        def llm_before(detail: str) -> int:
            with llm_lock:
                llm_seq[0] += 1
                n = llm_seq[0]
            if on_progress is not None:
                on_progress(
                    "OpenAI",
                    f"{course_code}: #{n} sending chat/completions — {detail}",
                )
            return n

        def llm_after(n: int) -> None:
            if on_progress is not None:
                on_progress(
                    "OpenAI",
                    f"{course_code}: #{n} response received",
                )

        llm_track: tuple[Callable[[str], int], Callable[[int], None]] | None = (
            (llm_before, llm_after) if on_progress is not None else None
        )

        if on_progress is not None:
            cap = max(1, self.settings.openai_max_concurrent_requests)
            on_progress(
                "AI extraction",
                f"{course_code}: {len(artifacts)} artifacts — "
                f"{len(assignment_chunks)} assignment LLM chunk(s), {len(rule_chunks)} rules chunk(s) "
                f"(OpenAI cap: {cap} in flight)",
            )

        logger.info(
            "crawl_extract.course_started",
            course_id=course_id,
            course_code=course_code,
            artifacts=len(artifacts),
            assignment_chunks=len(assignment_chunks),
        )

        n_assignment_chunks = len(assignment_chunks)

        async def run_assignment_chunk_task(
            chunk_index: int,
            chunk: list[CrawlArtifact],
        ) -> tuple[int, AssignmentExtractionResult]:
            chunk_detail = (
                f"assignments chunk {chunk_index}/{n_assignment_chunks} ({len(chunk)} page(s))"
                if n_assignment_chunks
                else f"assignments ({len(chunk)} page(s))"
            )

            def log_chunk_slot_acquired() -> None:
                logger.info(
                    "crawl_extract.assignment_chunk_started",
                    course_id=course_id,
                    chunk_index=chunk_index,
                    chunk_size=len(chunk),
                )

            parsed = await self.extract_assignment_chunk(
                course_code,
                course_name,
                chunk,
                course_id=course_id,
                llm_detail=chunk_detail,
                llm_track=llm_track,
                on_slot_acquired=log_chunk_slot_acquired,
                provenance_buffer=provenance_buffer,
                crawl_artifacts_dir=crawl_artifacts_dir,
            )
            logger.info(
                "crawl_extract.assignment_chunk_completed",
                course_id=course_id,
                chunk_index=chunk_index,
                assignments_found=len(parsed.assignments),
            )
            return chunk_index, parsed

        if assignment_chunks:
            assignment_pairs = await asyncio.gather(
                *[
                    run_assignment_chunk_task(index, chunk)
                    for index, chunk in enumerate(assignment_chunks, start=1)
                ],
                return_exceptions=True,
            )
            for item in assignment_pairs:
                if isinstance(item, BaseException):
                    raise item
                _, parsed = item
                assignment_candidates.extend(parsed.assignments)

        rules = RuleExtractionResult()
        if rule_artifacts:
            logger.info(
                "crawl_extract.rule_extraction_started",
                course_id=course_id,
                artifacts=len(rule_artifacts),
                rule_chunks=len(rule_chunks),
            )
            if on_progress is not None:
                on_progress(
                    "AI extraction",
                    f"{course_code}: extracting grading rules ({len(rule_chunks)} LLM chunk(s))...",
                )

            n_rule_chunks = len(rule_chunks)

            async def run_rule_chunk_task(
                chunk_index: int,
                chunk: list[CrawlArtifact],
            ) -> tuple[int, RuleExtractionResult]:
                rule_detail = (
                    f"grading rules chunk {chunk_index}/{n_rule_chunks} ({len(chunk)} page(s))"
                )
                result = await self.extract_rule_artifacts(
                    course_code,
                    course_name,
                    chunk,
                    course_id=course_id,
                    llm_detail=rule_detail,
                    llm_track=llm_track,
                    provenance_buffer=provenance_buffer,
                    crawl_artifacts_dir=crawl_artifacts_dir,
                )
                return chunk_index, result

            rule_pairs = await asyncio.gather(
                *[
                    run_rule_chunk_task(index, chunk)
                    for index, chunk in enumerate(rule_chunks, start=1)
                ],
                return_exceptions=True,
            )
            rules = RuleExtractionResult()
            for item in rule_pairs:
                if isinstance(item, BaseException):
                    raise item
                _, result = item
                rules = merge_rule_results(rules, result)
            logger.info(
                "crawl_extract.rule_extraction_completed",
                course_id=course_id,
                categories=len(rules.grade_categories),
            )

        assignments = consolidate_assignments(assignment_candidates)
        current_grade_pct, calculation_notes = calculate_current_grade(
            assignments,
            grade_categories=rules.grade_categories,
            timezone=self.settings.timezone,
        )
        logger.info(
            "crawl_extract.course_completed",
            course_id=course_id,
            assignments=len(assignments),
            current_grade_pct=current_grade_pct,
        )
        return ExtractedCourseResult(
            course_id=course_id,
            course_code=course_code,
            course_name=course_name,
            assignments=assignments,
            grade_categories=rules.grade_categories,
            grading_scale=rules.grading_scale,
            late_policy=rules.late_policy,
            current_grade_pct=current_grade_pct,
            calculation_notes=calculation_notes,
            artifact_count=len(artifacts),
            provenance_events=provenance_buffer,
        )

    async def extract_assignment_chunk(
        self,
        course_code: str,
        course_name: str,
        artifacts: list[CrawlArtifact],
        *,
        course_id: str,
        llm_detail: str | None = None,
        llm_track: tuple[Callable[[str], int], Callable[[int], None]] | None = None,
        on_slot_acquired: Callable[[], None] | None = None,
        provenance_buffer: list[dict[str, object]] | None = None,
        crawl_artifacts_dir: str | None = None,
    ) -> AssignmentExtractionResult:
        detail = llm_detail or f"assignments ({len(artifacts)} page(s))"
        prompt = build_assignment_prompt(course_code, course_name, artifacts)
        track_holder: list[int | None] = [None]

        def merged_slot_acquired() -> None:
            if on_slot_acquired is not None:
                on_slot_acquired()
            if llm_track is not None:
                before, _ = llm_track
                track_holder[0] = before(detail)

        slot_cb = (
            merged_slot_acquired
            if on_slot_acquired is not None or llm_track is not None
            else None
        )
        try:
            if slot_cb is not None:
                slot_cb()
            response = await self.client.complete_json(prompt)
        except Exception as exc:
            tid = track_holder[0]
            if tid is not None and llm_track is not None:
                llm_track[1](tid)
            if should_split_after_error(exc) and len(artifacts) > 1:
                midpoint = len(artifacts) // 2
                logger.info(
                    "crawl_extract.assignment_chunk_split",
                    course_code=course_code,
                    chunk_size=len(artifacts),
                    error=str(exc),
                )
                left = await self.extract_assignment_chunk(
                    course_code,
                    course_name,
                    artifacts[:midpoint],
                    course_id=course_id,
                    llm_detail=f"{detail} (split 1/2)",
                    llm_track=llm_track,
                    provenance_buffer=provenance_buffer,
                    crawl_artifacts_dir=crawl_artifacts_dir,
                )
                right = await self.extract_assignment_chunk(
                    course_code,
                    course_name,
                    artifacts[midpoint:],
                    course_id=course_id,
                    llm_detail=f"{detail} (split 2/2)",
                    llm_track=llm_track,
                    provenance_buffer=provenance_buffer,
                    crawl_artifacts_dir=crawl_artifacts_dir,
                )
                return AssignmentExtractionResult(assignments=left.assignments + right.assignments)
            raise
        tid = track_holder[0]
        if tid is not None and llm_track is not None:
            llm_track[1](tid)
        result = AssignmentExtractionResult.model_validate(json.loads(extract_json_text(response)))
        append_crawl_extract_provenance(
            provenance_buffer,
            stage="llm_crawl_extract_assignments",
            course_id=course_id,
            detail={
                "chunk": detail,
                "artifact_ids": [a.id for a in artifacts],
                "assignment_count": len(result.assignments),
                "sample_titles": [a.title for a in result.assignments[:25]],
                "assignment_evidence": [
                    {
                        "title": assignment.title,
                        "rationale": assignment.rationale,
                        "evidence_spans": [
                            {"artifact_id": span.artifact_id, "quote": span.quote[:240]}
                            for span in assignment.evidence_spans[:5]
                        ],
                    }
                    for assignment in result.assignments[:25]
                ],
            },
            artifact_ref=crawl_artifacts_dir,
        )
        return result

    async def extract_rule_artifacts(
        self,
        course_code: str,
        course_name: str,
        artifacts: list[CrawlArtifact],
        *,
        course_id: str,
        llm_detail: str | None = None,
        llm_track: tuple[Callable[[str], int], Callable[[int], None]] | None = None,
        provenance_buffer: list[dict[str, object]] | None = None,
        crawl_artifacts_dir: str | None = None,
    ) -> RuleExtractionResult:
        detail = llm_detail or f"grading rules ({len(artifacts)} page(s))"
        prompt = build_rule_prompt(course_code, course_name, artifacts)
        track_holder: list[int | None] = [None]

        def announce_llm_slot() -> None:
            if llm_track is None:
                return
            before, _ = llm_track
            track_holder[0] = before(detail)

        try:
            if llm_track is not None:
                announce_llm_slot()
            response = await self.client.complete_json(prompt)
        except Exception as exc:
            tid = track_holder[0]
            if tid is not None and llm_track is not None:
                llm_track[1](tid)
            if should_split_after_error(exc) and len(artifacts) > 1:
                midpoint = len(artifacts) // 2
                logger.info(
                    "crawl_extract.rule_artifacts_split",
                    course_code=course_code,
                    artifact_count=len(artifacts),
                    error=str(exc),
                )
                left = await self.extract_rule_artifacts(
                    course_code,
                    course_name,
                    artifacts[:midpoint],
                    course_id=course_id,
                    llm_detail=f"{detail} (split 1/2)",
                    llm_track=llm_track,
                    provenance_buffer=provenance_buffer,
                    crawl_artifacts_dir=crawl_artifacts_dir,
                )
                right = await self.extract_rule_artifacts(
                    course_code,
                    course_name,
                    artifacts[midpoint:],
                    course_id=course_id,
                    llm_detail=f"{detail} (split 2/2)",
                    llm_track=llm_track,
                    provenance_buffer=provenance_buffer,
                    crawl_artifacts_dir=crawl_artifacts_dir,
                )
                return merge_rule_results(left, right)
            raise
        tid = track_holder[0]
        if tid is not None and llm_track is not None:
            llm_track[1](tid)
        parsed_rules = RuleExtractionResult.model_validate(json.loads(extract_json_text(response)))
        append_crawl_extract_provenance(
            provenance_buffer,
            stage="llm_crawl_extract_rules",
            course_id=course_id,
            detail={
                "chunk": detail,
                "artifact_ids": [a.id for a in artifacts],
                "grade_category_names": [c.name for c in parsed_rules.grade_categories],
                "has_late_policy": parsed_rules.late_policy is not None,
                "rule_notes": parsed_rules.notes[:10],
            },
            artifact_ref=crawl_artifacts_dir,
        )
        return parsed_rules


def append_crawl_extract_provenance(
    buffer: list[dict[str, object]] | None,
    *,
    stage: str,
    course_id: str,
    detail: dict[str, object],
    source_url: str | None = None,
    artifact_ref: str | None = None,
    text_preview: str | None = None,
) -> None:
    if buffer is None:
        return
    row: dict[str, object] = {"stage": stage, "course_id": course_id, "detail": detail}
    if source_url is not None:
        row["source_url"] = source_url
    if artifact_ref is not None:
        row["artifact_ref"] = artifact_ref
    if text_preview is not None:
        row["text_preview"] = text_preview
    buffer.append(row)


def build_assignment_prompt(
    course_code: str,
    course_name: str,
    artifacts: list[CrawlArtifact],
) -> str:
    return (
        f"{CRAWL_ASSIGNMENT_EXTRACTION_PROMPT}\n\n"
        f"COURSE: {course_code} - {course_name}\n\n"
        "ARTIFACTS:\n"
        + "\n\n".join(format_artifact_for_prompt(artifact) for artifact in artifacts)
    )


def build_rule_prompt(
    course_code: str,
    course_name: str,
    artifacts: list[CrawlArtifact],
) -> str:
    return (
        f"{CRAWL_RULE_EXTRACTION_PROMPT}\n\n"
        f"COURSE: {course_code} - {course_name}\n\n"
        "ARTIFACTS:\n"
        + "\n\n".join(format_artifact_for_prompt(artifact) for artifact in artifacts)
    )


def format_artifact_for_prompt(artifact: CrawlArtifact) -> str:
    text = read_artifact_text(artifact)
    truncated = text[:MAX_ARTIFACT_TEXT_CHARS]
    score_line = ""
    raw_score = artifact.metadata.get("pearson_score_text")
    if raw_score:
        score_line = f"pearson_score_text: {raw_score}\n"
    return (
        f"artifact_id: {artifact.id}\n"
        f"source_platform: {artifact.source_platform}\n"
        f"page_kind: {artifact.page_kind}\n"
        f"title: {artifact.title or ''}\n"
        f"url: {artifact.url or ''}\n"
        f"parent_url: {artifact.parent_url or ''}\n"
        f"{score_line}"
        f"text:\n{truncated}"
    ).strip()


def read_artifact_text(artifact: CrawlArtifact) -> str:
    parts: list[str] = []

    if artifact.text_path is not None:
        text_path = Path(artifact.text_path)
        if text_path.exists():
            text = text_path.read_text(encoding="utf-8").strip()
            if text:
                parts.append(text)

    if artifact.html_path is not None and artifact.page_kind in HTML_AUGMENT_PAGE_KINDS:
        html_path = Path(artifact.html_path)
        if html_path.exists():
            html_text = extract_text_from_html(html_path.read_text(encoding="utf-8"))
            if html_text:
                parts.append(html_text)

    return "\n\n".join(dedupe_strings(parts))


def extract_text_from_html(value: str) -> str:
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.IGNORECASE | re.DOTALL)
    cleaned = re.sub(
        r'<abbr\b[^>]*class="[^"]*d2l-fuzzydate[^"]*"[^>]*title="([^"]+)"[^>]*>(.*?)</abbr>',
        lambda match: (
            f"{collapse_whitespace(strip_html_tags(decode_html_entities(match.group(2))))} "
            f"({collapse_whitespace(decode_html_entities(match.group(1)))})"
        ).strip(),
        cleaned,
        flags=re.IGNORECASE | re.DOTALL,
    )

    blocks = [
        collapse_whitespace(strip_html_tags(decode_html_entities(match)))
        for match in re.findall(r'<d2l-html-block[^>]*\shtml="([^"]*)"', cleaned)
    ]
    blocks = [block for block in blocks if block]

    stripped = collapse_whitespace(strip_html_tags(decode_html_entities(cleaned)))
    text_parts = [*blocks, stripped]
    return "\n\n".join(dedupe_strings(text_parts))


def decode_html_entities(value: str) -> str:
    decoded = value
    for _ in range(3):
        next_value = unescape(decoded)
        if next_value == decoded:
            break
        decoded = next_value
    return decoded


def strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def build_assignment_chunks(artifacts: list[CrawlArtifact]) -> list[list[CrawlArtifact]]:
    relevant = [artifact for artifact in artifacts if artifact.page_kind in ASSIGNMENT_PAGE_KINDS]
    return build_artifact_chunks(relevant, max_chunk_chars=MAX_ASSIGNMENT_CHUNK_CHARS)


def build_artifact_chunks(
    artifacts: list[CrawlArtifact],
    *,
    max_chunk_chars: int,
) -> list[list[CrawlArtifact]]:
    chunks: list[list[CrawlArtifact]] = []
    current: list[CrawlArtifact] = []
    current_size = 0
    for artifact in artifacts:
        artifact_size = len(read_artifact_text(artifact)[:MAX_ARTIFACT_TEXT_CHARS]) + 400
        if current and current_size + artifact_size > max_chunk_chars:
            chunks.append(current)
            current = []
            current_size = 0
        current.append(artifact)
        current_size += artifact_size
    if current:
        chunks.append(current)
    return chunks


def select_rule_artifacts(artifacts: list[CrawlArtifact]) -> list[CrawlArtifact]:
    return [artifact for artifact in artifacts if artifact.page_kind in RULE_PAGE_KINDS]


def extract_structured_assignment_facts(
    artifacts: list[CrawlArtifact],
    *,
    timezone: str,
) -> list[ExtractedAssignmentFact]:
    facts: list[ExtractedAssignmentFact] = []
    for artifact in artifacts:
        if artifact.page_kind == "tool_content":
            facts.extend(extract_d2l_content_due_facts(artifact, timezone=timezone))
    return facts


def extract_d2l_content_due_facts(
    artifact: CrawlArtifact,
    *,
    timezone: str,
) -> list[ExtractedAssignmentFact]:
    if artifact.html_path is None:
        return []
    html_path = Path(artifact.html_path)
    if not html_path.exists():
        return []

    html = html_path.read_text(encoding="utf-8")
    pattern = re.compile(
        r"<h3[^>]*>([^<]+?)\s*-\s*Due</h3>.*?"
        r'<abbr\b[^>]*class="[^"]*d2l-fuzzydate[^"]*"[^>]*title="([^"]+)"[^>]*>.*?</abbr>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    facts: list[ExtractedAssignmentFact] = []
    now = datetime.now(ZoneInfo(timezone))
    for title_text, due_text in pattern.findall(html):
        title = collapse_whitespace(strip_html_tags(decode_html_entities(title_text)))
        due_label = collapse_whitespace(decode_html_entities(due_text))
        due_at = parse_fuzzy_due_datetime(due_label, timezone=timezone)
        if not title or due_at is None:
            continue
        due_datetime = datetime.fromisoformat(due_at)
        facts.append(
            ExtractedAssignmentFact(
                title=title,
                assignment_type=infer_assignment_type(title),
                source_platform="d2l",
                due_at=due_at,
                due_text=due_label,
                status="upcoming" if due_datetime >= now else "overdue",
                evidence_artifact_ids=[artifact.id],
                notes=["Parsed due date from D2L content."],
            )
        )
    return facts


def should_split_after_error(error: Exception) -> bool:
    if isinstance(error, TimeoutError):
        return True
    if not isinstance(error, RuntimeError):
        return False
    message = str(error).lower()
    return any(
        token in message
        for token in (
            "timed out",
            "timeout",
            "context length",
            "maximum context",
            "too large",
        )
    )


def merge_rule_results(left: RuleExtractionResult, right: RuleExtractionResult) -> RuleExtractionResult:
    categories_by_key: dict[str, ExtractedGradeCategory] = {}
    for category in [*left.grade_categories, *right.grade_categories]:
        key = category_key(category.name)
        if not key:
            continue
        existing = categories_by_key.get(key)
        if existing is None:
            categories_by_key[key] = category
            continue
        categories_by_key[key] = ExtractedGradeCategory(
            name=existing.name if len(existing.name) <= len(category.name) else category.name,
            weight=existing.weight if existing.weight is not None else category.weight,
            notes=existing.notes or category.notes,
        )

    return RuleExtractionResult(
        course_code=left.course_code or right.course_code,
        course_name=left.course_name or right.course_name,
        grade_categories=sorted(categories_by_key.values(), key=lambda category: category.name.lower()),
        grading_scale=left.grading_scale or right.grading_scale,
        late_policy=left.late_policy or right.late_policy,
        notes=dedupe_strings([*left.notes, *right.notes]),
    )


def merge_saved_snapshot(
    existing: CrawlExtractionSnapshot,
    updated: CrawlExtractionSnapshot,
) -> CrawlExtractionSnapshot:
    updated_by_course = {course.course_id: course for course in updated.courses}
    merged_courses: list[ExtractedCourseResult] = []
    for course in existing.courses:
        replacement = updated_by_course.pop(course.course_id, None)
        if replacement is None:
            merged_courses.append(course)
            continue
        combined_events = [*course.provenance_events, *replacement.provenance_events]
        merged_courses.append(replacement.model_copy(update={"provenance_events": combined_events}))
    merged_courses.extend(updated_by_course.values())
    return CrawlExtractionSnapshot(
        extracted_at=updated.extracted_at,
        source_snapshot_path=updated.source_snapshot_path,
        source_artifacts_dir=updated.source_artifacts_dir,
        courses=sorted(merged_courses, key=lambda course: course.course_id),
    )


def infer_assignment_type(title: str) -> str:
    lowered = title.lower()
    if "quiz" in lowered:
        return "exam"
    if "lab" in lowered or "programming exercise" in lowered:
        return "lab"
    if "discussion" in lowered:
        return "discussion"
    if "project" in lowered:
        return "project"
    if "reading" in lowered:
        return "reading"
    if "exam" in lowered or "test" in lowered:
        return "exam"
    return "homework"


def parse_fuzzy_due_datetime(value: str, *, timezone: str) -> str | None:
    for format_string in ("%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p"):
        try:
            parsed = datetime.strptime(value, format_string)
        except ValueError:
            continue
        return parsed.replace(tzinfo=ZoneInfo(timezone)).isoformat()
    return None


def consolidate_assignments(candidates: list[ExtractedAssignmentFact]) -> list[ExtractedAssignment]:
    grouped: dict[str, list[ExtractedAssignmentFact]] = {}
    for candidate in candidates:
        key = assignment_key(candidate.title)
        if key == "item":
            continue
        grouped.setdefault(key, []).append(candidate)

    assignments: list[ExtractedAssignment] = []
    for key in sorted(grouped):
        group = list(grouped[key])
        best_grade = max(group, key=grade_preference_key)
        best_due = preferred_due_candidate(group)
        assignments.append(
            ExtractedAssignment(
                title=preferred_title(group),
                assignment_type=first_non_null(item.assignment_type for item in group),
                source_platforms=sorted(
                    {item.source_platform for item in group if item.source_platform}
                ),
                grade_category=first_non_null(item.grade_category for item in group),
                due_at=best_due.due_at if best_due else None,
                due_on=best_due.due_on if best_due else None,
                due_text=best_due.due_text if best_due else first_non_null(item.due_text for item in group),
                weight_pct=first_non_null(item.weight_pct for item in group),
                points_possible=best_grade.points_possible
                if best_grade.points_possible is not None
                else first_non_null(item.points_possible for item in group),
                points_earned=best_grade.points_earned
                if best_grade.points_earned is not None
                else first_non_null(item.points_earned for item in group),
                grade_pct=best_grade.grade_pct
                if best_grade.grade_pct is not None
                else first_non_null(item.grade_pct for item in group),
                submitted=merge_bool(item.submitted for item in group),
                graded=merge_bool(item.graded for item in group),
                optional=any(bool(item.optional) for item in group),
                extra_credit=any(bool(item.extra_credit) for item in group),
                counts_toward_grade=merge_counts_toward_grade(group),
                status=preferred_status(group),
                rationale=first_non_null(item.rationale for item in group),
                evidence_spans=dedupe_evidence_spans(
                    span
                    for item in group
                    for span in item.evidence_spans
                ),
                evidence_artifact_ids=sorted(
                    {artifact_id for item in group for artifact_id in item.evidence_artifact_ids}
                ),
                notes=dedupe_strings(note for item in group for note in item.notes),
            )
        )

    return assignments


def calculate_current_grade(
    assignments: list[ExtractedAssignment],
    *,
    grade_categories: list[ExtractedGradeCategory],
    timezone: str,
) -> tuple[float | None, list[str]]:
    notes: list[str] = []
    now = datetime.now(ZoneInfo(timezone))
    gradeable = [
        assignment
        for assignment in assignments
        if assignment_counts_for_current_grade(assignment, now=now, timezone=timezone)
    ]

    if not gradeable:
        return None, ["No due and graded coursework available for calculation."]

    explicit_weight_points = 0.0
    explicit_weight_total = 0.0
    categories_with_explicit_weights: set[str] = set()
    for assignment in gradeable:
        if assignment.grade_pct is None or assignment.weight_pct in (None, 0):
            continue
        explicit_weight_points += assignment.weight_pct * (assignment.grade_pct / 100)
        explicit_weight_total += assignment.weight_pct
        if assignment.grade_category:
            categories_with_explicit_weights.add(category_key(assignment.grade_category))

    category_points = 0.0
    category_total = 0.0
    for category in grade_categories:
        key = category_key(category.name)
        if not key or key in categories_with_explicit_weights or category.weight in (None, 0):
            continue
        category_assignments = [assignment for assignment in gradeable if category_matches(assignment, key)]
        category_grade = average_grade(category_assignments)
        if category_grade is None:
            continue
        category_points += category.weight * (category_grade / 100)
        category_total += category.weight

    total_weight = explicit_weight_total + category_total
    if total_weight > 0:
        notes.append("Calculated from graded assignments that are already due.")
        return round(((explicit_weight_points + category_points) / total_weight) * 100, 2), notes

    straight_average = average_grade(gradeable)
    if straight_average is not None:
        notes.append("Used straight average because no usable category or item weights were found.")
        return round(straight_average, 2), notes
    return None, ["No graded coursework available for calculation."]


def assignment_counts_for_current_grade(
    assignment: ExtractedAssignment,
    *,
    now: datetime,
    timezone: str,
) -> bool:
    if assignment.optional or assignment.extra_credit:
        return False
    if assignment.counts_toward_grade is False:
        return False
    if assignment.submitted and not assignment.graded:
        return False
    if zero_grade_means_not_turned_in(
        grade_pct=assignment.grade_pct,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
    ):
        return False
    if assignment.grade_pct is None and assignment.points_earned is None:
        return False
    due_cutoff = assignment_due_cutoff(assignment, timezone=timezone)
    if due_cutoff is not None and due_cutoff > now:
        return False
    return True


def assignment_due_cutoff(assignment: ExtractedAssignment, *, timezone: str) -> datetime | None:
    if assignment.due_at:
        parsed = parse_datetime(assignment.due_at)
        if parsed is not None:
            return parsed.astimezone(ZoneInfo(timezone))
    if assignment.due_on:
        try:
            parsed_date = date.fromisoformat(assignment.due_on)
        except ValueError:
            return None
        return datetime.combine(parsed_date, time(23, 59), ZoneInfo(timezone))
    return None


def average_grade(assignments: list[ExtractedAssignment]) -> float | None:
    weighted_sum = 0.0
    weight_total = 0.0
    for assignment in assignments:
        if assignment.grade_pct is None:
            continue
        weight = assignment.points_possible if assignment.points_possible not in (None, 0) else 1.0
        weighted_sum += assignment.grade_pct * weight
        weight_total += weight
    if weight_total == 0:
        return None
    return weighted_sum / weight_total


def preferred_title(group: list[ExtractedAssignmentFact]) -> str:
    return min((candidate.title for candidate in group), key=lambda value: (len(value), value.lower()))


def preferred_due_candidate(group: list[ExtractedAssignmentFact]) -> ExtractedAssignmentFact | None:
    d2l_with_due = [candidate for candidate in group if candidate.source_platform == "d2l" and (candidate.due_at or candidate.due_on)]
    if d2l_with_due:
        return d2l_with_due[0]
    with_due = [candidate for candidate in group if candidate.due_at or candidate.due_on]
    return with_due[0] if with_due else None


def grade_preference_key(candidate: ExtractedAssignmentFact) -> tuple[int, float, int]:
    grade_pct = candidate.grade_pct if candidate.grade_pct is not None else -1.0
    return (
        1 if candidate.grade_pct is not None or candidate.points_earned is not None else 0,
        grade_pct,
        len(candidate.evidence_artifact_ids),
    )


def preferred_status(group: list[ExtractedAssignmentFact]) -> str | None:
    statuses = [candidate.status for candidate in group if candidate.status]
    for preferred in ("graded", "completed", "submitted", "in_progress", "overdue", "upcoming", "available"):
        if preferred in statuses:
            return preferred
    return statuses[0] if statuses else None


def merge_bool(values: Iterable[bool | None]) -> bool | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return any(filtered)


def merge_counts_toward_grade(group: list[ExtractedAssignmentFact]) -> bool:
    if any(item.counts_toward_grade is False for item in group):
        return False
    if any(item.extra_credit for item in group):
        return False
    if any(item.optional for item in group):
        return False
    if any(item.counts_toward_grade is True for item in group):
        return True
    return True


def dedupe_strings(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        output.append(cleaned)
        seen.add(cleaned)
    return output


def dedupe_evidence_spans(values: Iterable[EvidenceSpan]) -> list[EvidenceSpan]:
    seen: set[tuple[str, str]] = set()
    output: list[EvidenceSpan] = []
    for value in values:
        artifact_id = value.artifact_id.strip()
        quote = value.quote.strip()
        if not artifact_id or not quote:
            continue
        key = (artifact_id, quote)
        if key in seen:
            continue
        output.append(EvidenceSpan(artifact_id=artifact_id, quote=quote))
        seen.add(key)
    return output


def category_matches(assignment: ExtractedAssignment, category: str) -> bool:
    if assignment.grade_category:
        return category_key(assignment.grade_category) == category
    type_text = f"{assignment.assignment_type or ''} {assignment.title}".lower()
    if category in {"homework", "programming-exercises"}:
        return any(token in type_text for token in ("homework", "exercise", "assignment", "programming exercise"))
    if category in {"quizzes", "quiz", "tests", "test", "exams", "exam"}:
        return any(token in type_text for token in ("quiz", "test", "exam"))
    if category == "labs":
        return "lab" in type_text
    return False


def category_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def assignment_key(value: str) -> str:
    cleaned = value.lower().strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"\s*:\s*unit.*$", "", cleaned)
    return re.sub(r"[^a-z0-9]+", "-", cleaned).strip("-") or "item"


def parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def first_non_null(values: Iterable[object]) -> object | None:
    for value in values:
        if value is not None:
            return value
    return None
