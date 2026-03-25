from dataclasses import dataclass
from datetime import UTC, date, datetime
from collections.abc import Callable, Sequence
from collections import Counter
import re
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from acc.ai.crawl_extractor import (
    CrawlExtractionSnapshot,
    ExtractedAssignment,
    ExtractedCourseResult,
    ExtractedGradeCategory,
    ExtractedLatePolicy,
)
from acc.db.models import AgendaEntry, Assignment, Course, ProvenanceEvent
from acc.engine.normalizer import (
    NormalizedAssignment,
    NormalizedCourse,
    NormalizedSnapshot,
    build_assignment_id,
    infer_assignment_type,
    programming_exercise_match_key,
)
from acc.config import get_settings
from acc.grading_signals import zero_grade_means_not_turned_in
from acc.scrapers.snapshots import (
    CrawlArtifact,
    CrawlSnapshot,
    ExternalAssignmentSnapshot,
    ExternalScrapeSnapshot,
)


@dataclass(slots=True)
class SyncSummary:
    courses_upserted: int = 0
    assignments_upserted: int = 0
    assignments_deleted: int = 0


@dataclass(slots=True)
class CourseOverview:
    course: Course
    assignment_count: int = 0


@dataclass(slots=True)
class CanonicalAssignment:
    id: str
    course_id: str
    title: str
    description: str | None
    type: str
    source_platform: str
    external_url: str | None
    due_date: datetime | None
    grade_category: str | None
    grade_weight_pct: float | None
    grade_pct: float | None
    points_possible: float | None
    points_earned: float | None
    status: str
    late_policy: dict | None
    estimated_minutes: int | None
    raw_scraped_data: dict | None
    course: Course | None


STATUS_TAIL_RE = re.compile(
    r"(submitted|not started|in progress|grading in progress|counts towards grade).*$",
    re.IGNORECASE,
)
DISPLAY_BREAK_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
MATCH_SUFFIX_RE = re.compile(r"\b(?:due|in class)\b$", re.IGNORECASE)
UNIT_CONTEXT_RE = re.compile(r"^unit\b.*\b(?:code|apply)\b", re.IGNORECASE)
STATUS_SCORES = {
    "overdue": 5,
    "in_progress": 4,
    "upcoming": 3,
    "completed": 2,
    "graded": 2,
    "submitted": 2,
    "available": 1,
}
ROLLUP_TITLE_RE = re.compile(
    r"\b(final calculated grade|current grade|course total|total grade|weighted total|final grade)\b",
    re.IGNORECASE,
)


def apply_zero_grade_as_not_turned_in(
    *,
    status: str,
    is_submitted: bool,
    grade_pct: float | None,
    points_earned: float | None,
    points_possible: float | None,
    due_date: datetime | None,
    now: datetime | None = None,
) -> tuple[str, bool]:
    if not zero_grade_means_not_turned_in(
        grade_pct=grade_pct,
        points_earned=points_earned,
        points_possible=points_possible,
    ):
        return status, is_submitted
    now = now or datetime.now(UTC)
    is_submitted = False
    if status in {"graded", "completed", "submitted", "in_progress"}:
        if due_date is not None and due_date < now:
            return "overdue", is_submitted
        return "upcoming", is_submitted
    return status, is_submitted


def status_after_zero_grade_rule(
    status: str,
    *,
    grade_pct: float | None,
    points_earned: float | None,
    points_possible: float | None,
    due_date: datetime | None,
    now: datetime | None = None,
) -> str:
    adjusted, _ = apply_zero_grade_as_not_turned_in(
        status=status,
        is_submitted=False,
        grade_pct=grade_pct,
        points_earned=points_earned,
        points_possible=points_possible,
        due_date=due_date,
        now=now,
    )
    return adjusted


def inferred_grade_pct_from_points(
    points_earned: float | None,
    points_possible: float | None,
) -> float | None:
    if points_earned is None or points_possible in (None, 0):
        return None
    return round((points_earned / points_possible) * 100, 2)


def signal_grade_pct_for_missing_work(
    points_earned: float | None,
    points_possible: float | None,
    reported_grade_pct: float | None,
) -> float | None:
    inferred = inferred_grade_pct_from_points(points_earned, points_possible)
    if inferred is not None:
        return inferred
    return reported_grade_pct


class Repository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_course(self, course: Course) -> Course:
        existing = await self.session.get(Course, course.id)
        if existing is not None:
            if course.last_scraped_external is None:
                course.last_scraped_external = existing.last_scraped_external
            if course.last_syllabus_parse is None:
                course.last_syllabus_parse = existing.last_syllabus_parse
        stored = await self.session.merge(course)
        await self.session.flush()
        return stored

    async def upsert_assignment(self, assignment: Assignment) -> Assignment:
        stored = await self.session.merge(assignment)
        await self.session.flush()
        return stored

    async def sync_normalized_snapshot(self, snapshot: NormalizedSnapshot) -> SyncSummary:
        summary = SyncSummary()

        for course in snapshot.courses:
            await self.upsert_course(course_to_model(course, snapshot.source_fetched_at))
            summary.courses_upserted += 1

        for assignment in snapshot.assignments:
            await self.upsert_assignment(
                assignment_to_model(assignment, snapshot.source_fetched_at)
            )
            summary.assignments_upserted += 1

        summary.assignments_deleted = await self.delete_missing_snapshot_assignments(snapshot)
        return summary

    async def sync_external_snapshot(self, snapshot: ExternalScrapeSnapshot) -> SyncSummary:
        summary = SyncSummary()
        scraped_at = snapshot.fetched_at.astimezone(UTC)

        for external_course in snapshot.courses:
            stored_course = await self.session.get(Course, external_course.course_id)
            if stored_course is None:
                continue

            stored_course.last_scraped_external = scraped_at
            if stored_course.external_platform is None:
                stored_course.external_platform = external_course.source_platform
            if stored_course.external_platform_url is None:
                stored_course.external_platform_url = external_course.launch_url
            summary.courses_upserted += 1

        for assignment in snapshot.assignments:
            await self.upsert_assignment(external_assignment_to_model(assignment, snapshot.fetched_at))
            summary.assignments_upserted += 1

        summary.assignments_deleted = await self.delete_missing_external_assignments(snapshot)
        return summary

    async def sync_crawl_extraction_snapshot(
        self,
        snapshot: CrawlExtractionSnapshot,
        *,
        crawl_manifest: CrawlSnapshot | None = None,
        on_course_progress: Callable[[int, int, str], None] | None = None,
        prune_missing_assignments: bool = True,
    ) -> SyncSummary:
        summary = SyncSummary()
        scraped_at = snapshot.extracted_at.astimezone(UTC)
        total = len(snapshot.courses)

        for index, course in enumerate(snapshot.courses, start=1):
            if on_course_progress is not None:
                on_course_progress(index, total, course.course_code)

            d2l_course_id, d2l_url = resolve_d2l_home_from_crawl_manifest(
                crawl_manifest,
                course.course_id,
            )
            existing = await self.session.get(Course, course.course_id)
            crawl_artifacts_by_id: dict[str, CrawlArtifact] = {}
            if crawl_manifest is not None:
                crawl_artifacts_by_id = {
                    artifact.id: artifact
                    for artifact in crawl_manifest.artifacts
                    if artifact.course_id == course.course_id
                }
            db_course = crawl_course_to_model(
                course,
                scraped_at=scraped_at,
                d2l_course_id=d2l_course_id,
                d2l_url=d2l_url,
                existing=existing,
            )
            await self.upsert_course(db_course)
            summary.courses_upserted += 1

            for assignment in course.assignments:
                model = crawl_extracted_assignment_to_model(
                    assignment,
                    course_id=course.course_id,
                    scraped_at=scraped_at,
                    crawl_artifacts_by_id=crawl_artifacts_by_id,
                )
                await self.upsert_assignment(model)
                summary.assignments_upserted += 1

            for raw in course.provenance_events:
                if not isinstance(raw, dict):
                    continue
                stage = raw.get("stage")
                if not isinstance(stage, str) or not stage.strip():
                    continue
                detail = raw.get("detail")
                if not isinstance(detail, dict):
                    detail = {"payload": raw}
                await self.record_provenance_event(
                    stage=stage,
                    course_id=course.course_id,
                    assignment_id=raw.get("assignment_id")
                    if isinstance(raw.get("assignment_id"), str)
                    else None,
                    source_url=raw.get("source_url") if isinstance(raw.get("source_url"), str) else None,
                    artifact_ref=raw.get("artifact_ref") if isinstance(raw.get("artifact_ref"), str) else None,
                    text_preview=raw.get("text_preview") if isinstance(raw.get("text_preview"), str) else None,
                    detail=detail,
                )

        if prune_missing_assignments:
            summary.assignments_deleted = await self.delete_missing_crawl_assignments(snapshot)
        return summary

    async def delete_missing_snapshot_assignments(self, snapshot: NormalizedSnapshot) -> int:
        course_ids = [course.id for course in snapshot.courses]
        assignment_ids = [assignment.id for assignment in snapshot.assignments]

        if not course_ids:
            return 0

        statement = delete(Assignment).where(
            Assignment.course_id.in_(course_ids),
            Assignment.source_platform == "d2l",
        )
        if assignment_ids:
            statement = statement.where(Assignment.id.not_in(assignment_ids))

        result = await self.session.execute(statement)
        return result.rowcount or 0

    async def delete_missing_external_assignments(self, snapshot: ExternalScrapeSnapshot) -> int:
        if not snapshot.courses:
            return 0

        assignment_ids_by_scope: dict[tuple[str, str], list[str]] = {}
        for assignment in snapshot.assignments:
            key = (assignment.course_id, assignment.source_platform)
            assignment_ids_by_scope.setdefault(key, []).append(assignment.id)

        deleted = 0
        seen_scopes: set[tuple[str, str]] = set()
        for course in snapshot.courses:
            scope = (course.course_id, course.source_platform)
            if scope in seen_scopes:
                continue
            seen_scopes.add(scope)

            statement = delete(Assignment).where(
                Assignment.course_id == course.course_id,
                Assignment.source_platform == course.source_platform,
            )
            assignment_ids = assignment_ids_by_scope.get(scope, [])
            if assignment_ids:
                statement = statement.where(Assignment.id.not_in(assignment_ids))

            result = await self.session.execute(statement)
            deleted += result.rowcount or 0

        return deleted

    async def delete_missing_crawl_assignments(self, snapshot: CrawlExtractionSnapshot) -> int:
        course_ids = [course.course_id for course in snapshot.courses]
        assignment_ids: list[str] = []
        for course in snapshot.courses:
            for assignment in course.assignments:
                assignment_ids.append(
                    crawl_extracted_assignment_id(course.course_id, assignment),
                )

        if not course_ids:
            return 0

        orphan_statement = select(Assignment.id).where(Assignment.course_id.in_(course_ids))
        if assignment_ids:
            orphan_statement = orphan_statement.where(Assignment.id.not_in(assignment_ids))
        orphan_ids = list((await self.session.scalars(orphan_statement)).all())
        if orphan_ids:
            await self.session.execute(
                delete(AgendaEntry).where(AgendaEntry.assignment_id.in_(orphan_ids))
            )

        statement = delete(Assignment).where(Assignment.course_id.in_(course_ids))
        if assignment_ids:
            statement = statement.where(Assignment.id.not_in(assignment_ids))

        result = await self.session.execute(statement)
        return result.rowcount or 0

    async def list_course_overview(self) -> Sequence[CourseOverview]:
        canonical_assignments = await self.list_canonical_assignments()
        assignment_counts = Counter(assignment.course_id for assignment in canonical_assignments)
        effective_grades = compute_effective_course_grades(canonical_assignments)
        result = await self.session.execute(select(Course).order_by(Course.code.asc(), Course.name.asc()))
        overviews: list[CourseOverview] = []
        for course in result.scalars().all():
            effective_grade = effective_grades.get(course.id)
            if effective_grade is not None:
                course.current_grade_pct = effective_grade
            overviews.append(
                CourseOverview(course=course, assignment_count=assignment_counts.get(course.id, 0))
            )
        return overviews

    async def list_upcoming_assignments(
        self,
        limit: int = 25,
        now: datetime | None = None,
    ) -> Sequence[CanonicalAssignment]:
        assignments = await self.list_canonical_assignments()
        upcoming = [assignment for assignment in assignments if assignment.due_date is not None]
        if now is not None:
            upcoming = [assignment for assignment in upcoming if assignment.due_date >= now]
        return sorted(upcoming, key=lambda assignment: (assignment.due_date, assignment.title))[:limit]

    async def list_canonical_assignments(self) -> Sequence[CanonicalAssignment]:
        result = await self.session.scalars(
            select(Assignment)
            .options(selectinload(Assignment.course))
            .order_by(Assignment.course_id.asc(), Assignment.title.asc(), Assignment.id.asc())
        )
        canonical_assignments = reconcile_assignments(result.all())
        effective_grades = compute_effective_course_grades(canonical_assignments)
        for assignment in canonical_assignments:
            if assignment.course is None:
                continue
            effective_grade = effective_grades.get(assignment.course_id)
            if effective_grade is not None:
                assignment.course.current_grade_pct = effective_grade
        return canonical_assignments

    async def list_courses_for_syllabus_parse(
        self,
        *,
        force: bool = False,
        course_id: str | None = None,
    ) -> Sequence[Course]:
        statement = (
            select(Course)
            .where(Course.syllabus_raw_text.is_not(None))
            .order_by(Course.code.asc(), Course.name.asc())
        )
        if not force:
            statement = statement.where(Course.last_syllabus_parse.is_(None))
        if course_id is not None:
            statement = statement.where(Course.id == course_id)

        result = await self.session.scalars(statement)
        return result.all()

    async def record_provenance_event(
        self,
        *,
        stage: str,
        course_id: str | None = None,
        assignment_id: str | None = None,
        source_url: str | None = None,
        artifact_ref: str | None = None,
        text_preview: str | None = None,
        detail: dict | None = None,
    ) -> ProvenanceEvent:
        preview = text_preview[:8000] if text_preview else None
        event = ProvenanceEvent(
            stage=stage,
            course_id=course_id,
            assignment_id=assignment_id,
            source_url=source_url,
            artifact_ref=artifact_ref,
            text_preview=preview,
            detail=detail,
        )
        self.session.add(event)
        await self.session.flush()
        return event

    async def list_provenance_events(
        self,
        *,
        course_id: str | None = None,
        assignment_id: str | None = None,
        limit: int = 200,
    ) -> Sequence[ProvenanceEvent]:
        statement = select(ProvenanceEvent).order_by(ProvenanceEvent.id.desc()).limit(max(1, min(limit, 2000)))
        if course_id is not None:
            statement = statement.where(ProvenanceEvent.course_id == course_id)
        if assignment_id is not None:
            statement = statement.where(ProvenanceEvent.assignment_id == assignment_id)
        result = await self.session.scalars(statement)
        return result.all()


def course_to_model(course: NormalizedCourse, scraped_at) -> Course:
    return Course(
        id=course.id,
        code=course.code,
        name=course.name,
        instructor=None,
        d2l_course_id=course.d2l_course_id,
        d2l_url=course.d2l_url,
        semester=course.semester or "Unknown term",
        external_platform=course.external_platform,
        external_platform_url=course.external_platform_url,
        textbook=None,
        syllabus_raw_text=course.syllabus_raw_text,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=course.current_grade_pct,
        current_letter_grade=None,
        last_scraped_d2l=scraped_at.astimezone(UTC),
        last_scraped_external=None,
        last_syllabus_parse=None,
    )


def assignment_to_model(assignment: NormalizedAssignment, scraped_at) -> Assignment:
    due_date = assignment.due_at.astimezone(UTC) if assignment.due_at else None
    signal_grade = signal_grade_pct_for_missing_work(
        assignment.points_earned,
        assignment.points_possible,
        assignment.grade_pct,
    )
    status, is_submitted = apply_zero_grade_as_not_turned_in(
        status=assignment.status,
        is_submitted=False,
        grade_pct=signal_grade,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
        due_date=due_date,
    )
    return Assignment(
        id=assignment.id,
        course_id=assignment.course_id,
        title=assignment.title,
        description=None,
        type=assignment.type,
        source_platform=assignment.source_platform,
        external_url=assignment.external_url,
        available_date=None,
        due_date=due_date,
        close_date=None,
        grade_category=assignment.grade_category,
        grade_weight_pct=assignment.weight_possible,
        points_possible=assignment.points_possible,
        points_earned=assignment.points_earned,
        grade_pct=signal_grade,
        status=status,
        is_submitted=is_submitted,
        submitted_at=scraped_at.astimezone(UTC) if is_submitted else None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=assignment.raw_source,
        last_scraped=scraped_at.astimezone(UTC),
    )


def resolve_d2l_home_from_crawl_manifest(
    crawl_manifest: CrawlSnapshot | None,
    course_id: str,
) -> tuple[str, str]:
    if crawl_manifest is not None:
        for artifact in crawl_manifest.artifacts:
            if artifact.course_id != course_id:
                continue
            if artifact.page_kind != "course_home" or not artifact.url:
                continue
            url = artifact.url
            match = re.search(r"/d2l/home/(\d+)", url)
            if match is not None:
                return match.group(1), url
    return course_id, f"https://d2l.invalid/d2l/home/{course_id}"


def crawl_extracted_assignment_id(course_id: str, assignment: ExtractedAssignment) -> str:
    source_hint = assignment.evidence_artifact_ids[0] if assignment.evidence_artifact_ids else None
    return build_assignment_id(course_id, assignment.title, source_hint)


def late_policy_to_text(policy: ExtractedLatePolicy | None) -> str | None:
    if policy is None:
        return None
    parts: list[str] = []
    if policy.raw_text:
        parts.append(policy.raw_text.strip())
    if policy.accepts_late is not None:
        parts.append(f"Accepts late submissions: {policy.accepts_late}")
    if policy.default_penalty_per_day is not None:
        parts.append(f"Default penalty per day: {policy.default_penalty_per_day}%")
    if policy.max_late_days is not None:
        parts.append(f"Max late days: {policy.max_late_days}")
    return "\n".join(parts) if parts else None


def grade_categories_to_json(categories: list[ExtractedGradeCategory]) -> list[dict[str, object]]:
    return [category.model_dump(mode="json") for category in categories]


def infer_external_platform_from_assignments(
    assignments: list[ExtractedAssignment],
) -> tuple[str | None, str | None]:
    found: set[str] = set()
    for assignment in assignments:
        for platform in assignment.source_platforms:
            if platform in {"pearson_mylab", "cengage_mindtap"}:
                found.add(platform)
    if "pearson_mylab" in found:
        return "pearson_mylab", None
    if "cengage_mindtap" in found:
        return "cengage_mindtap", None
    return None, None


def normalize_crawl_status(assignment: ExtractedAssignment) -> str:
    raw = (assignment.status or "").lower().strip()
    if raw in {"graded", "completed", "submitted", "in_progress", "overdue", "upcoming", "available"}:
        return raw
    if assignment.graded:
        return "graded"
    if assignment.submitted:
        return "submitted"
    return "upcoming"


def crawl_assignment_type(assignment: ExtractedAssignment) -> str:
    hint = (assignment.assignment_type or "").lower().strip()
    if hint in {"homework", "exam", "lab", "discussion", "project", "reading"}:
        return hint
    return infer_assignment_type(assignment.title)


def crawl_source_platform(assignment: ExtractedAssignment) -> str:
    for platform in assignment.source_platforms:
        if platform in {"d2l", "pearson_mylab", "cengage_mindtap"}:
            return platform
    if assignment.source_platforms:
        return assignment.source_platforms[0]
    return "d2l"


def crawl_course_to_model(
    course: ExtractedCourseResult,
    *,
    scraped_at: datetime,
    d2l_course_id: str,
    d2l_url: str,
    existing: Course | None,
) -> Course:
    ext_platform, ext_url = infer_external_platform_from_assignments(course.assignments)
    if existing is not None:
        ext_platform = ext_platform or existing.external_platform
        ext_url = ext_url or existing.external_platform_url

    grading_scale = course.grading_scale or None
    grade_categories = grade_categories_to_json(course.grade_categories) if course.grade_categories else None
    late_text = late_policy_to_text(course.late_policy)

    return Course(
        id=course.course_id,
        code=course.course_code,
        name=course.course_name,
        instructor=existing.instructor if existing else None,
        d2l_course_id=d2l_course_id,
        d2l_url=d2l_url,
        semester=existing.semester if existing is not None else "Unknown term",
        external_platform=ext_platform,
        external_platform_url=ext_url,
        textbook=existing.textbook if existing else None,
        syllabus_raw_text=existing.syllabus_raw_text if existing else None,
        syllabus_parsed=existing.syllabus_parsed if existing else None,
        grading_scale=grading_scale,
        grade_categories=grade_categories,
        late_policy_global=late_text,
        current_grade_pct=None,
        current_letter_grade=existing.current_letter_grade if existing else None,
        last_scraped_d2l=scraped_at,
        last_scraped_external=scraped_at,
        last_syllabus_parse=existing.last_syllabus_parse if existing else None,
    )


def crawl_extracted_assignment_to_model(
    assignment: ExtractedAssignment,
    *,
    course_id: str,
    scraped_at: datetime,
    crawl_artifacts_by_id: dict[str, CrawlArtifact] | None = None,
) -> Assignment:
    due_date = parse_extracted_due_datetime(assignment)
    status = normalize_crawl_status(assignment)
    submitted = bool(assignment.submitted) or status in {"submitted", "graded", "completed"}
    signal_grade = signal_grade_pct_for_missing_work(
        assignment.points_earned,
        assignment.points_possible,
        assignment.grade_pct,
    )
    status, submitted = apply_zero_grade_as_not_turned_in(
        status=status,
        is_submitted=submitted,
        grade_pct=signal_grade,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
        due_date=due_date,
    )
    assignment_type = crawl_assignment_type(assignment)
    source_platform = crawl_source_platform(assignment)
    external_url = inferred_assignment_url_from_artifacts(
        assignment,
        source_platform=source_platform,
        crawl_artifacts_by_id=crawl_artifacts_by_id,
    )

    raw_payload: dict[str, object] = {
        "source": "crawl_extract",
        "due_text": assignment.due_text,
        "due_on": assignment.due_on,
        "due_at": assignment.due_at,
        "rationale": assignment.rationale,
        "evidence_spans": [span.model_dump(mode="json") for span in assignment.evidence_spans],
        "notes": assignment.notes,
        "evidence_artifact_ids": assignment.evidence_artifact_ids,
        "optional": assignment.optional,
        "extra_credit": assignment.extra_credit,
        "counts_toward_grade": assignment.counts_toward_grade,
        "inferred_external_url": external_url,
    }

    return Assignment(
        id=crawl_extracted_assignment_id(course_id, assignment),
        course_id=course_id,
        title=assignment.title,
        description=None,
        type=assignment_type,
        source_platform=source_platform,
        external_url=external_url,
        available_date=None,
        due_date=due_date,
        close_date=None,
        grade_category=assignment.grade_category,
        grade_weight_pct=assignment.weight_pct,
        points_possible=assignment.points_possible,
        points_earned=assignment.points_earned,
        grade_pct=None,
        status=status,
        is_submitted=submitted,
        submitted_at=scraped_at if submitted else None,
        is_late=status == "overdue",
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=raw_payload,
        last_scraped=scraped_at,
    )


def parse_extracted_due_datetime(assignment: ExtractedAssignment) -> datetime | None:
    if assignment.due_at:
        try:
            parsed = datetime.fromisoformat(assignment.due_at.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
    if assignment.due_on:
        try:
            parsed_date = datetime.fromisoformat(assignment.due_on)
        except ValueError:
            return None
        return datetime(parsed_date.year, parsed_date.month, parsed_date.day, 23, 59, tzinfo=UTC)
    return None


def inferred_assignment_url_from_artifacts(
    assignment: ExtractedAssignment,
    *,
    source_platform: str,
    crawl_artifacts_by_id: dict[str, CrawlArtifact] | None,
) -> str | None:
    if not crawl_artifacts_by_id:
        return None
    candidate_ids: list[str] = []
    candidate_ids.extend(assignment.evidence_artifact_ids)
    candidate_ids.extend(span.artifact_id for span in assignment.evidence_spans)
    seen: set[str] = set()
    for artifact_id in candidate_ids:
        if artifact_id in seen:
            continue
        seen.add(artifact_id)
        artifact = crawl_artifacts_by_id.get(artifact_id)
        if artifact is None:
            continue
        url = getattr(artifact, "url", None)
        parent_url = getattr(artifact, "parent_url", None)
        if isinstance(url, str) and url.strip():
            return url.strip()
        if isinstance(parent_url, str) and parent_url.strip():
            return parent_url.strip()
    for artifact in crawl_artifacts_by_id.values():
        platform = getattr(artifact, "source_platform", None)
        if platform != source_platform:
            continue
        url = getattr(artifact, "url", None)
        if isinstance(url, str) and url.strip():
            return url.strip()
    return None


def external_assignment_to_model(assignment: ExternalAssignmentSnapshot, scraped_at) -> Assignment:
    submitted = assignment.status in {"completed", "submitted"}
    due_date = assignment.due_at.astimezone(UTC) if assignment.due_at else None
    signal_grade = signal_grade_pct_for_missing_work(
        assignment.points_earned,
        assignment.points_possible,
        assignment.grade_pct,
    )
    status, submitted = apply_zero_grade_as_not_turned_in(
        status=assignment.status,
        is_submitted=submitted,
        grade_pct=signal_grade,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
        due_date=due_date,
    )
    return Assignment(
        id=assignment.id,
        course_id=assignment.course_id,
        title=assignment.title,
        description=assignment.description,
        type=assignment.type,
        source_platform=assignment.source_platform,
        external_url=assignment.external_url,
        available_date=None,
        due_date=due_date,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=assignment.points_possible,
        points_earned=assignment.points_earned,
        grade_pct=None,
        status=status,
        is_submitted=submitted,
        submitted_at=scraped_at.astimezone(UTC) if submitted else None,
        is_late=assignment.status == "overdue",
        days_late=0,
        late_policy=None,
        estimated_minutes=assignment.estimated_minutes,
        is_multi_day=False,
        raw_scraped_data=assignment.raw_source,
        last_scraped=scraped_at.astimezone(UTC),
    )


def reconcile_assignments(assignments: Sequence[Assignment]) -> list[CanonicalAssignment]:
    groups: dict[tuple[str, str], list[Assignment]] = {}
    for assignment in assignments:
        if is_rollup_assignment_row(assignment):
            continue
        key = (assignment.course_id, assignment_match_key(assignment.title))
        groups.setdefault(key, []).append(assignment)

    canonical: list[CanonicalAssignment] = []
    for key in sorted(groups):
        canonical.append(merge_assignment_group(groups[key]))
    return canonical


def merge_assignment_group(assignments: Sequence[Assignment]) -> CanonicalAssignment:
    preferred = preferred_canonical_source(assignments)
    preferred_grade = max(assignments, key=grade_data_preference_score)
    merged_raw_data = merge_raw_data(assignments)
    due_date = preferred_due_date(assignments)
    due_norm = normalize_datetime(due_date)
    merged_grade_pct = assignment_grade_pct(preferred_grade)
    merged_points_possible = (
        preferred_grade.points_possible
        if preferred_grade.points_possible is not None
        else next(
            (assignment.points_possible for assignment in assignments if assignment.points_possible is not None),
            None,
        )
    )
    merged_points_earned = (
        preferred_grade.points_earned
        if preferred_grade.points_earned is not None
        else next(
            (assignment.points_earned for assignment in assignments if assignment.points_earned is not None),
            None,
        )
    )
    merged_status = status_after_zero_grade_rule(
        preferred_status(assignments),
        grade_pct=merged_grade_pct,
        points_earned=merged_points_earned,
        points_possible=merged_points_possible,
        due_date=due_norm,
    )
    merged_raw_data = with_reconcile_provenance(
        merged_raw_data,
        assignments=assignments,
        canonical_source=preferred,
        grade_source=preferred_grade,
        due_date=due_norm,
    )
    return CanonicalAssignment(
        id=preferred.id,
        course_id=preferred.course_id,
        title=preferred_display_title(assignments),
        description=preferred.description
        or next((assignment.description for assignment in assignments if assignment.description), None),
        type=preferred.type,
        source_platform=preferred.source_platform,
        external_url=preferred_external_url(assignments),
        due_date=due_norm,
        grade_category=preferred.grade_category
        or next(
            (assignment.grade_category for assignment in assignments if assignment.grade_category),
            None,
        ),
        grade_weight_pct=preferred.grade_weight_pct
        or next(
            (assignment.grade_weight_pct for assignment in assignments if assignment.grade_weight_pct),
            None,
        ),
        grade_pct=merged_grade_pct,
        points_possible=merged_points_possible,
        points_earned=merged_points_earned,
        status=merged_status,
        late_policy=preferred.late_policy
        or next(
            (assignment.late_policy for assignment in assignments if assignment.late_policy),
            None,
        ),
        estimated_minutes=preferred.estimated_minutes
        or next(
            (assignment.estimated_minutes for assignment in assignments if assignment.estimated_minutes),
            None,
        ),
        raw_scraped_data=merged_raw_data,
        course=preferred.course,
    )


def preferred_due_date(assignments: Sequence[Assignment]) -> datetime | None:
    d2l_due_date = next(
        (
            assignment.due_date
            for assignment in assignments
            if assignment.source_platform == "d2l" and assignment.due_date is not None
        ),
        None,
    )
    if d2l_due_date is not None:
        return d2l_due_date

    preferred = max(assignments, key=assignment_preference_score)
    if preferred.due_date is not None:
        return preferred.due_date

    return next(
        (assignment.due_date for assignment in assignments if assignment.due_date is not None),
        None,
    )


def preferred_canonical_source(assignments: Sequence[Assignment]) -> Assignment:
    d2l_rows = [assignment for assignment in assignments if assignment.source_platform == "d2l"]
    if d2l_rows:
        return max(d2l_rows, key=assignment_preference_score)
    return max(assignments, key=assignment_preference_score)


def assignment_preference_score(assignment: Assignment) -> tuple[int, int, int, int]:
    has_grade_signal = assignment_grade_pct(assignment) is not None
    return (
        1 if assignment.source_platform != "d2l" else 0,
        1 if has_grade_signal or assignment.points_earned is not None else 0,
        1 if assignment.estimated_minutes is not None else 0,
        1 if assignment.due_date is not None else 0,
    )


def grade_data_preference_score(assignment: Assignment) -> tuple[int, int, int, int, datetime]:
    grade_pct = assignment_grade_pct(assignment)
    return (
        1 if grade_pct is not None else 0,
        int(round((grade_pct or -1.0) * 100)),
        1 if assignment.source_platform != "d2l" else 0,
        1 if assignment.points_earned is not None or assignment.points_possible is not None else 0,
        normalize_datetime(assignment.last_scraped) or datetime.min.replace(tzinfo=UTC),
    )


def preferred_status(assignments: Sequence[Assignment]) -> str:
    if any(
        assignment.status in {"completed", "graded", "submitted"}
        and assignment.source_platform != "d2l"
        for assignment in assignments
    ):
        return "completed"
    if any(assignment.status == "in_progress" for assignment in assignments):
        return "in_progress"
    if any(assignment.status == "overdue" for assignment in assignments):
        return "overdue"
    if any(assignment.status == "upcoming" for assignment in assignments):
        return "upcoming"
    return max(assignments, key=lambda assignment: STATUS_SCORES.get(assignment.status, 0)).status


def preferred_display_title(assignments: Sequence[Assignment]) -> str:
    candidates = [display_title(assignment.title) for assignment in assignments]
    return min(candidates, key=lambda value: (len(value), value.lower()))


def preferred_external_url(assignments: Sequence[Assignment]) -> str | None:
    for assignment in assignments:
        if assignment.external_url and assignment.external_url != "#/#":
            return assignment.external_url
    return None


def merge_raw_data(assignments: Sequence[Assignment]) -> dict | None:
    merged: dict[str, object] = {}
    for assignment in assignments:
        if assignment.raw_scraped_data:
            merged.update(assignment.raw_scraped_data)
    return merged or None


def with_reconcile_provenance(
    merged_raw_data: dict | None,
    *,
    assignments: Sequence[Assignment],
    canonical_source: Assignment,
    grade_source: Assignment,
    due_date: datetime | None,
) -> dict:
    payload: dict[str, object] = dict(merged_raw_data or {})
    payload["_reconcile"] = {
        "inputs": [
            {
                "id": assignment.id,
                "source_platform": assignment.source_platform,
                "status": assignment.status,
                "has_due_date": assignment.due_date is not None,
                "has_grade": assignment_grade_pct(assignment) is not None,
            }
            for assignment in assignments
        ],
        "canonical_source_id": canonical_source.id,
        "grade_source_id": grade_source.id,
        "due_source": "d2l" if any(
            assignment.source_platform == "d2l" and assignment.due_date == due_date
            for assignment in assignments
        ) else "best_available",
        "strategy": "d2l_canonical_external_fill",
    }
    return payload


def is_rollup_assignment_row(assignment: Assignment) -> bool:
    if ROLLUP_TITLE_RE.search(assignment.title or "") is None:
        return False
    title = (assignment.title or "").strip().lower()
    if any(token in title for token in ("attendance", "participation", "discussion")):
        return False
    return True


def assignment_match_key(title: str) -> str:
    pe_key = programming_exercise_match_key(title)
    if pe_key is not None:
        return pe_key
    cleaned = display_title(title).lower()
    cleaned = MATCH_SUFFIX_RE.sub("", cleaned).strip(" :-")
    cleaned = " ".join(cleaned.split())
    return re.sub(r"[^a-z0-9]+", "-", cleaned).strip("-") or "item"


def display_title(title: str) -> str:
    cleaned = DISPLAY_BREAK_RE.sub(" ", title)
    cleaned = " ".join(cleaned.split())
    cleaned = STATUS_TAIL_RE.sub("", cleaned).strip(" :-|")
    parts = [part.strip() for part in cleaned.split(":")]
    if len(parts) >= 2 and UNIT_CONTEXT_RE.match(parts[1]):
        cleaned = parts[0]
    elif len(parts) >= 4 and parts[0].lower() == "pre-requisite":
        cleaned = ": ".join(parts[:2])
    cleaned = MATCH_SUFFIX_RE.sub("", cleaned).strip(" :-")
    return cleaned or title


def normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def canonical_due_instant_utc(assignment: CanonicalAssignment) -> datetime | None:
    """UTC instant for the assignment deadline (prefers crawl `due_at` ISO when present)."""
    raw = assignment.raw_scraped_data if isinstance(assignment.raw_scraped_data, dict) else None
    if raw:
        due_at_str = raw.get("due_at")
        if isinstance(due_at_str, str):
            text = due_at_str.strip()
            if text:
                try:
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    pass
                else:
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=UTC)
                    return parsed.astimezone(UTC)
    return normalize_datetime(assignment.due_date)


def canonical_due_calendar_date(assignment: CanonicalAssignment) -> date | None:
    """Calendar due date for planning and urgency (prefers crawl `due_on` / offset `due_at`)."""
    raw = assignment.raw_scraped_data if isinstance(assignment.raw_scraped_data, dict) else None
    if raw:
        due_on = raw.get("due_on")
        if isinstance(due_on, str):
            text = due_on.strip()
            if text:
                try:
                    return date.fromisoformat(text)
                except ValueError:
                    pass
        due_at_str = raw.get("due_at")
        if isinstance(due_at_str, str):
            text = due_at_str.strip()
            if text:
                try:
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    pass
                else:
                    if parsed.tzinfo is not None:
                        return parsed.date()
    due_utc = normalize_datetime(assignment.due_date)
    if due_utc is None:
        return None
    tz = ZoneInfo(get_settings().timezone)
    return due_utc.astimezone(tz).date()


def assignment_grade_pct(assignment: Assignment | CanonicalAssignment) -> float | None:
    inferred = inferred_grade_pct_from_points(assignment.points_earned, assignment.points_possible)
    if inferred is not None:
        return inferred
    raw = getattr(assignment, "grade_pct", None)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    return None


def compute_effective_course_grades(
    assignments: Sequence[CanonicalAssignment],
) -> dict[str, float]:
    assignments_by_course: dict[str, list[CanonicalAssignment]] = {}
    for assignment in assignments:
        assignments_by_course.setdefault(assignment.course_id, []).append(assignment)

    effective_grades: dict[str, float] = {}
    for course_id, course_assignments in assignments_by_course.items():
        course = next(
            (assignment.course for assignment in course_assignments if assignment.course is not None),
            None,
        )
        effective_grade = compute_effective_course_grade(course_assignments, course=course)
        if effective_grade is not None:
            effective_grades[course_id] = effective_grade
    return effective_grades


_MAX_GRADE_DETAIL_EXCLUDED = 40

# D2L/Brightspace "weight" on a grade row is usually share of the *category*, not % of final grade.
# Whenever the course has syllabus-derived grade categories with positive weights, those govern
# the final blend (not per-row LMS weights).


def _syllabus_category_weight_sum(course: Course | None) -> float:
    total = 0.0
    for category in iter_course_categories(course):
        w = category_weight_pct(category)
        if w is not None and w > 0:
            total += float(w)
    return total


def _course_has_syllabus_category_weights(course: Course | None) -> bool:
    return any(
        category_weight_pct(category) not in (None, 0) for category in iter_course_categories(course)
    )


def _reference_utc(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if now.tzinfo is None:
        return now.replace(tzinfo=UTC)
    return now.astimezone(UTC)


def effective_assignment_grade_pct_for_course(
    assignment: CanonicalAssignment,
    *,
    reference: datetime | None = None,
) -> float | None:
    """Percentage used in course-grade math; past-due missing work counts as 0%."""
    if not counts_toward_course_grade(assignment, reference=reference):
        return None
    base = assignment_grade_pct(assignment)
    if base is not None:
        return apply_late_policy_penalty(base, assignment=assignment)
    ref = _reference_utc(reference)
    due = normalize_datetime(assignment.due_date)
    if due is not None and due < ref:
        return apply_late_policy_penalty(0.0, assignment=assignment)
    return None


def apply_late_policy_penalty(base_grade_pct: float, *, assignment: CanonicalAssignment) -> float:
    grade = max(0.0, min(100.0, float(base_grade_pct)))
    policy = assignment.late_policy if isinstance(assignment.late_policy, dict) else None
    if policy is None and assignment.course and isinstance(assignment.course.syllabus_parsed, dict):
        candidate = assignment.course.syllabus_parsed.get("late_policy")
        if isinstance(candidate, dict):
            policy = candidate
    if policy is None:
        return grade
    days_late = int(getattr(assignment, "raw_scraped_data", {}).get("days_late", 0)) if isinstance(getattr(assignment, "raw_scraped_data", None), dict) else 0
    if days_late <= 0:
        return grade
    max_late_days_raw = policy.get("max_late_days")
    if isinstance(max_late_days_raw, (int, float)) and max_late_days_raw >= 0 and days_late > int(max_late_days_raw):
        return 0.0
    penalty_raw = policy.get("default_penalty_per_day")
    if not isinstance(penalty_raw, (int, float)):
        return grade
    penalty_pct = float(penalty_raw)
    if penalty_pct <= 0:
        return grade
    if penalty_pct <= 1.0:
        penalty_pct *= 100.0
    adjusted = grade - (days_late * penalty_pct)
    return max(0.0, min(100.0, round(adjusted, 2)))


def _course_grade_exclusion_reason(
    assignment: CanonicalAssignment,
    *,
    reference: datetime | None = None,
) -> str:
    if "practice test" in assignment.title.lower():
        return "Practice tests are excluded from this course grade estimate."
    if zero_grade_means_not_turned_in(
        grade_pct=assignment.grade_pct,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
    ):
        ref = _reference_utc(reference)
        due = normalize_datetime(assignment.due_date)
        if due is None:
            return "Zero score with no due date is excluded until a due time is known."
        return "Zero or missing work is excluded until the due date passes."
    return "Not counted toward the course grade (rules)."


def explain_effective_course_grade(
    assignments: Sequence[CanonicalAssignment],
    *,
    course: Course | None,
    now: datetime | None = None,
) -> dict[str, object]:
    _, detail = effective_course_grade_with_detail(assignments, course=course, now=now)
    return detail


def compute_effective_course_grade(
    assignments: Sequence[CanonicalAssignment],
    *,
    course: Course | None,
    now: datetime | None = None,
) -> float | None:
    final, _ = effective_course_grade_with_detail(assignments, course=course, now=now)
    return final


def effective_course_grade_with_detail(
    assignments: Sequence[CanonicalAssignment],
    *,
    course: Course | None,
    now: datetime | None = None,
) -> tuple[float | None, dict[str, object]]:
    ref = _reference_utc(now)
    components: list[dict[str, object]] = []
    excluded: list[dict[str, str]] = []

    for assignment in assignments:
        if not counts_toward_course_grade(assignment, reference=ref):
            excluded.append(
                {
                    "title": assignment.title,
                    "reason": _course_grade_exclusion_reason(assignment, reference=ref),
                }
            )

    use_syllabus_category_blend = _course_has_syllabus_category_weights(course)
    syllabus_sum = _syllabus_category_weight_sum(course)

    if use_syllabus_category_blend:
        weighted_points = 0.0
        weighted_total = 0.0
        category_points = 0.0
        category_total = 0.0
        matched_ids: set[str] = set()

        for category in iter_course_categories(course):
            category_key = category_name_key(category.get("name"))
            if not category_key:
                continue

            weight_pct = category_weight_pct(category)
            if weight_pct in (None, 0):
                continue

            members = [
                assignment
                for assignment in assignments
                if infer_assignment_category_key(assignment, course) == category_key
            ]
            for member in members:
                matched_ids.add(member.id)

            category_grade = compute_category_grade_pct(members, reference=ref)
            if category_grade is None:
                continue

            contrib = weight_pct * (category_grade / 100.0)
            category_points += contrib
            category_total += weight_pct

            count_members = [
                assignment
                for assignment in members
                if counts_toward_course_grade(assignment, reference=ref)
            ]
            blend_denom = 0.0
            for assignment in count_members:
                gp_m = effective_assignment_grade_pct_for_course(assignment, reference=ref)
                if gp_m is None:
                    continue
                w_m = assignment.points_possible if assignment.points_possible not in (None, 0) else 1.0
                blend_denom += float(w_m)

            member_rows: list[dict[str, object]] = []
            for assignment in count_members:
                gp_m = effective_assignment_grade_pct_for_course(assignment, reference=ref)
                w_m = assignment.points_possible if assignment.points_possible not in (None, 0) else 1.0
                w_m_f = float(w_m)
                row_m: dict[str, object] = {"title": assignment.title}
                if gp_m is not None:
                    row_m["grade_pct"] = float(gp_m)
                    if blend_denom > 0:
                        row_m["share_of_category_avg_pct"] = round((w_m_f / blend_denom) * 100.0, 2)
                else:
                    row_m["grade_pct"] = None
                row_m["category_weight_units"] = w_m_f
                if assignment.points_possible not in (None, 0):
                    row_m["points_possible"] = float(assignment.points_possible)
                if assignment.points_earned is not None:
                    row_m["points_earned"] = float(assignment.points_earned)
                if (
                    gp_m is not None
                    and assignment_grade_pct(assignment) is None
                    and gp_m == 0.0
                ):
                    row_m["note"] = "Counted as 0% (due date passed, no score recorded)."
                member_rows.append(row_m)

            components.append(
                {
                    "type": "category",
                    "name": str(category.get("name") or "Category"),
                    "weight_pct": float(weight_pct),
                    "grade_pct": float(category_grade),
                    "weighted_points": round(contrib, 6),
                    "members": member_rows,
                }
            )

        for assignment in assignments:
            if not counts_toward_course_grade(assignment, reference=ref):
                continue
            if assignment.id in matched_ids:
                continue
            if effective_assignment_grade_pct_for_course(assignment, reference=ref) is None:
                continue
            excluded.append(
                {
                    "title": assignment.title,
                    "reason": "Graded item does not match any syllabus category name for this course.",
                }
            )

        total_weight = weighted_total + category_total
        numerator = weighted_points + category_points
        excluded_truncated = max(0, len(excluded) - _MAX_GRADE_DETAIL_EXCLUDED)
        excluded_payload = excluded[:_MAX_GRADE_DETAIL_EXCLUDED]

        notes: list[str] = [
            "Final grade follows syllabus category weights (stored on the course), not each LMS row's weight field.",
            f"Syllabus categories on file total about {round(syllabus_sum, 2)}% toward the grade scheme used here.",
            "Inside each category, averages use points possible as weights when available.",
            "Zeros and missing scores count as 0% once the assignment due instant (stored UTC) is before the calculation time.",
        ]

        if total_weight > 0:
            final = round((numerator / total_weight) * 100, 2)
            notes.append(
                f"Totals: {round(numerator, 6)} weighted points / {round(total_weight, 6)}% total weight = {final}%."
            )
            return final, {
                "final_grade_pct": final,
                "total_weight_pct": round(total_weight, 4),
                "numerator_weighted_points": round(numerator, 6),
                "components": components,
                "excluded": excluded_payload,
                "excluded_count": len(excluded),
                "excluded_truncated": excluded_truncated,
                "notes": notes,
            }

        notes.append("No graded components with weights were available to compute a course grade.")
        return None, {
            "final_grade_pct": None,
            "total_weight_pct": round(total_weight, 4),
            "numerator_weighted_points": round(numerator, 6),
            "components": components,
            "excluded": excluded_payload,
            "excluded_count": len(excluded),
            "excluded_truncated": excluded_truncated,
            "notes": notes,
        }

    weighted_points = 0.0
    weighted_total = 0.0
    categories_with_explicit_weights: set[str] = set()

    for assignment in assignments:
        if not counts_toward_course_grade(assignment, reference=ref):
            continue
        grade_pct = effective_assignment_grade_pct_for_course(assignment, reference=ref)
        if grade_pct is None or assignment.grade_weight_pct in (None, 0):
            if grade_pct is None and assignment.grade_weight_pct not in (None, 0):
                reason = "No scored grade yet (points or percentage missing)."
            elif grade_pct is not None and assignment.grade_weight_pct in (None, 0):
                reason = "Has a grade but no weight in the course breakdown."
            else:
                reason = "No scored grade and no course weight on this row."
            excluded.append({"title": assignment.title, "reason": reason})
            continue
        w = assignment.grade_weight_pct
        contrib = w * (grade_pct / 100.0)
        weighted_points += contrib
        weighted_total += w
        row: dict[str, object] = {
            "type": "assignment",
            "title": assignment.title,
            "weight_pct": float(w),
            "grade_pct": float(grade_pct),
            "weighted_points": round(contrib, 6),
        }
        if assignment_grade_pct(assignment) is None and grade_pct == 0.0:
            row["note"] = "Counted as 0% (due date passed, no score recorded)."
        components.append(row)
        category_key = infer_assignment_category_key(assignment, course)
        if category_key is not None:
            categories_with_explicit_weights.add(category_key)

    category_points = 0.0
    category_total = 0.0
    for category in iter_course_categories(course):
        category_key = category_name_key(category.get("name"))
        if not category_key or category_key in categories_with_explicit_weights:
            continue

        weight_pct = category_weight_pct(category)
        if weight_pct in (None, 0):
            continue

        category_grade = compute_category_grade_pct(
            [
                assignment
                for assignment in assignments
                if infer_assignment_category_key(assignment, course) == category_key
            ],
            reference=ref,
        )
        if category_grade is None:
            continue

        contrib = weight_pct * (category_grade / 100.0)
        category_points += contrib
        category_total += weight_pct
        components.append(
            {
                "type": "category",
                "name": str(category.get("name") or "Category"),
                "weight_pct": float(weight_pct),
                "grade_pct": float(category_grade),
                "weighted_points": round(contrib, 6),
            }
        )

    total_weight = weighted_total + category_total
    numerator = weighted_points + category_points
    excluded_truncated = max(0, len(excluded) - _MAX_GRADE_DETAIL_EXCLUDED)
    excluded_payload = excluded[:_MAX_GRADE_DETAIL_EXCLUDED]

    notes = [
        "Each row adds weighted points = (weight % x grade %) / 100. "
        "The course grade is sum(weighted points) / sum(weights) x 100.",
        "When no syllabus category weights are stored on the course, LMS row weights are treated as percent of the final grade.",
        "Zeros and missing scores count as 0% once the assignment due instant (stored UTC) is before the calculation time.",
    ]

    if total_weight > 0:
        final = round((numerator / total_weight) * 100, 2)
        notes.append(
            f"Totals: {round(numerator, 6)} weighted points / {round(total_weight, 6)}% total weight = {final}%."
        )
        return final, {
            "final_grade_pct": final,
            "total_weight_pct": round(total_weight, 4),
            "numerator_weighted_points": round(numerator, 6),
            "components": components,
            "excluded": excluded_payload,
            "excluded_count": len(excluded),
            "excluded_truncated": excluded_truncated,
            "notes": notes,
        }

    notes.append("No graded components with weights were available to compute a course grade.")
    return None, {
        "final_grade_pct": None,
        "total_weight_pct": round(total_weight, 4),
        "numerator_weighted_points": round(numerator, 6),
        "components": components,
        "excluded": excluded_payload,
        "excluded_count": len(excluded),
        "excluded_truncated": excluded_truncated,
        "notes": notes,
    }


def compute_category_grade_pct(
    assignments: Sequence[CanonicalAssignment],
    *,
    reference: datetime | None = None,
) -> float | None:
    ref = _reference_utc(reference)
    weighted_sum = 0.0
    weight_total = 0.0

    for assignment in assignments:
        if not counts_toward_course_grade(assignment, reference=ref):
            continue
        grade_pct = effective_assignment_grade_pct_for_course(assignment, reference=ref)
        if grade_pct is None:
            continue
        weight = assignment.points_possible if assignment.points_possible not in (None, 0) else 1.0
        weighted_sum += grade_pct * weight
        weight_total += weight

    if weight_total == 0:
        return None
    return round(weighted_sum / weight_total, 2)


def iter_course_categories(course: Course | None) -> list[dict]:
    if course is None or not isinstance(course.grade_categories, list):
        return []
    return [category for category in course.grade_categories if isinstance(category, dict)]


def infer_assignment_category_key(
    assignment: CanonicalAssignment,
    course: Course | None,
) -> str | None:
    if assignment.grade_category:
        return category_name_key(assignment.grade_category)

    categories = iter_course_categories(course)
    if not categories:
        return None

    title = assignment.title.lower()
    type_aliases = assignment_category_aliases(assignment)
    for category in categories:
        category_name = category.get("name")
        category_key = category_name_key(category_name)
        if not category_name or not category_key:
            continue
        lowered_name = str(category_name).lower()
        if any(alias in lowered_name or lowered_name in alias for alias in type_aliases):
            return category_key
    return None


def assignment_category_aliases(assignment: CanonicalAssignment) -> set[str]:
    aliases = {assignment.type.lower()}
    title = assignment.title.lower()
    if assignment.type == "homework" or title.startswith("hw "):
        aliases.update({"homework", "hw", "exercise", "assignment", "problem"})
    if assignment.type == "exam":
        aliases.update({"exam", "quiz", "test"})
    if assignment.type == "lab":
        aliases.add("lab")
    if assignment.type == "discussion":
        aliases.add("discussion")
    if assignment.type == "project":
        aliases.add("project")
    if assignment.type == "reading":
        aliases.add("reading")
    return aliases


def category_name_key(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or None


def category_weight_pct(category: dict) -> float | None:
    weight = category.get("weight")
    if not isinstance(weight, (int, float)):
        return None
    if weight <= 0:
        return None
    return round(weight * 100, 4) if weight <= 1.5 else float(weight)


def counts_toward_course_grade(
    assignment: CanonicalAssignment,
    *,
    reference: datetime | None = None,
) -> bool:
    if "practice test" in assignment.title.lower():
        return False
    ref = _reference_utc(reference)
    due = normalize_datetime(assignment.due_date)
    past_due = due is not None and due < ref
    if zero_grade_means_not_turned_in(
        grade_pct=assignment.grade_pct,
        points_earned=assignment.points_earned,
        points_possible=assignment.points_possible,
    ):
        return past_due
    return True
