from dataclasses import dataclass
from datetime import UTC, datetime
from collections.abc import Sequence
from collections import Counter
import re

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from acc.db.models import AgendaEntry, Assignment, Course
from acc.engine.normalizer import NormalizedAssignment, NormalizedCourse, NormalizedSnapshot
from acc.scrapers.snapshots import ExternalAssignmentSnapshot, ExternalScrapeSnapshot


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
    grade_pct: float | None
    points_possible: float | None
    points_earned: float | None
    status: str
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

    async def list_course_overview(self) -> Sequence[CourseOverview]:
        canonical_assignments = await self.list_canonical_assignments()
        assignment_counts = Counter(assignment.course_id for assignment in canonical_assignments)
        result = await self.session.execute(select(Course).order_by(Course.code.asc(), Course.name.asc()))
        return [
            CourseOverview(course=course, assignment_count=assignment_counts.get(course.id, 0))
            for course in result.scalars().all()
        ]

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
        return reconcile_assignments(result.all())

    async def replace_agenda_entries(
        self,
        entries: Sequence[AgendaEntry],
        assignment_ids: Sequence[str],
    ) -> int:
        if assignment_ids:
            await self.session.execute(
                delete(AgendaEntry).where(AgendaEntry.assignment_id.in_(assignment_ids))
            )

        self.session.add_all(entries)
        await self.session.flush()
        return len(entries)

    async def list_agenda_entries(
        self,
        *,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> Sequence[AgendaEntry]:
        statement = (
            select(AgendaEntry)
            .options(
                selectinload(AgendaEntry.assignment).selectinload(Assignment.course),
            )
            .order_by(AgendaEntry.agenda_date.asc(), AgendaEntry.priority_score.desc(), AgendaEntry.id.asc())
        )
        if date_from is not None:
            statement = statement.where(AgendaEntry.agenda_date >= date_from.date())
        if date_to is not None:
            statement = statement.where(AgendaEntry.agenda_date <= date_to.date())

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
    return Assignment(
        id=assignment.id,
        course_id=assignment.course_id,
        title=assignment.title,
        description=None,
        type=assignment.type,
        source_platform=assignment.source_platform,
        external_url=assignment.external_url,
        available_date=None,
        due_date=assignment.due_at.astimezone(UTC) if assignment.due_at else None,
        close_date=None,
        grade_category=assignment.grade_category,
        grade_weight_pct=assignment.weight_possible,
        points_possible=assignment.points_possible,
        points_earned=assignment.points_earned,
        grade_pct=assignment.grade_pct,
        status=assignment.status,
        is_submitted=False,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=assignment.raw_source,
        last_scraped=scraped_at.astimezone(UTC),
    )


def external_assignment_to_model(assignment: ExternalAssignmentSnapshot, scraped_at) -> Assignment:
    submitted = assignment.status in {"completed", "submitted"}
    return Assignment(
        id=assignment.id,
        course_id=assignment.course_id,
        title=assignment.title,
        description=assignment.description,
        type=assignment.type,
        source_platform=assignment.source_platform,
        external_url=assignment.external_url,
        available_date=None,
        due_date=assignment.due_at.astimezone(UTC) if assignment.due_at else None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=assignment.points_possible,
        points_earned=assignment.points_earned,
        grade_pct=assignment.grade_pct,
        status=assignment.status,
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
        key = (assignment.course_id, assignment_match_key(assignment.title))
        groups.setdefault(key, []).append(assignment)

    canonical: list[CanonicalAssignment] = []
    for key in sorted(groups):
        canonical.append(merge_assignment_group(groups[key]))
    return canonical


def merge_assignment_group(assignments: Sequence[Assignment]) -> CanonicalAssignment:
    preferred = max(assignments, key=assignment_preference_score)
    merged_raw_data = merge_raw_data(assignments)
    due_date = preferred.due_date or next(
        (assignment.due_date for assignment in assignments if assignment.due_date is not None),
        None,
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
        due_date=normalize_datetime(due_date),
        grade_pct=preferred.grade_pct,
        points_possible=preferred.points_possible,
        points_earned=preferred.points_earned,
        status=preferred_status(assignments),
        estimated_minutes=preferred.estimated_minutes
        or next(
            (assignment.estimated_minutes for assignment in assignments if assignment.estimated_minutes),
            None,
        ),
        raw_scraped_data=merged_raw_data,
        course=preferred.course,
    )


def assignment_preference_score(assignment: Assignment) -> tuple[int, int, int, int]:
    return (
        1 if assignment.source_platform != "d2l" else 0,
        1 if assignment.grade_pct is not None or assignment.points_earned is not None else 0,
        1 if assignment.estimated_minutes is not None else 0,
        1 if assignment.due_date is not None else 0,
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


def assignment_match_key(title: str) -> str:
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
