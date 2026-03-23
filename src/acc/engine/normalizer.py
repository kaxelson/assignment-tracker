from datetime import UTC, datetime
import re
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

from acc.scrapers.snapshots import (
    D2LContentTopic,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
    D2LGradeRow,
    D2LUpcomingEvent,
)

MONTH_TO_NUMBER = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

SLUG_RE = re.compile(r"[^a-z0-9]+")
EVENT_ID_RE = re.compile(r"/event/(?P<event_id>\d+)")


class NormalizedCourse(BaseModel):
    id: str
    d2l_course_id: str
    code: str
    name: str
    semester: str | None = None
    d2l_url: str
    source_platform: str = "d2l"
    current_grade_pct: float | None = None
    current_grade_text: str | None = None
    syllabus_raw_text: str | None = None
    external_platform: str | None = None
    external_platform_url: str | None = None
    external_platform_urls: dict[str, str] = Field(default_factory=dict)


class NormalizedAssignment(BaseModel):
    id: str
    course_id: str
    title: str
    type: str
    status: str
    source_platform: str = "d2l"
    external_url: str | None = None
    grade_category: str | None = None
    points_earned: float | None = None
    points_possible: float | None = None
    weight_achieved: float | None = None
    weight_possible: float | None = None
    grade_pct: float | None = None
    due_at: datetime | None = None
    due_text: str | None = None
    raw_source: dict[str, str | None] = Field(default_factory=dict)


class NormalizedSnapshot(BaseModel):
    normalized_at: datetime
    source_fetched_at: datetime
    courses: list[NormalizedCourse]
    assignments: list[NormalizedAssignment]


def normalize_d2l_snapshot(
    snapshot: D2LDashboardSnapshot,
    timezone: str = "America/Chicago",
) -> NormalizedSnapshot:
    courses = [normalize_course(course) for course in snapshot.courses]
    assignments: list[NormalizedAssignment] = []

    for course in snapshot.courses:
        normalized_course_id = build_course_id(course)
        assignments.extend(
            normalize_course_assignments(
                course=course,
                course_id=normalized_course_id,
                fetched_at=snapshot.fetched_at,
                timezone=timezone,
            )
        )

    return NormalizedSnapshot(
        normalized_at=datetime.now(UTC),
        source_fetched_at=snapshot.fetched_at,
        courses=courses,
        assignments=assignments,
    )


def normalize_course(course: D2LCourseSnapshot) -> NormalizedCourse:
    external_platform_urls = {
        tool.name.lower().replace(" / ", "_").replace(" ", "_"): tool.url
        for tool in course.tool_links
    }
    for topic in course.external_tools:
        external_platform_urls[slugify(topic.title)] = topic.launch_url or topic.url

    syllabus_raw_text = next(
        (topic.extracted_text for topic in course.syllabus_topics if topic.extracted_text),
        None,
    )
    primary_external_tool = choose_primary_external_tool(course.external_tools)
    return NormalizedCourse(
        id=build_course_id(course),
        d2l_course_id=course.course_id,
        code=course.code,
        name=course.name,
        semester=course.semester,
        d2l_url=course.home_url,
        current_grade_pct=parse_percent(course.final_calculated_grade.grade_text)
        if course.final_calculated_grade
        else None,
        current_grade_text=course.final_calculated_grade.grade_text if course.final_calculated_grade else None,
        syllabus_raw_text=syllabus_raw_text,
        external_platform=detect_external_platform_topic(primary_external_tool)
        if primary_external_tool
        else None,
        external_platform_url=(primary_external_tool.launch_url or primary_external_tool.url)
        if primary_external_tool
        else None,
        external_platform_urls=external_platform_urls,
    )


def normalize_course_assignments(
    course: D2LCourseSnapshot,
    course_id: str,
    fetched_at: datetime,
    timezone: str,
) -> list[NormalizedAssignment]:
    assignments_by_key: dict[str, NormalizedAssignment] = {}

    for grade_row in course.grade_rows:
        if grade_row.is_category:
            continue
        assignment = normalize_grade_row(course_id=course_id, grade_row=grade_row)
        assignments_by_key[assignment_match_key(assignment.title)] = assignment

    for event in course.upcoming_events:
        event_assignment = normalize_event(
            course_id=course_id,
            fetched_at=fetched_at,
            event=event,
            timezone=timezone,
        )
        key = assignment_match_key(event_assignment.title)
        existing = assignments_by_key.get(key)
        if existing is None:
            assignments_by_key[key] = event_assignment
            continue

        existing.id = event_assignment.id
        existing.external_url = existing.external_url or event_assignment.external_url
        existing.due_at = existing.due_at or event_assignment.due_at
        existing.due_text = existing.due_text or event_assignment.due_text
        if existing.status == "available" and event_assignment.status == "upcoming":
            existing.status = "upcoming"
        existing.raw_source.update(
            {
                "event_details_url": event.details_url,
                "event_due_text": event.due_text,
            }
        )

    return list(assignments_by_key.values())


def normalize_event(
    course_id: str,
    fetched_at: datetime,
    event: D2LUpcomingEvent,
    timezone: str,
) -> NormalizedAssignment:
    due_at = parse_due_text(event.due_text, fetched_at, timezone=timezone)
    return NormalizedAssignment(
        id=build_assignment_id(course_id, event.title, source_identifier=event.details_url),
        course_id=course_id,
        title=event.title,
        type=infer_assignment_type(event.title),
        status=infer_status(event.title),
        external_url=event.details_url,
        due_at=due_at,
        due_text=event.due_text,
        raw_source={
            "details_url": event.details_url,
            "due_text": event.due_text,
        },
    )


def normalize_grade_row(course_id: str, grade_row: D2LGradeRow) -> NormalizedAssignment:
    points_earned, points_possible = parse_fraction(grade_row.points_text)
    weight_earned, weight_possible = parse_fraction(grade_row.weight_achieved_text)
    return NormalizedAssignment(
        id=build_assignment_id(course_id, grade_row.title),
        course_id=course_id,
        title=grade_row.title,
        type=infer_assignment_type(grade_row.title),
        status=infer_status(grade_row.title),
        grade_category=grade_row.category_title,
        points_earned=points_earned,
        points_possible=points_possible,
        weight_achieved=weight_earned,
        weight_possible=weight_possible,
        grade_pct=parse_percent(grade_row.grade_text),
        due_at=None,
        due_text=None,
        raw_source={
            "grade_category": grade_row.category_title,
            "points_text": grade_row.points_text,
            "weight_achieved_text": grade_row.weight_achieved_text,
            "grade_text": grade_row.grade_text,
        },
    )


def build_course_id(course: D2LCourseSnapshot) -> str:
    semester = slugify(course.semester or "unknown-term")
    code = slugify(course.code)
    return f"{code}-{semester}"


def build_assignment_id(
    course_id: str,
    title: str,
    source_identifier: str | None = None,
) -> str:
    stable_identifier = extract_assignment_source_id(source_identifier) or assignment_match_key(title)
    return f"{course_id}-{stable_identifier}"


def extract_assignment_source_id(source_identifier: str | None) -> str | None:
    if not source_identifier:
        return None

    match = EVENT_ID_RE.search(source_identifier)
    if match is not None:
        return match.group("event_id")

    return None


def infer_assignment_type(title: str) -> str:
    lowered = title.lower()
    if "quiz" in lowered or "exam" in lowered:
        return "exam"
    if "lab" in lowered:
        return "lab"
    if "discussion" in lowered:
        return "discussion"
    if "project" in lowered:
        return "project"
    if "reading" in lowered:
        return "reading"
    return "homework"


def infer_status(title: str) -> str:
    lowered = title.lower()
    if "in class" in lowered:
        return "available"
    if "due" in lowered:
        return "upcoming"
    return "available"


def assignment_match_key(title: str) -> str:
    normalized = title.lower().strip()
    for suffix in (" - due", " due", " in class"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return slugify(normalized)


def parse_due_text(
    value: str | None,
    reference: datetime,
    timezone: str = "America/Chicago",
) -> datetime | None:
    if not value:
        return None

    parts = value.split()
    if len(parts) < 2:
        return None

    month = MONTH_TO_NUMBER.get(parts[0].upper())
    if month is None:
        return None

    try:
        day = int(parts[1])
    except ValueError:
        return None

    hour = 23
    minute = 59
    if len(parts) >= 4:
        time_text = f"{parts[2]} {parts[3]}"
        parsed_time = datetime.strptime(time_text, "%I:%M %p")
        hour = parsed_time.hour
        minute = parsed_time.minute

    local_timezone = ZoneInfo(timezone)
    reference_local = reference.astimezone(local_timezone)
    local_due_at = datetime(reference_local.year, month, day, hour, minute, tzinfo=local_timezone)
    return local_due_at.astimezone(UTC)


def parse_fraction(value: str | None) -> tuple[float | None, float | None]:
    if not value or "/" not in value:
        return None, None

    left, right = [part.strip() for part in value.split("/", 1)]
    try:
        return float(left), float(right)
    except ValueError:
        return None, None


def parse_percent(value: str | None) -> float | None:
    if not value:
        return None
    cleaned = value.replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def choose_primary_external_tool(topics: list[D2LContentTopic]) -> D2LContentTopic | None:
    if not topics:
        return None
    return max(topics, key=score_external_tool)


def score_external_tool(topic: D2LContentTopic) -> tuple[int, int]:
    text = f"{topic.module_title or ''} {topic.title} {topic.launch_url or ''} {topic.url}".lower()
    score = 0
    if "assignment" in text:
        score += 4
    if "mindtap" in text or "mastering" in text:
        score += 3
    if "pearson" in text or "cengage" in text:
        score += 2
    if "dashboard" in text or "support" in text:
        score -= 2
    if "etext" in text:
        score -= 1
    return score, -len(topic.title)


def detect_external_platform_topic(topic: D2LContentTopic) -> str | None:
    haystack = " ".join(
        value
        for value in (topic.module_title, topic.title, topic.launch_url, topic.url)
        if value
    ).lower()
    if any(token in haystack for token in ("cengage", "mindtap")):
        return "cengage_mindtap"
    if any(token in haystack for token in ("pearson", "mastering", "mylab")):
        return "pearson_mylab"
    return None


def slugify(value: str) -> str:
    lowered = value.lower().strip()
    slug = SLUG_RE.sub("-", lowered).strip("-")
    return slug or "item"
