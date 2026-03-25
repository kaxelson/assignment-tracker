from datetime import UTC, datetime
import hashlib
import re
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

from acc.scrapers.snapshots import (
    D2LAnnouncement,
    D2LAnnouncementItem,
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
NEWS_ID_RE = re.compile(r"/news/\d+/(?P<news_id>\d+)/view")
QUICKLINK_CODE_RE = re.compile(r"[?&]rcode=(?P<rcode>[^&#]+)")

# Combined syllabus text for AI parse; individual D2L extractions are capped much lower.
_MAX_COMBINED_SYLLABUS_CHARS = 96_000

# Pull in outline pages whose titles look like policies/grading (many courses split these out of SYLLABUS).
_CONTENT_OUTLINE_SYLLABUS_TITLE_HINT = re.compile(
    r"syllabus|grading|grade\s*(?:breakdown|distribution|components|scheme|weights?)|"
    r"course\s*(?:policy|policies|expectations)|assessment|evaluation|late\s*work|"
    r"attendance|accommodat|weights?\s*(?:and|&|\+)\s*grades?",
    re.IGNORECASE,
)


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


def compose_syllabus_raw_text(course: D2LCourseSnapshot) -> str | None:
    """Merge all syllabus-topic extracts plus grading-like content-outline pages.

    D2L courses often split the syllabus across several Content items; the old behavior
    kept only the first extract and dropped the rest.
    """
    parts: list[str] = []
    seen_digests: set[str] = set()

    def push_section(heading: str | None, text: str | None) -> None:
        if not text or not str(text).strip():
            return
        body = str(text).strip()
        digest = hashlib.sha256(body.encode("utf-8", errors="replace")).hexdigest()
        if digest in seen_digests:
            return
        seen_digests.add(digest)
        if heading:
            parts.append(f"{heading}\n{body}")
        else:
            parts.append(body)

    for topic in course.syllabus_topics:
        label = f"[{topic.title}]" if topic.title else None
        push_section(label, topic.extracted_text)

    for topic in course.content_outline_topics:
        haystack = f"{topic.title or ''} {topic.module_title or ''}"
        if not _CONTENT_OUTLINE_SYLLABUS_TITLE_HINT.search(haystack):
            continue
        label = f"[{topic.module_title or 'Content'}: {topic.title}]"
        push_section(label, topic.extracted_text)

    if not parts:
        return None
    combined = "\n\n---\n\n".join(parts)
    if len(combined) > _MAX_COMBINED_SYLLABUS_CHARS:
        combined = (
            combined[:_MAX_COMBINED_SYLLABUS_CHARS].rstrip()
            + "\n\n[... syllabus text truncated for parsing ...]"
        )
    return combined


def normalize_course(course: D2LCourseSnapshot) -> NormalizedCourse:
    external_platform_urls = {
        tool.name.lower().replace(" / ", "_").replace(" ", "_"): tool.url
        for tool in course.tool_links
    }
    for topic in course.external_tools:
        external_platform_urls[slugify(topic.title)] = topic.launch_url or topic.url

    syllabus_raw_text = compose_syllabus_raw_text(course)
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
    category_titles_with_children = {
        row.category_title
        for row in course.grade_rows
        if not row.is_category and row.category_title
    }

    for grade_row in course.grade_rows:
        if not should_normalize_grade_row(grade_row, category_titles_with_children):
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

    for announcement in course.announcements:
        for item in announcement.items:
            announcement_assignment = normalize_announcement_item(
                course_id=course_id,
                announcement=announcement,
                item=item,
            )
            key = assignment_match_key(announcement_assignment.title)
            existing = assignments_by_key.get(key)
            if existing is None:
                assignments_by_key[key] = announcement_assignment
                continue

            existing.raw_source.update(announcement_assignment.raw_source)

    for topic in course.content_outline_topics:
        if not topic.extracted_text:
            continue
        for title, due_text in extract_assignments_from_content_outline(topic.extracted_text):
            outline_assignment = normalize_content_outline_assignment(
                course_id=course_id,
                topic=topic,
                title=title,
                due_text=due_text,
                fetched_at=fetched_at,
                timezone=timezone,
            )
            key = assignment_match_key(outline_assignment.title)
            existing = assignments_by_key.get(key)
            if existing is None:
                assignments_by_key[key] = outline_assignment
                continue

            existing.due_at = existing.due_at or outline_assignment.due_at
            existing.due_text = existing.due_text or outline_assignment.due_text
            if existing.status == "available" and outline_assignment.status == "upcoming":
                existing.status = "upcoming"
            existing.raw_source.update(outline_assignment.raw_source)

    return list(assignments_by_key.values())


def should_normalize_grade_row(
    grade_row: D2LGradeRow,
    category_titles_with_children: set[str],
) -> bool:
    if not grade_row.is_category:
        return True
    if grade_row.title in category_titles_with_children:
        return False
    return bool(grade_row.grade_text or grade_row.weight_achieved_text or grade_row.points_text)


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


def normalize_announcement_item(
    course_id: str,
    *,
    announcement: D2LAnnouncement,
    item: D2LAnnouncementItem,
) -> NormalizedAssignment:
    return NormalizedAssignment(
        id=build_assignment_id(
            course_id,
            item.title,
            source_identifier=item.url or f"{announcement.url}#{item.title}",
        ),
        course_id=course_id,
        title=item.title,
        type=infer_assignment_type(item.title),
        status="available",
        external_url=item.url,
        due_at=None,
        due_text=None,
        raw_source={
            "announcement_title": announcement.title,
            "announcement_url": announcement.url,
            "announcement_posted_at_text": announcement.posted_at_text,
            "announcement_item_url": item.url,
        },
    )


def normalize_content_outline_assignment(
    course_id: str,
    *,
    topic: D2LContentTopic,
    title: str,
    due_text: str,
    fetched_at: datetime,
    timezone: str,
) -> NormalizedAssignment:
    due_at = parse_flexible_due_at(due_text, fetched_at, timezone=timezone)
    return NormalizedAssignment(
        id=build_assignment_id(course_id, title, source_identifier=topic.url),
        course_id=course_id,
        title=title,
        type=infer_assignment_type(title),
        status="upcoming" if due_at else infer_status(title),
        external_url=None,
        due_at=due_at,
        due_text=due_text,
        raw_source={
            "content_outline_topic": topic.title,
            "content_outline_module": topic.module_title,
            "content_outline_url": topic.url,
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

    match = QUICKLINK_CODE_RE.search(source_identifier)
    if match is not None:
        return match.group("rcode")

    match = NEWS_ID_RE.search(source_identifier)
    if match is not None:
        return match.group("news_id")

    return None


def infer_assignment_type(title: str) -> str:
    lowered = title.lower()
    if "quiz" in lowered or "exam" in lowered or "test" in lowered:
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


# After "6.7", MindTap often glues status text ("6.7Not started") where \b would
# match only after the major digit ("6"), collapsing 6.7 to 6. Require the
# numeric token to not continue with another digit; allow letters/punctuation next.
PROGRAMMING_EXERCISE_KEY_RE = re.compile(
    r"programming\s+exercise\s+(\d+(?:\.\d+)?)(?![0-9])",
    re.IGNORECASE,
)


def programming_exercise_match_key(title: str) -> str | None:
    match = PROGRAMMING_EXERCISE_KEY_RE.search(title)
    if match is None:
        return None
    return f"programming-exercise-{match.group(1).replace('.', '-')}"


def assignment_match_key(title: str) -> str:
    pe_key = programming_exercise_match_key(title)
    if pe_key is not None:
        return pe_key
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


CONTENT_OUTLINE_LONG_DUE_RE = re.compile(
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+"
    r"\d{1,2},?\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:AM|PM)",
    re.IGNORECASE,
)

CONTENT_OUTLINE_SHORT_DUE_RE = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2})\s+(\d{1,2}:\d{2}\s*(?:AM|PM))\b",
)

_CONTENT_OUTLINE_TITLE_NOISE = frozenset(
    {
        "objectives",
        "reading",
        "graded tasks",
        "grades tasks",
        "grade tasks",
        "extra credit",
        "table of contents",
    }
)


def parse_flexible_due_at(
    value: str | None,
    reference: datetime,
    timezone: str = "America/Chicago",
) -> datetime | None:
    if not value:
        return None
    compact = " ".join(value.split())
    parsed = parse_due_text(compact, reference, timezone=timezone)
    if parsed is not None:
        return parsed
    local_timezone = ZoneInfo(timezone)
    for fmt in (
        "%b %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M %p",
        "%b %d %Y %I:%M %p",
    ):
        try:
            dt = datetime.strptime(compact, fmt).replace(tzinfo=local_timezone)
            return dt.astimezone(UTC)
        except ValueError:
            continue
    return None


def _content_outline_line_looks_like_assignment(title: str) -> bool:
    tl = title.lower().strip(" :-|")
    if len(tl) < 4:
        return False
    if tl in _CONTENT_OUTLINE_TITLE_NOISE:
        return False
    if tl.startswith("graded tasks") or tl.startswith("grades tasks"):
        return False
    keywords = (
        "programming",
        "exercise",
        "quiz",
        "exam",
        "lab",
        "homework",
        "assignment",
        "extra credit",
        "project",
        "chapter",
        "discussion",
        "activity",
        "mindtap",
        "pearson",
        "dropbox",
    )
    if any(k in tl for k in keywords):
        return True
    if re.search(r"\d+\.\d+", title):
        return True
    return False


def extract_assignments_from_content_outline(text: str) -> list[tuple[str, str]]:
    results: list[tuple[str, str]] = []
    for raw_line in text.replace("\t", " ").splitlines():
        line = " ".join(raw_line.split())
        if not line or len(line) < 8:
            continue
        m_long = CONTENT_OUTLINE_LONG_DUE_RE.search(line)
        if m_long:
            due_fragment = m_long.group(0)
            title = line[: m_long.start()].strip()
        else:
            m_short = CONTENT_OUTLINE_SHORT_DUE_RE.search(line)
            if not m_short:
                continue
            due_fragment = f"{m_short.group(1).upper()} {m_short.group(2)} {m_short.group(3)}"
            title = line[: m_short.start()].strip()

        title = re.sub(r"\s+[Dd]ue\s*$", "", title).strip(" :-|")
        if not _content_outline_line_looks_like_assignment(title):
            continue
        results.append((title, due_fragment))
    return results


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
