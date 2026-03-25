import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
import json

from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from acc.ai.client import JsonModelClient, OpenAIChatClient, extract_json_text
from acc.ai.prompts import SYLLABUS_PARSE_PROMPT
from acc.config import Settings
from acc.db.models import Course
from acc.db.repository import Repository


class GradeCategory(BaseModel):
    name: str
    weight: float | None = None
    description: str | None = None
    drop_lowest: int | None = None
    total_count: int | None = None


class LatePolicy(BaseModel):
    default_penalty_per_day: float | None = None
    max_late_days: int | None = None
    accepts_late: bool | None = None
    exceptions: str | None = None
    raw_text: str | None = None


class ExamItem(BaseModel):
    name: str
    date: str | None = None
    weight_pct: float | None = None
    topics: str | None = None
    location: str | None = None


class ImportantDate(BaseModel):
    date: str | None = None
    event: str


class ExternalTool(BaseModel):
    name: str
    purpose: str | None = None
    textbook: str | None = None


class SyllabusParseResult(BaseModel):
    course_name: str | None = None
    course_code: str | None = None
    instructor: str | None = None
    semester: str | None = None
    grade_categories: list[GradeCategory] = Field(default_factory=list)
    grading_scale: dict[str, list[float]] = Field(default_factory=dict)
    late_policy: LatePolicy | None = None
    exams: list[ExamItem] = Field(default_factory=list)
    important_dates: list[ImportantDate] = Field(default_factory=list)
    external_tools: list[ExternalTool] = Field(default_factory=list)
    office_hours: str | None = None
    attendance_policy: str | None = None
    extra_credit: str | None = None


@dataclass(slots=True)
class ParsedSyllabus:
    parsed: SyllabusParseResult
    review_flags: list[str]


@dataclass(slots=True)
class SyllabusParseSummary:
    courses_parsed: int = 0
    courses_skipped: int = 0
    courses_failed: int = 0


class SyllabusParser:
    def __init__(
        self,
        settings: Settings,
        client: JsonModelClient | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or OpenAIChatClient(settings, context="syllabus parse request")

    async def parse(self, syllabus_text: str) -> ParsedSyllabus:
        prompt = build_syllabus_prompt(syllabus_text)
        raw_response = await self.client.complete_json(prompt)
        response_payload = json.loads(extract_json_text(raw_response))
        parsed = SyllabusParseResult.model_validate(response_payload)
        review_flags = build_review_flags(parsed)
        return ParsedSyllabus(parsed=parsed, review_flags=review_flags)


SYLLABUS_PARSE_PROVENANCE_STAGE = "llm_syllabus_parse"
SYLLABUS_PARSE_ERROR_STAGE = "llm_syllabus_parse_error"


async def parse_saved_syllabi(
    session: AsyncSession,
    settings: Settings,
    *,
    force: bool = False,
    course_id: str | None = None,
    client: JsonModelClient | None = None,
) -> SyllabusParseSummary:
    repository = Repository(session)
    parser = SyllabusParser(settings, client=client)
    courses = await repository.list_courses_for_syllabus_parse(force=force, course_id=course_id)
    summary = SyllabusParseSummary()
    parse_targets = [(course, course.syllabus_raw_text) for course in courses if course.syllabus_raw_text]
    summary.courses_skipped += len(courses) - len(parse_targets)
    if not parse_targets:
        return summary

    sem = asyncio.Semaphore(max(1, min(len(parse_targets), settings.openai_max_concurrent_requests)))

    async def parse_one(course: Course, raw: str) -> tuple[Course, str, ParsedSyllabus | None, Exception | None]:
        async with sem:
            try:
                parsed = await parser.parse(raw)
                return course, raw, parsed, None
            except Exception as error:
                return course, raw, None, error

    parsed_rows = await asyncio.gather(*[parse_one(course, raw) for course, raw in parse_targets])
    for course, raw, parsed_syllabus, error in parsed_rows:
        if error is not None:
            summary.courses_failed += 1
            await repository.record_provenance_event(
                stage=SYLLABUS_PARSE_ERROR_STAGE,
                course_id=course.id,
                text_preview=raw[:8000] if raw else None,
                detail={
                    "ok": False,
                    "error_type": type(error).__name__,
                    "error": str(error)[:800],
                    "openai_model": settings.openai_model,
                },
            )
            continue

        assert parsed_syllabus is not None
        apply_syllabus_parse(course, parsed_syllabus, parsed_at=datetime.now(UTC))
        summary.courses_parsed += 1
        parsed = parsed_syllabus.parsed
        await repository.record_provenance_event(
            stage=SYLLABUS_PARSE_PROVENANCE_STAGE,
            course_id=course.id,
            text_preview=raw[:8000] if raw else None,
            detail={
                "ok": True,
                "openai_model": settings.openai_model,
                "syllabus_chars": len(raw),
                "grade_category_count": len(parsed.grade_categories),
                "grading_scale_letter_count": len(parsed.grading_scale),
                "review_flags": list(parsed_syllabus.review_flags),
                "has_late_policy_raw": bool(parsed.late_policy and parsed.late_policy.raw_text),
            },
        )

    return summary


def apply_syllabus_parse(
    course: Course,
    parsed_syllabus: ParsedSyllabus,
    *,
    parsed_at: datetime,
) -> None:
    payload = parsed_syllabus.parsed.model_dump(mode="json")
    payload["review_flags"] = parsed_syllabus.review_flags

    course.syllabus_parsed = payload
    course.grading_scale = parsed_syllabus.parsed.grading_scale or None
    course.grade_categories = [
        category.model_dump(mode="json") for category in parsed_syllabus.parsed.grade_categories
    ] or None
    course.late_policy_global = (
        parsed_syllabus.parsed.late_policy.raw_text
        if parsed_syllabus.parsed.late_policy and parsed_syllabus.parsed.late_policy.raw_text
        else None
    )
    if parsed_syllabus.parsed.instructor:
        course.instructor = parsed_syllabus.parsed.instructor
    if not course.textbook:
        course.textbook = first_textbook(parsed_syllabus.parsed.external_tools)
    course.last_syllabus_parse = parsed_at.astimezone(UTC)


def build_syllabus_prompt(syllabus_text: str) -> str:
    return f"{SYLLABUS_PARSE_PROMPT}\n\nSYLLABUS TEXT:\n{syllabus_text.strip()}"


def build_review_flags(parsed: SyllabusParseResult) -> list[str]:
    flags: list[str] = []
    total_weight = sum(category.weight or 0 for category in parsed.grade_categories)
    if parsed.grade_categories and not 0.95 <= total_weight <= 1.05:
        flags.append(f"grade category weights sum to {total_weight:.2f}")
    if parsed.late_policy is None or parsed.late_policy.raw_text is None:
        flags.append("late policy needs review")
    if not parsed.grading_scale:
        flags.append("grading scale missing")
    return flags


def first_textbook(external_tools: list[ExternalTool]) -> str | None:
    for tool in external_tools:
        if tool.textbook:
            return tool.textbook
    return None
