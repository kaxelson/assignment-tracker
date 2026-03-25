from datetime import UTC, datetime
import json

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.ai.syllabus_parser import (
    SyllabusParseResult,
    SYLLABUS_PARSE_ERROR_STAGE,
    SYLLABUS_PARSE_PROVENANCE_STAGE,
    apply_syllabus_parse,
    parse_saved_syllabi,
)
from acc.config import Settings
from acc.db.models import Base, Course, ProvenanceEvent


class FakeSyllabusClient:
    def __init__(self, payload: dict[str, object] | None = None, *, error: Exception | None = None) -> None:
        self.payload = payload
        self.error = error
        self.calls = 0

    async def complete_json(self, prompt: str) -> str:
        self.calls += 1
        assert "SYLLABUS TEXT" in prompt
        if self.error is not None:
            raise self.error
        assert self.payload is not None
        return json.dumps(self.payload)


def build_parse_payload() -> dict[str, object]:
    return {
        "course_name": "Data Structures in Python",
        "course_code": "CSC-242-0C1",
        "instructor": "Professor Lambert",
        "semester": "Spring 2026",
        "grade_categories": [
            {
                "name": "Programming Exercises",
                "weight": 0.4,
                "description": "MindTap exercises",
                "drop_lowest": 2,
                "total_count": 40,
            },
            {
                "name": "Exams",
                "weight": 0.6,
                "description": "Two major exams",
                "drop_lowest": None,
                "total_count": 2,
            },
        ],
        "grading_scale": {
            "A": [93, 100],
            "B": [83, 87],
            "C": [73, 77],
        },
        "late_policy": {
            "default_penalty_per_day": 0.02,
            "max_late_days": 5,
            "accepts_late": True,
            "exceptions": "No late exams",
            "raw_text": "Late work loses 2% per day for up to 5 days.",
        },
        "exams": [
            {
                "name": "Midterm",
                "date": "2026-04-12",
                "weight_pct": 30,
                "topics": "Units 1-6",
                "location": "In class",
            }
        ],
        "important_dates": [],
        "external_tools": [
            {
                "name": "Cengage MindTap",
                "purpose": "Homework",
                "textbook": "Fundamentals of Python Data Structures",
            }
        ],
        "office_hours": "Tuesdays 2-3 PM",
        "attendance_policy": "Attendance is expected.",
        "extra_credit": None,
    }


def test_apply_syllabus_parse_maps_course_fields() -> None:
    course = Course(
        id="csc-242-0c1-spring-2026",
        code="CSC-242-0C1",
        name="Data Structures in Python",
        instructor=None,
        d2l_course_id="189252",
        d2l_url="https://d2l.oakton.edu/d2l/home/189252",
        semester="Spring 2026",
        external_platform="cengage_mindtap",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text="Syllabus text",
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    parsed = SyllabusParseResult.model_validate(build_parse_payload())

    apply_syllabus_parse(
        course,
        parsed_syllabus=type("Parsed", (), {"parsed": parsed, "review_flags": []})(),
        parsed_at=datetime(2026, 3, 23, 2, 0, tzinfo=UTC),
    )

    assert course.instructor == "Professor Lambert"
    assert course.textbook == "Fundamentals of Python Data Structures"
    assert course.late_policy_global == "Late work loses 2% per day for up to 5 days."
    assert course.grading_scale == build_parse_payload()["grading_scale"]
    assert course.grade_categories is not None
    assert course.grade_categories[0]["name"] == "Programming Exercises"
    assert course.syllabus_parsed is not None
    assert course.syllabus_parsed["course_name"] == "Data Structures in Python"


@pytest.mark.asyncio
async def test_parse_saved_syllabi_persists_results_and_skips_parsed_courses(tmp_path) -> None:
    db_path = tmp_path / "syllabus.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        session.add_all(
            [
                Course(
                    id="csc-242-0c1-spring-2026",
                    code="CSC-242-0C1",
                    name="Data Structures in Python",
                    instructor=None,
                    d2l_course_id="189252",
                    d2l_url="https://d2l.oakton.edu/d2l/home/189252",
                    semester="Spring 2026",
                    external_platform="cengage_mindtap",
                    external_platform_url=None,
                    textbook=None,
                    syllabus_raw_text="Python syllabus",
                    syllabus_parsed=None,
                    grading_scale=None,
                    grade_categories=None,
                    late_policy_global=None,
                    current_grade_pct=None,
                    current_letter_grade=None,
                    last_scraped_d2l=None,
                    last_scraped_external=None,
                    last_syllabus_parse=None,
                ),
                Course(
                    id="phy-221-001-spring-2026",
                    code="PHY-221-001",
                    name="General Physics I",
                    instructor=None,
                    d2l_course_id="188307",
                    d2l_url="https://d2l.oakton.edu/d2l/home/188307",
                    semester="Spring 2026",
                    external_platform="pearson_mylab",
                    external_platform_url=None,
                    textbook=None,
                    syllabus_raw_text="Physics syllabus",
                    syllabus_parsed={"course_name": "General Physics I"},
                    grading_scale=None,
                    grade_categories=None,
                    late_policy_global=None,
                    current_grade_pct=None,
                    current_letter_grade=None,
                    last_scraped_d2l=None,
                    last_scraped_external=None,
                    last_syllabus_parse=datetime(2026, 3, 20, 1, 0, tzinfo=UTC),
                ),
            ]
        )
        await session.commit()

    fake_client = FakeSyllabusClient(build_parse_payload())
    settings = Settings()

    async with session_factory() as session:
        summary = await parse_saved_syllabi(
            session,
            settings,
            force=False,
            client=fake_client,
        )
        await session.commit()
        parsed_course = await session.get(Course, "csc-242-0c1-spring-2026")
        skipped_course = await session.get(Course, "phy-221-001-spring-2026")
        prov_rows = (await session.scalars(select(ProvenanceEvent).order_by(ProvenanceEvent.id.asc()))).all()

    await engine.dispose()

    assert summary.courses_parsed == 1
    assert summary.courses_skipped == 0
    assert summary.courses_failed == 0
    assert fake_client.calls == 1
    assert parsed_course is not None
    assert parsed_course.syllabus_parsed is not None
    assert parsed_course.instructor == "Professor Lambert"
    assert skipped_course is not None
    assert skipped_course.syllabus_parsed == {"course_name": "General Physics I"}
    assert len(prov_rows) == 1
    assert prov_rows[0].stage == SYLLABUS_PARSE_PROVENANCE_STAGE
    assert prov_rows[0].course_id == "csc-242-0c1-spring-2026"
    assert prov_rows[0].detail["ok"] is True
    assert prov_rows[0].detail["grade_category_count"] == 2


@pytest.mark.asyncio
async def test_parse_saved_syllabi_records_provenance_on_llm_error(tmp_path) -> None:
    db_path = tmp_path / "syllabus-fail.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        session.add(
            Course(
                id="fail-101-spring-2026",
                code="FAIL-101",
                name="Failure Lab",
                instructor=None,
                d2l_course_id="1",
                d2l_url="https://d2l.oakton.edu/d2l/home/1",
                semester="Spring 2026",
                external_platform=None,
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text="Some syllabus body for preview",
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=None,
                current_letter_grade=None,
                last_scraped_d2l=None,
                last_scraped_external=None,
                last_syllabus_parse=None,
            )
        )
        await session.commit()

    fake_client = FakeSyllabusClient(error=RuntimeError("OpenAI unavailable"))
    settings = Settings()

    async with session_factory() as session:
        summary = await parse_saved_syllabi(session, settings, force=False, client=fake_client)
        await session.commit()
        prov_rows = (await session.scalars(select(ProvenanceEvent))).all()

    await engine.dispose()

    assert summary.courses_parsed == 0
    assert summary.courses_failed == 1
    assert len(prov_rows) == 1
    assert prov_rows[0].stage == SYLLABUS_PARSE_ERROR_STAGE
    assert prov_rows[0].detail["ok"] is False
    assert prov_rows[0].detail["error_type"] == "RuntimeError"
    assert "OpenAI unavailable" in str(prov_rows[0].detail["error"])
    assert prov_rows[0].text_preview == "Some syllabus body for preview"
