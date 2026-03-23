from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.db.models import Assignment, Base, Course
from acc.db.repository import Repository
from acc.scheduler.planner import generate_agenda_plan


def test_generate_agenda_plan_splits_work_across_days() -> None:
    course = Course(
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
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=90.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )

    from acc.db.repository import CanonicalAssignment

    assignment = CanonicalAssignment(
        id="pearson-phy-221-001-spring-2026-hw-6",
        course_id="phy-221-001-spring-2026",
        title="HW 6",
        description=None,
        type="homework",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        grade_pct=None,
        points_possible=None,
        points_earned=None,
        status="upcoming",
        estimated_minutes=90,
        raw_scraped_data=None,
        course=course,
    )

    plan = generate_agenda_plan(
        [assignment],
        now=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        horizon_days=3,
        daily_minutes=60,
    )

    assert [(entry.agenda_date.isoformat(), entry.planned_minutes) for entry in plan] == [
        ("2026-03-25", 45),
        ("2026-03-26", 45),
    ]


@pytest.mark.asyncio
async def test_agenda_generation_uses_canonical_assignments(tmp_path) -> None:
    db_path = tmp_path / "agenda.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime(2026, 3, 22, 18, 0, tzinfo=UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                instructor=None,
                d2l_course_id="189252",
                d2l_url="https://d2l.oakton.edu/d2l/home/189252",
                semester="Spring 2026",
                external_platform="cengage_mindtap",
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=46.92,
                current_letter_grade=None,
                last_scraped_d2l=now,
                last_scraped_external=now,
                last_syllabus_parse=None,
            )
        )
        session.add_all(
            [
                Assignment(
                    id="d2l-6-7",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.7: Unit 6 Code: Unit 6: Inheritance and...",
                    description=None,
                    type="homework",
                    source_platform="d2l",
                    external_url="https://d2l.oakton.edu/event/7590085",
                    available_date=None,
                    due_date=datetime(2026, 3, 23, 4, 59, tzinfo=UTC),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=100.0,
                    points_earned=None,
                    grade_pct=None,
                    status="upcoming",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"due_text": "MAR 22 11:59 PM"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cengage-6-7",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.7Not startedCOUNTS TOWARDS GRADE",
                    description="Chapter exercise",
                    type="homework",
                    source_platform="cengage_mindtap",
                    external_url="https://ng.cengage.com/activity/2733512518",
                    available_date=None,
                    due_date=None,
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=100.0,
                    points_earned=None,
                    grade_pct=None,
                    status="upcoming",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=30,
                    is_multi_day=False,
                    raw_scraped_data={"row_text": "Programming Exercise 6.7Not startedCOUNTS TOWARDS GRADE"},
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async with session_factory() as session:
        repository = Repository(session)
        canonical_assignments = list(await repository.list_canonical_assignments())
        plan = generate_agenda_plan(
            canonical_assignments,
            now=now,
            horizon_days=3,
            daily_minutes=90,
        )

    await engine.dispose()

    assert len(canonical_assignments) == 1
    assert canonical_assignments[0].title == "Programming Exercise 6.7"
    assert len(plan) == 1
    assert plan[0].assignment_id == "cengage-6-7"
    assert plan[0].planned_minutes == 30


@pytest.mark.asyncio
async def test_canonical_status_prefers_completed_external_state(tmp_path) -> None:
    db_path = tmp_path / "agenda-status.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime(2026, 3, 22, 18, 0, tzinfo=UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                instructor=None,
                d2l_course_id="189252",
                d2l_url="https://d2l.oakton.edu/d2l/home/189252",
                semester="Spring 2026",
                external_platform="cengage_mindtap",
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=46.92,
                current_letter_grade=None,
                last_scraped_d2l=now,
                last_scraped_external=now,
                last_syllabus_parse=None,
            )
        )
        session.add_all(
            [
                Assignment(
                    id="d2l-6-3",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.3: Unit 6 Code: Unit 6: Inheritance and...",
                    description=None,
                    type="homework",
                    source_platform="d2l",
                    external_url="https://d2l.oakton.edu/event/7590084",
                    available_date=None,
                    due_date=datetime(2026, 3, 23, 4, 59, tzinfo=UTC),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=100.0,
                    points_earned=None,
                    grade_pct=None,
                    status="upcoming",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"due_text": "MAR 22 11:59 PM"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cengage-6-3",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.3SubmittedCOUNTS TOWARDS GRADE",
                    description=None,
                    type="homework",
                    source_platform="cengage_mindtap",
                    external_url="https://ng.cengage.com/activity/2733512516",
                    available_date=None,
                    due_date=None,
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=100.0,
                    points_earned=100.0,
                    grade_pct=100.0,
                    status="completed",
                    is_submitted=True,
                    submitted_at=now,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data=None,
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async with session_factory() as session:
        repository = Repository(session)
        canonical_assignments = list(await repository.list_canonical_assignments())

    await engine.dispose()

    assert len(canonical_assignments) == 1
    assert canonical_assignments[0].status == "completed"
