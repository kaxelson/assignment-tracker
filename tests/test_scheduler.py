from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.db.models import Assignment, Base, Course
from acc.db.repository import Repository
from acc.scheduler.planner import explain_priority, generate_agenda_plan, priority_score


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
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=None,
        points_earned=None,
        status="upcoming",
        late_policy=None,
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
        ("2026-03-24", 45),
        ("2026-03-25", 45),
    ]


def test_generate_agenda_plan_schedules_overdue_work_on_today() -> None:
    course = Course(
        id="cis-156-spring-2026",
        code="CIS-156",
        name="Data Structures",
        instructor=None,
        d2l_course_id="156001",
        d2l_url="https://d2l.oakton.edu/d2l/home/156001",
        semester="Spring 2026",
        external_platform=None,
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
        id="cis-156-spring-2026-late-lab",
        course_id="cis-156-spring-2026",
        title="Late Lab",
        description=None,
        type="homework",
        source_platform="d2l",
        external_url=None,
        due_date=datetime(2026, 3, 10, 5, 59, 0, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=None,
        points_earned=None,
        status="overdue",
        late_policy=None,
        estimated_minutes=45,
        raw_scraped_data={"due_on": "2026-03-10"},
        course=course,
    )

    plan = generate_agenda_plan(
        [assignment],
        now=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
        horizon_days=7,
        daily_minutes=120,
    )

    assert plan, "overdue assignment should appear on the agenda"
    assert all(entry.agenda_date.isoformat() == "2026-03-24" for entry in plan)
    assert sum(entry.planned_minutes for entry in plan) == 45


def test_generate_agenda_plan_does_not_split_exams_across_agenda_days() -> None:
    course = Course(
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
        current_grade_pct=90.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )

    from acc.db.repository import CanonicalAssignment

    assignment = CanonicalAssignment(
        id="csc-242-0c1-spring-2026-unit-7-quiz",
        course_id="csc-242-0c1-spring-2026",
        title="Unit 7 Reviewing the Basics Quiz",
        description=None,
        type="exam",
        source_platform="cengage_mindtap",
        external_url=None,
        due_date=datetime(2026, 3, 30, 5, 59, 0),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=100.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=None,
        raw_scraped_data={
            "due_on": "2026-03-29",
            "due_at": "2026-03-29T23:59:00-06:00",
        },
        course=course,
    )

    plan = generate_agenda_plan(
        [assignment],
        now=datetime(2026, 3, 28, 18, 0, tzinfo=UTC),
        horizon_days=5,
        daily_minutes=120,
    )

    assert {entry.agenda_date.isoformat() for entry in plan} == {"2026-03-29"}
    assert sum(entry.planned_minutes for entry in plan) == 90


def test_explain_priority_handles_missing_due_date() -> None:
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
        id="pearson-phy-221-001-spring-2026-reading",
        course_id="phy-221-001-spring-2026",
        title="Reading Review",
        description=None,
        type="reading",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=None,
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=None,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=30,
        raw_scraped_data=None,
        course=course,
    )

    assert explain_priority(assignment, today=datetime(2026, 3, 24, 12, 0, tzinfo=UTC).date()) == []


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
    assert plan[0].assignment_id == "d2l-6-7"
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


def test_priority_score_uses_parsed_grade_categories() -> None:
    weighted_course = Course(
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
        grade_categories=[
            {
                "name": "Homework",
                "weight": 0.2,
                "description": "Weekly homework assignments via MyLab",
            }
        ],
        late_policy_global=None,
        current_grade_pct=78.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    unweighted_course = Course(
        id="eng-101-spring-2026",
        code="ENG-101",
        name="Composition",
        instructor=None,
        d2l_course_id="100001",
        d2l_url="https://d2l.oakton.edu/d2l/home/100001",
        semester="Spring 2026",
        external_platform=None,
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=92.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )

    from acc.db.repository import CanonicalAssignment

    weighted_homework = CanonicalAssignment(
        id="hw-6",
        course_id=weighted_course.id,
        title="HW 6",
        description=None,
        type="homework",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=100.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=45,
        raw_scraped_data=None,
        course=weighted_course,
    )
    unweighted_reading = CanonicalAssignment(
        id="reading-7",
        course_id=unweighted_course.id,
        title="Chapter 7 Reading",
        description=None,
        type="reading",
        source_platform="d2l",
        external_url=None,
        due_date=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=None,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=45,
        raw_scraped_data=None,
        course=unweighted_course,
    )

    assert priority_score(weighted_homework, today=datetime(2026, 3, 23, tzinfo=UTC).date()) > priority_score(
        unweighted_reading,
        today=datetime(2026, 3, 23, tzinfo=UTC).date(),
    )


def test_priority_score_uses_late_policy_severity() -> None:
    strict_course = Course(
        id="exam-course",
        code="MTH-131",
        name="Calculus I",
        instructor=None,
        d2l_course_id="100002",
        d2l_url="https://d2l.oakton.edu/d2l/home/100002",
        semester="Spring 2026",
        external_platform=None,
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed={
            "late_policy": {
                "accepts_late": False,
                "exceptions": "Exams and quizzes cannot be submitted late",
            }
        },
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=88.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    flexible_course = Course(
        id="lab-course",
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
        syllabus_parsed={
            "late_policy": {
                "accepts_late": True,
                "default_penalty_per_day": 0.02,
                "max_late_days": 5,
                "exceptions": "Exams and quizzes cannot be submitted late",
            }
        },
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=88.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )

    from acc.db.repository import CanonicalAssignment

    strict_exam = CanonicalAssignment(
        id="midterm-1",
        course_id=strict_course.id,
        title="Midterm 1",
        description=None,
        type="exam",
        source_platform="d2l",
        external_url=None,
        due_date=datetime(2026, 3, 24, 18, 0, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=100.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=90,
        raw_scraped_data=None,
        course=strict_course,
    )
    flexible_lab = CanonicalAssignment(
        id="lab-7",
        course_id=flexible_course.id,
        title="Lab 7",
        description=None,
        type="lab",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=datetime(2026, 3, 24, 18, 0, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=25.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=60,
        raw_scraped_data=None,
        course=flexible_course,
    )

    assert priority_score(strict_exam, today=datetime(2026, 3, 23, tzinfo=UTC).date()) > priority_score(
        flexible_lab,
        today=datetime(2026, 3, 23, tzinfo=UTC).date(),
    )


def test_priority_score_uses_grading_scale_pressure() -> None:
    near_cutoff_course = Course(
        id="near-cutoff-course",
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
        grading_scale={"A": [93.0, 100.0], "A-": [90.0, 93.0]},
        grade_categories=[{"name": "Homework", "weight": 0.2}],
        late_policy_global=None,
        current_grade_pct=89.2,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    stable_course = Course(
        id="stable-course",
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
        grading_scale={"A": [93.0, 100.0], "A-": [90.0, 93.0]},
        grade_categories=[{"name": "Homework", "weight": 0.2}],
        late_policy_global=None,
        current_grade_pct=82.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )

    from acc.db.repository import CanonicalAssignment

    near_cutoff_homework = CanonicalAssignment(
        id="near-cutoff-hw",
        course_id=near_cutoff_course.id,
        title="HW 6",
        description=None,
        type="homework",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=100.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=45,
        raw_scraped_data=None,
        course=near_cutoff_course,
    )
    stable_homework = CanonicalAssignment(
        id="stable-hw",
        course_id=stable_course.id,
        title="HW 6",
        description=None,
        type="homework",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=None,
        points_possible=100.0,
        points_earned=None,
        status="upcoming",
        late_policy=None,
        estimated_minutes=45,
        raw_scraped_data=None,
        course=stable_course,
    )

    assert priority_score(
        near_cutoff_homework,
        today=datetime(2026, 3, 23, tzinfo=UTC).date(),
    ) > priority_score(
        stable_homework,
        today=datetime(2026, 3, 23, tzinfo=UTC).date(),
    )
