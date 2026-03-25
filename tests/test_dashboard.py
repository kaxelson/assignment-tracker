from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import acc.dashboard.app as dashboard_app_module
from acc.dashboard.app import app, format_local_due_label, render_dashboard_html
from acc.db.engine import get_session
from acc.db.models import Assignment, Base, Course


@pytest.mark.asyncio
async def test_overview_endpoint_returns_synced_coursework(tmp_path) -> None:
    db_path = tmp_path / "dashboard.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="cis-156-spring-2026",
                code="CIS-156",
                name="Data Structures in Python",
                instructor="Professor Lambert",
                d2l_course_id="156001",
                d2l_url="https://d2l.oakton.edu/d2l/home/156001",
                semester="Spring 2026",
                external_platform=None,
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed={
                    "late_policy": {
                        "accepts_late": True,
                        "default_penalty_per_day": 0.05,
                        "max_late_days": 2,
                    }
                },
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=94.5,
                current_letter_grade="A",
                last_scraped_d2l=now,
                last_scraped_external=None,
                last_syllabus_parse=None,
            )
        )
        session.add_all(
            [
                Assignment(
                    id="cis-156-spring-2026-linked-list-lab",
                    course_id="cis-156-spring-2026",
                    title="Linked List Lab",
                    description=None,
                    type="lab",
                    source_platform="d2l",
                    external_url=None,
                    available_date=None,
                    due_date=now + timedelta(days=1),
                    close_date=None,
                    grade_category="Labs",
                    grade_weight_pct=10.0,
                    points_possible=20.0,
                    points_earned=19.0,
                    grade_pct=None,
                    status="upcoming",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"due_text": "APR 1 11:59 PM"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cis-156-spring-2026-stack-quiz",
                    course_id="cis-156-spring-2026",
                    title="Stack Quiz",
                    description=None,
                    type="exam",
                    source_platform="d2l",
                    external_url=None,
                    available_date=None,
                    due_date=now - timedelta(days=1),
                    close_date=None,
                    grade_category="Quizzes",
                    grade_weight_pct=15.0,
                    points_possible=10.0,
                    points_earned=10.0,
                    grade_pct=None,
                    status="graded",
                    is_submitted=True,
                    submitted_at=now - timedelta(days=2),
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"due_text": "MAR 28 11:59 PM"},
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()

    assert payload["database_ready"] is True
    assert payload["summary"] == {
        "course_count": 1,
        "assignment_count": 2,
        "upcoming_count": 1,
        "urgent_count": 1,
    }
    courses = payload["courses"]
    assert len(courses) == 1
    grade_detail = courses[0].pop("grade_detail")
    assert isinstance(grade_detail, dict)
    assert grade_detail.get("final_grade_pct") == 98.0
    assert courses == [
        {
            "id": "cis-156-spring-2026",
            "code": "CIS-156",
            "name": "Data Structures in Python",
            "semester": "Spring 2026",
            "current_grade_pct": 98.0,
            "assignment_count": 2,
            "upcoming_count": 1,
            "external_platform": None,
            "d2l_url": "https://d2l.oakton.edu/d2l/home/156001",
            "syllabus_url": "https://d2l.oakton.edu/d2l/le/content/156001/Home",
        }
    ]
    assert payload["upcoming_assignments"][0]["title"] == "Linked List Lab"
    assert payload["upcoming_assignments"][0]["course_code"] == "CIS-156"
    expected_due_label = (now + timedelta(days=1)).astimezone(ZoneInfo("America/Chicago")).strftime(
        "%m/%d/%y %-I:%M %p"
    )
    assert payload["upcoming_assignments"][0]["due_label"] == expected_due_label
    assert payload["upcoming_assignments"][0]["priority_score"] > 0
    assert payload["upcoming_assignments"][0]["priority_reasons"] == [
        "Due tomorrow",
        "Labs worth about 10% of course grade",
        "5% penalty per late day",
    ]
    lab_id = "cis-156-spring-2026-linked-list-lab"
    assert any(row["assignment_id"] == lab_id for row in payload["agenda_entries"])
    assert payload["agenda_days"]
    assert any(
        any(item["assignment_id"] == lab_id for item in day["items"]) for day in payload["agenda_days"]
    )


@pytest.mark.asyncio
async def test_overview_endpoint_dedupes_d2l_and_external_assignments(tmp_path) -> None:
    db_path = tmp_path / "dashboard-dedupe.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                instructor="Professor H",
                d2l_course_id="189252",
                d2l_url="https://d2l.oakton.edu/d2l/home/189252",
                semester="Spring 2026",
                external_platform="cengage_mindtap",
                external_platform_url="https://gateway.cengage.com/course/123",
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
                    id="csc-242-0c1-spring-2026-7590085",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.7: Unit 6 Code: Unit 6: Inheritance and...",
                    description=None,
                    type="homework",
                    source_platform="d2l",
                    external_url="https://d2l.oakton.edu/d2l/le/calendar/189252/event/7590085/detailsview#7590085",
                    available_date=None,
                    due_date=now + timedelta(days=1),
                    close_date=None,
                    grade_category="Programming Exercises",
                    grade_weight_pct=5.0,
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
                    raw_scraped_data={"due_text": "MAR 23 11:59 PM"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cengage-csc-242-0c1-spring-2026-programming-exercise-6-7",
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
                    estimated_minutes=25,
                    is_multi_day=False,
                    raw_scraped_data={"row_text": "Programming Exercise 6.7Not startedCOUNTS TOWARDS GRADE"},
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"] == {
        "course_count": 1,
        "assignment_count": 1,
        "upcoming_count": 1,
        "urgent_count": 1,
    }
    assert payload["courses"][0]["assignment_count"] == 1
    assert payload["courses"][0]["upcoming_count"] == 1
    assert payload["upcoming_assignments"][0]["title"] == "Programming Exercise 6.7"
    expected_due_label = (now + timedelta(days=1)).astimezone(ZoneInfo("America/Chicago")).strftime(
        "%m/%d/%y %-I:%M %p"
    )
    assert payload["upcoming_assignments"][0]["due_label"] == expected_due_label
    merged_id = payload["upcoming_assignments"][0]["id"]
    assert any(row["assignment_id"] == merged_id for row in payload["agenda_entries"])
    assert payload["agenda_days"]


@pytest.mark.asyncio
async def test_overview_endpoint_filters_unassigned_external_inventory_from_counts(tmp_path) -> None:
    db_path = tmp_path / "dashboard-python-filter.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                instructor="Professor H",
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
                    id="cengage-programming-exercise-7-10",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 7.10Not startedCOUNTS TOWARDS GRADE",
                    description="Complete this lab assignment in the code editor.",
                    type="homework",
                    source_platform="cengage_mindtap",
                    external_url=None,
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
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"row_text": "Programming Exercise 7.10Not startedCOUNTS TOWARDS GRADE"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cengage-programming-exercise-7-2",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 7.2Not startedCOUNTS TOWARDS GRADE",
                    description="Complete this lab assignment in the code editor.",
                    type="homework",
                    source_platform="cengage_mindtap",
                    external_url=None,
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
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={
                        "announcement_title": "What to do the 10th week?",
                        "announcement_posted_at_text": "Mar 23, 2026 12:01 AM",
                    },
                    last_scraped=now,
                ),
                Assignment(
                    id="d2l-programming-exercise-6-7",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.7: Unit 6 Code: Unit 6: Inheritance and...",
                    description=None,
                    type="homework",
                    source_platform="d2l",
                    external_url=None,
                    available_date=None,
                    due_date=None,
                    close_date=None,
                    grade_category="Midterm Exam",
                    grade_weight_pct=10.0,
                    points_possible=100.0,
                    points_earned=0.0,
                    grade_pct=0.0,
                    status="available",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"grade_category": "Midterm Exam"},
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["assignment_count"] == 2
    assert payload["courses"][0]["assignment_count"] == 2


@pytest.mark.asyncio
async def test_overview_endpoint_merges_external_grades_into_course_health(tmp_path) -> None:
    db_path = tmp_path / "dashboard-grade-merge.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                instructor="Professor H",
                d2l_course_id="189252",
                d2l_url="https://d2l.oakton.edu/d2l/home/189252",
                semester="Spring 2026",
                external_platform="cengage_mindtap",
                external_platform_url="https://gateway.cengage.com/course/123",
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=[{"name": "Programming Exercises", "weight": 0.4}],
                late_policy_global=None,
                current_grade_pct=75.0,
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
                    due_date=now + timedelta(days=1),
                    close_date=None,
                    grade_category="Programming Exercises",
                    grade_weight_pct=20.0,
                    points_possible=100.0,
                    points_earned=50.0,
                    grade_pct=50.0,
                    status="graded",
                    is_submitted=True,
                    submitted_at=now,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data={"due_text": "APR 1 11:59 PM"},
                    last_scraped=now,
                ),
                Assignment(
                    id="cengage-6-7",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.7SubmittedCOUNTS TOWARDS GRADE",
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
                    points_earned=100.0,
                    grade_pct=100.0,
                    status="completed",
                    is_submitted=True,
                    submitted_at=now,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=25,
                    is_multi_day=False,
                    raw_scraped_data={"row_text": "Programming Exercise 6.7 Submitted 100/100 points"},
                    last_scraped=now,
                ),
                Assignment(
                    id="d2l-6-5",
                    course_id="csc-242-0c1-spring-2026",
                    title="Programming Exercise 6.5",
                    description=None,
                    type="homework",
                    source_platform="d2l",
                    external_url=None,
                    available_date=None,
                    due_date=now + timedelta(days=2),
                    close_date=None,
                    grade_category="Programming Exercises",
                    grade_weight_pct=20.0,
                    points_possible=100.0,
                    points_earned=100.0,
                    grade_pct=100.0,
                    status="graded",
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

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"]["assignment_count"] == 2
    assert payload["courses"][0]["current_grade_pct"] == 100.0


@pytest.mark.asyncio
async def test_overview_endpoint_counts_overdue_items_beyond_display_limit(tmp_path) -> None:
    db_path = tmp_path / "dashboard-urgent-count.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
            Course(
                id="mat-251-050-spring-2026",
                code="MAT-251-050",
                name="Calculus II",
                instructor=None,
                d2l_course_id="188500",
                d2l_url="https://d2l.oakton.edu/d2l/home/188500",
                semester="Spring 2026",
                external_platform="pearson_mylab",
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=None,
                current_letter_grade=None,
                last_scraped_d2l=now,
                last_scraped_external=now,
                last_syllabus_parse=None,
            )
        )
        for index in range(14):
            session.add(
                Assignment(
                    id=f"mat-overdue-{index}",
                    course_id="mat-251-050-spring-2026",
                    title=f"HW {index}",
                    description=None,
                    type="homework",
                    source_platform="pearson_mylab",
                    external_url=None,
                    available_date=None,
                    due_date=now - timedelta(days=index + 1),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=None,
                    points_earned=None,
                    grade_pct=None,
                    status="overdue",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=True,
                    days_late=index + 1,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data=None,
                    last_scraped=now,
                )
            )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"]["upcoming_count"] == 14
    assert payload["summary"]["urgent_count"] == 14
    assert len(payload["upcoming_assignments"]) == 12


@pytest.mark.asyncio
async def test_overview_endpoint_excludes_practice_tests_completely_and_sorts_by_due_then_course(
    tmp_path,
) -> None:
    db_path = tmp_path / "dashboard-sort.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add_all(
            [
                Course(
                    id="mat-251-050-spring-2026",
                    code="MAT-251-050",
                    name="Calculus II",
                    instructor=None,
                    d2l_course_id="188500",
                    d2l_url="https://d2l.oakton.edu/d2l/home/188500",
                    semester="Spring 2026",
                    external_platform="pearson_mylab",
                    external_platform_url=None,
                    textbook=None,
                    syllabus_raw_text=None,
                    syllabus_parsed=None,
                    grading_scale=None,
                    grade_categories=None,
                    late_policy_global=None,
                    current_grade_pct=None,
                    current_letter_grade=None,
                    last_scraped_d2l=now,
                    last_scraped_external=now,
                    last_syllabus_parse=None,
                ),
                Course(
                    id="phy-221-001-spring-2026",
                    code="PHY-221-001",
                    name="Physics I",
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
                    current_grade_pct=None,
                    current_letter_grade=None,
                    last_scraped_d2l=now,
                    last_scraped_external=now,
                    last_syllabus_parse=None,
                ),
            ]
        )
        session.add_all(
            [
                Assignment(
                    id="practice-test",
                    course_id="mat-251-050-spring-2026",
                    title="Practice Test 1",
                    description=None,
                    type="exam",
                    source_platform="pearson_mylab",
                    external_url=None,
                    available_date=None,
                    due_date=now + timedelta(hours=2),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=None,
                    points_earned=None,
                    grade_pct=None,
                    status="in_progress",
                    is_submitted=False,
                    submitted_at=None,
                    is_late=False,
                    days_late=0,
                    late_policy=None,
                    estimated_minutes=None,
                    is_multi_day=False,
                    raw_scraped_data=None,
                    last_scraped=now,
                ),
                Assignment(
                    id="math-homework",
                    course_id="mat-251-050-spring-2026",
                    title="HW 8.9",
                    description=None,
                    type="homework",
                    source_platform="pearson_mylab",
                    external_url=None,
                    available_date=None,
                    due_date=now + timedelta(hours=3),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=None,
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
                    raw_scraped_data=None,
                    last_scraped=now,
                ),
                Assignment(
                    id="physics-homework",
                    course_id="phy-221-001-spring-2026",
                    title="Centripetal Force Lab",
                    description=None,
                    type="lab",
                    source_platform="d2l",
                    external_url=None,
                    available_date=None,
                    due_date=now + timedelta(hours=3),
                    close_date=None,
                    grade_category=None,
                    grade_weight_pct=None,
                    points_possible=None,
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
                    raw_scraped_data=None,
                    last_scraped=now,
                ),
            ]
        )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/api/overview")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    payload = response.json()

    assert payload["summary"]["assignment_count"] == 2
    assert payload["summary"]["upcoming_count"] == 2
    assert payload["summary"]["urgent_count"] == 2
    assert [item["title"] for item in payload["upcoming_assignments"][:3]] == [
        "HW 8.9",
        "Centripetal Force Lab",
    ]


def test_render_dashboard_html_includes_grade_detail_dialog() -> None:
    html = render_dashboard_html(
        {
            "generated_at": "2026-03-23T23:00:00+00:00",
            "database_ready": True,
            "error": None,
            "d2l_storage_state": True,
            "summary": {
                "course_count": 1,
                "assignment_count": 1,
                "upcoming_count": 0,
                "urgent_count": 0,
            },
            "courses": [
                {
                    "id": "demo-course",
                    "code": "DEMO-100",
                    "name": "Demo",
                    "semester": "Fall 2026",
                    "current_grade_pct": 88.0,
                    "assignment_count": 1,
                    "upcoming_count": 0,
                    "external_platform": None,
                    "d2l_url": None,
                    "grade_detail": {
                        "final_grade_pct": 88.0,
                        "total_weight_pct": 100.0,
                        "numerator_weighted_points": 88.0,
                        "components": [
                            {
                                "type": "assignment",
                                "title": "Lab 1",
                                "weight_pct": 100.0,
                                "grade_pct": 88.0,
                                "weighted_points": 88.0,
                            }
                        ],
                        "excluded": [],
                        "excluded_count": 0,
                        "excluded_truncated": 0,
                        "notes": ["Test note."],
                    },
                }
            ],
            "upcoming_assignments": [],
            "agenda_entries": [],
            "agenda_days": [],
        }
    )
    assert "grade-detail-dialog" in html
    assert "Show detail" in html
    assert "openGradeDetail" in html
    assert "demo-course" in html


def test_render_dashboard_html_includes_assignment_button_and_syllabus_link() -> None:
    html = render_dashboard_html(
        {
            "generated_at": "2026-03-23T23:00:00+00:00",
            "database_ready": True,
            "error": None,
            "d2l_storage_state": True,
            "summary": {
                "course_count": 1,
                "assignment_count": 1,
                "upcoming_count": 1,
                "urgent_count": 1,
            },
            "courses": [
                {
                    "id": "demo-course",
                    "code": "DEMO-100",
                    "name": "Demo",
                    "semester": "Fall 2026",
                    "current_grade_pct": 88.0,
                    "assignment_count": 1,
                    "upcoming_count": 1,
                    "external_platform": None,
                    "d2l_url": "https://d2l.example.edu/d2l/home/100",
                    "syllabus_url": "https://d2l.example.edu/d2l/le/content/100/Home",
                    "grade_detail": {},
                }
            ],
            "upcoming_assignments": [
                {
                    "id": "a1",
                    "title": "Lab 1",
                    "course_code": "DEMO-100",
                    "course_name": "Demo",
                    "due_at": "2026-03-24T12:00:00Z",
                    "due_calendar_date": "2026-03-24",
                    "due_label": "03/24/26 7:00 AM",
                    "status": "upcoming",
                    "type": "lab",
                    "grade_pct": None,
                    "external_url": "https://d2l.example.edu/d2l/le/content/100/viewContent/1/View",
                    "priority_score": 1.0,
                    "priority_reasons": ["Due tomorrow"],
                }
            ],
            "agenda_entries": [],
            "agenda_days": [],
        }
    )
    assert "Go to assignment" in html
    assert "Syllabus" in html


def test_format_local_due_label_treats_naive_db_datetime_as_utc_wall(monkeypatch) -> None:
    monkeypatch.setattr(dashboard_app_module.settings, "timezone", "America/Chicago")
    naive_utc = datetime(2026, 3, 30, 5, 59, 0)
    expected = naive_utc.replace(tzinfo=UTC).astimezone(ZoneInfo("America/Chicago")).strftime(
        "%m/%d/%y %-I:%M %p"
    )
    assert format_local_due_label(naive_utc) == expected


def test_render_dashboard_html_formats_updated_time_in_local_timezone() -> None:
    html = render_dashboard_html(
        {
            "generated_at": "2026-03-23T23:00:00+00:00",
            "database_ready": True,
            "error": None,
            "d2l_storage_state": True,
            "summary": {
                "course_count": 1,
                "assignment_count": 1,
                "upcoming_count": 1,
                "urgent_count": 1,
            },
            "courses": [],
            "upcoming_assignments": [],
            "agenda_entries": [],
            "agenda_days": [],
        }
    )

    assert "Monday, March 23" in html
    assert "updated 6:00 PM CDT" in html


def test_render_dashboard_html_failed_refresh_shows_hoverable_error() -> None:
    html = render_dashboard_html(
        {
            "generated_at": "2026-03-23T23:00:00+00:00",
            "database_ready": True,
            "error": None,
            "d2l_storage_state": True,
            "refresh_status": {
                "running": False,
                "current_phase": None,
                "current_detail": None,
                "progress_fraction": None,
                "last_started_at": None,
                "last_completed_at": "2026-03-23T23:00:00+00:00",
                "last_error": "ACC_OPENAI_API_KEY is required",
                "last_result": None,
                "message": None,
            },
            "summary": {
                "course_count": 1,
                "assignment_count": 1,
                "upcoming_count": 1,
                "urgent_count": 1,
            },
            "courses": [],
            "upcoming_assignments": [],
            "agenda_entries": [],
            "agenda_days": [],
        }
    )
    assert "refresh-status--failed" in html
    assert "ACC_OPENAI_API_KEY is required" in html
    assert "title=" in html


def test_render_dashboard_html_includes_refresh_controls() -> None:
    html = render_dashboard_html(
        {
            "generated_at": "2026-03-23T23:00:00+00:00",
            "database_ready": True,
            "error": None,
            "d2l_storage_state": True,
            "refresh_status": {
                "running": False,
                "current_phase": None,
                "current_detail": None,
                "progress_fraction": None,
                "last_started_at": None,
                "last_completed_at": None,
                "last_error": None,
                "last_result": None,
                "message": None,
            },
            "summary": {
                "course_count": 1,
                "assignment_count": 1,
                "upcoming_count": 1,
                "urgent_count": 1,
            },
            "courses": [],
            "upcoming_assignments": [],
            "agenda_entries": [],
            "agenda_days": [],
        }
    )

    assert 'id="refresh-button"' in html
    assert 'id="refresh-status"' in html
    assert 'id="refresh-status-label"' in html
    assert 'id="refresh-status-detail"' in html
    assert 'id="refresh-progress"' in html
    assert "Ready to refresh." in html
    assert "/api/refresh" in html
    assert "/api/refresh-status" in html


def test_refresh_status_endpoint_returns_state() -> None:
    with TestClient(app) as client:
        response = client.get("/api/refresh-status")

    assert response.status_code == 200
    payload = response.json()

    assert set(payload) == {
        "running",
        "current_phase",
        "current_detail",
        "progress_fraction",
        "last_started_at",
        "last_completed_at",
        "last_error",
        "last_result",
        "message",
    }


def test_refresh_endpoint_accepts_additive_mode(monkeypatch) -> None:
    class DummyTask:
        pass

    monkeypatch.setattr(
        dashboard_app_module.asyncio,
        "create_task",
        lambda coro: (coro.close(), DummyTask())[1],
    )
    dashboard_app_module.refresh_state.running = False
    dashboard_app_module.refresh_state.task = None

    with TestClient(app) as client:
        response = client.post("/api/refresh?mode=additive")

    assert response.status_code == 202
    payload = response.json()
    assert payload["running"] is True
    assert dashboard_app_module.refresh_state.requested_mode == "additive"
    dashboard_app_module.refresh_state.running = False
    dashboard_app_module.refresh_state.task = None


@pytest.mark.asyncio
async def test_dashboard_html_renders_saved_agenda_section(tmp_path) -> None:
    db_path = tmp_path / "dashboard-html.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    now = datetime.now(UTC)
    async with session_factory() as session:
        session.add(
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
                syllabus_raw_text=None,
                syllabus_parsed={
                    "late_policy": {
                        "accepts_late": True,
                        "default_penalty_per_day": 0.02,
                        "max_late_days": 5,
                    }
                },
                grading_scale=None,
                grade_categories=[{"name": "Homework", "weight": 0.2}],
                late_policy_global=None,
                current_grade_pct=90.23,
                current_letter_grade=None,
                last_scraped_d2l=now,
                last_scraped_external=now,
                last_syllabus_parse=None,
            )
        )
        session.add(
            Assignment(
                id="pearson-phy-221-001-spring-2026-hw-6",
                course_id="phy-221-001-spring-2026",
                title="HW 6",
                description=None,
                type="homework",
                source_platform="pearson_mylab",
                external_url=None,
                available_date=None,
                due_date=now + timedelta(days=2),
                close_date=None,
                grade_category=None,
                grade_weight_pct=None,
                points_possible=None,
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
                raw_scraped_data={"due_text": "APR 2 10:59 PM"},
                last_scraped=now,
            )
        )
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    try:
        with TestClient(app) as client:
            response = client.get("/")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    assert "Today's Focus" in response.text
    assert "Next 7 Days" in response.text
    assert "Course Health" in response.text
    assert "HW 6" in response.text
    assert "Due in 2 days" in response.text
    assert "Homework worth about 20% of course grade" in response.text
    assert "2% penalty per late day" in response.text
