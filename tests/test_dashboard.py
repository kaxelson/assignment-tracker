from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.dashboard.app import app
from acc.db.engine import get_session
from acc.db.models import AgendaEntry, Assignment, Base, Course


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
                syllabus_parsed=None,
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
                    grade_pct=95.0,
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
                    grade_pct=100.0,
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
        session.add(
            AgendaEntry(
                assignment_id="cis-156-spring-2026-linked-list-lab",
                agenda_date=(now + timedelta(days=1)).date(),
                planned_minutes=30,
                priority_score=6.5,
                notes="Auto-planned for due tomorrow",
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

    assert payload["database_ready"] is True
    assert payload["summary"] == {
        "course_count": 1,
        "assignment_count": 2,
        "upcoming_count": 1,
    }
    assert payload["courses"] == [
        {
            "id": "cis-156-spring-2026",
            "code": "CIS-156",
            "name": "Data Structures in Python",
            "semester": "Spring 2026",
            "current_grade_pct": 94.5,
            "assignment_count": 2,
            "upcoming_count": 1,
            "external_platform": None,
            "d2l_url": "https://d2l.oakton.edu/d2l/home/156001",
        }
    ]
    assert payload["upcoming_assignments"][0]["title"] == "Linked List Lab"
    assert payload["upcoming_assignments"][0]["course_code"] == "CIS-156"
    assert payload["upcoming_assignments"][0]["due_label"] == "APR 1 11:59 PM"
    assert payload["agenda_entries"] == [
        {
            "agenda_date": (now + timedelta(days=1)).date().isoformat(),
            "planned_minutes": 30,
            "priority_score": 6.5,
            "assignment_id": "cis-156-spring-2026-linked-list-lab",
            "assignment_title": "Linked List Lab",
            "course_code": "CIS-156",
            "notes": "Auto-planned for due tomorrow",
        }
    ]
    assert payload["agenda_days"] == [
        {
            "agenda_date": (now + timedelta(days=1)).date().isoformat(),
            "total_minutes": 30,
            "entry_count": 1,
            "items": payload["agenda_entries"],
        }
    ]


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
    }
    assert payload["courses"][0]["assignment_count"] == 1
    assert payload["courses"][0]["upcoming_count"] == 1
    assert payload["upcoming_assignments"][0]["title"] == "Programming Exercise 6.7"
    assert payload["upcoming_assignments"][0]["due_label"] == "MAR 23 11:59 PM"
    assert payload["agenda_entries"] == []
    assert payload["agenda_days"] == []


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
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
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
        session.add_all(
            [
                AgendaEntry(
                    assignment_id="pearson-phy-221-001-spring-2026-hw-6",
                    agenda_date=(now + timedelta(days=1)).date(),
                    planned_minutes=30,
                    priority_score=5.5,
                    notes="Auto-planned for due 2026-04-02",
                ),
                AgendaEntry(
                    assignment_id="pearson-phy-221-001-spring-2026-hw-6",
                    agenda_date=(now + timedelta(days=1)).date(),
                    planned_minutes=15,
                    priority_score=5.0,
                    notes=None,
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
            response = client.get("/")
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()

    assert response.status_code == 200
    assert "Saved Agenda" in response.text
    assert "HW 6" in response.text
    assert "45 min" in response.text
    assert "1 items planned" in response.text
    assert "45 min total" in response.text
