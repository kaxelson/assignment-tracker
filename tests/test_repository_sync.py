from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.db.models import Assignment, Base, Course
from acc.db.repository import Repository
from acc.engine.normalizer import NormalizedAssignment, NormalizedCourse, NormalizedSnapshot
from acc.scrapers.snapshots import ExternalAssignmentSnapshot, ExternalCourseSnapshot, ExternalScrapeSnapshot


@pytest.mark.asyncio
async def test_sync_normalized_snapshot_deletes_stale_d2l_assignments(tmp_path) -> None:
    db_path = tmp_path / "sync.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    snapshot = NormalizedSnapshot(
        normalized_at=datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
        source_fetched_at=datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
        courses=[
            NormalizedCourse(
                id="cis-156-spring-2026",
                d2l_course_id="156001",
                code="CIS-156",
                name="Data Structures in Python",
                semester="Spring 2026",
                d2l_url="https://d2l.oakton.edu/d2l/home/156001",
            )
        ],
        assignments=[
            NormalizedAssignment(
                id="cis-156-spring-2026-7590085",
                course_id="cis-156-spring-2026",
                title="Linked List Lab",
                type="lab",
                status="upcoming",
                source_platform="d2l",
                due_at=datetime(2026, 3, 24, 4, 59, tzinfo=UTC),
            )
        ],
    )

    async with session_factory() as session:
        session.add(
            Assignment(
                id="cis-156-spring-2026-stale-row",
                course_id="cis-156-spring-2026",
                title="Old Assignment",
                description=None,
                type="homework",
                source_platform="d2l",
                external_url=None,
                available_date=None,
                due_date=datetime(2026, 3, 20, 23, 59, tzinfo=UTC),
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
                last_scraped=datetime(2026, 3, 20, 23, 59, tzinfo=UTC),
            )
        )
        await session.commit()

    async with session_factory() as session:
        repository = Repository(session)
        summary = await repository.sync_normalized_snapshot(snapshot)
        await session.commit()

        assignments = (
            await session.scalars(select(Assignment).order_by(Assignment.id.asc()))
        ).all()

    await engine.dispose()

    assert summary.assignments_upserted == 1
    assert summary.assignments_deleted == 1
    assert [assignment.id for assignment in assignments] == ["cis-156-spring-2026-7590085"]


@pytest.mark.asyncio
async def test_sync_external_snapshot_updates_courses_and_deletes_stale_external_assignments(
    tmp_path,
) -> None:
    db_path = tmp_path / "external-sync.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

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
                external_platform_url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/5072361/View",
                textbook=None,
                syllabus_raw_text=None,
                syllabus_parsed=None,
                grading_scale=None,
                grade_categories=None,
                late_policy_global=None,
                current_grade_pct=90.23,
                current_letter_grade=None,
                last_scraped_d2l=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
                last_scraped_external=None,
                last_syllabus_parse=None,
            )
        )
        session.add(
            Assignment(
                id="pearson-phy-221-001-spring-2026-stale",
                course_id="phy-221-001-spring-2026",
                title="Old Pearson Item",
                description=None,
                type="homework",
                source_platform="pearson_mylab",
                external_url=None,
                available_date=None,
                due_date=datetime(2026, 3, 18, 3, 59, tzinfo=UTC),
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
                last_scraped=datetime(2026, 3, 18, 3, 59, tzinfo=UTC),
            )
        )
        await session.commit()

    snapshot = ExternalScrapeSnapshot(
        fetched_at=datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
        courses=[
            ExternalCourseSnapshot(
                course_id="phy-221-001-spring-2026",
                source_platform="pearson_mylab",
                launch_url="https://session.physics-mastering.pearson.com/myct/mastering#/",
                title="General Physics I",
            )
        ],
        assignments=[
            ExternalAssignmentSnapshot(
                id="pearson-phy-221-001-spring-2026-hw-6",
                course_id="phy-221-001-spring-2026",
                source_platform="pearson_mylab",
                title="HW 6",
                type="homework",
                status="upcoming",
                external_url="https://session.physics-mastering.pearson.com/myct/item/123",
                due_at=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
                due_text="03/25/2026 10:59 PM",
                estimated_minutes=28,
                raw_source={"row_text": "HW 6 03/25/2026 10:59 PM 6 items, about 25-30 minutes"},
            )
        ],
    )

    async with session_factory() as session:
        repository = Repository(session)
        summary = await repository.sync_external_snapshot(snapshot)
        await session.commit()

        course = await session.get(Course, "phy-221-001-spring-2026")
        assignments = (
            await session.scalars(
                select(Assignment).where(Assignment.course_id == "phy-221-001-spring-2026")
            )
        ).all()

    await engine.dispose()

    assert summary.courses_upserted == 1
    assert summary.assignments_upserted == 1
    assert summary.assignments_deleted == 1
    assert course is not None
    assert course.last_scraped_external == datetime(2026, 3, 23, 1, 0)
    assert [assignment.id for assignment in assignments] == ["pearson-phy-221-001-spring-2026-hw-6"]
