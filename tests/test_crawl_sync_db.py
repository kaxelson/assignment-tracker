from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from acc.config import Settings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.ai.crawl_extractor import (
    CrawlExtractionSnapshot,
    ExtractedAssignment,
    ExtractedCourseResult,
    ExtractedGradeCategory,
    ExtractedLatePolicy,
)
from acc.db.models import Assignment, Base, Course, ProvenanceEvent
from acc.db.repository import Repository
from acc.scrapers.snapshots import CrawlArtifact, CrawlCourseSnapshot, CrawlSnapshot


@pytest.mark.asyncio
async def test_sync_crawl_extraction_replaces_assignments_and_sets_course_rules(tmp_path) -> None:
    db_path = tmp_path / "crawl-sync.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    extracted_at = datetime(2026, 3, 23, 12, 0, tzinfo=UTC)
    snapshot = CrawlExtractionSnapshot(
        extracted_at=extracted_at,
        source_snapshot_path=str(tmp_path / "crawl-snapshot.json"),
        source_artifacts_dir=str(tmp_path / "artifacts"),
        courses=[
            ExtractedCourseResult(
                course_id="cis-156-spring-2026",
                course_code="CIS-156",
                course_name="Data Structures",
                assignments=[
                    ExtractedAssignment(
                        title="Linked List Lab",
                        due_at="2026-03-24T04:59:00+00:00",
                        status="upcoming",
                        points_possible=10.0,
                        source_platforms=["d2l"],
                    )
                ],
                grade_categories=[ExtractedGradeCategory(name="Labs", weight=40.0)],
                grading_scale={"A": [90.0, 100.0]},
                late_policy=ExtractedLatePolicy(raw_text="No late labs.", accepts_late=False),
                current_grade_pct=88.5,
                provenance_events=[
                    {
                        "stage": "llm_crawl_extract_rules",
                        "course_id": "cis-156-spring-2026",
                        "detail": {"note": "synthetic test event"},
                    }
                ],
            )
        ],
    )

    manifest = CrawlSnapshot(
        fetched_at=extracted_at,
        artifacts_dir=str(tmp_path / "a"),
        courses=[CrawlCourseSnapshot(course_id="cis-156-spring-2026", code="CIS-156", name="Data Structures")],
        artifacts=[
            CrawlArtifact(
                id="x",
                course_id="cis-156-spring-2026",
                course_code="CIS-156",
                source_platform="d2l",
                artifact_type="page",
                page_kind="course_home",
                title="Home",
                url="https://d2l.oakton.edu/d2l/home/156001",
                fetched_at=extracted_at,
                html_path=None,
                text_path=None,
                screenshot_path=None,
            )
        ],
    )

    async with session_factory() as session:
        session.add(
            Assignment(
                id="cis-156-spring-2026-stale",
                course_id="cis-156-spring-2026",
                title="Old row",
                description=None,
                type="homework",
                source_platform="d2l",
                external_url=None,
                available_date=None,
                due_date=None,
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
                last_scraped=extracted_at,
            )
        )
        await session.commit()

    async with session_factory() as session:
        repository = Repository(session)
        summary = await repository.sync_crawl_extraction_snapshot(snapshot, crawl_manifest=manifest)
        await session.commit()

        course = await session.get(Course, "cis-156-spring-2026")
        assignments = (await session.scalars(select(Assignment).order_by(Assignment.id.asc()))).all()
        prov_rows = (await session.scalars(select(ProvenanceEvent))).all()

    await engine.dispose()

    assert summary.courses_upserted == 1
    assert summary.assignments_upserted == 1
    assert summary.assignments_deleted == 1
    assert course is not None
    assert course.d2l_course_id == "156001"
    assert "d2l/home/156001" in course.d2l_url
    assert course.current_grade_pct is None
    assert course.late_policy_global == "No late labs.\nAccepts late submissions: False"
    assert len(course.grade_categories or []) == 1
    assert len(assignments) == 1
    assert assignments[0].title == "Linked List Lab"
    assert assignments[0].points_possible == 10.0
    assert assignments[0].external_url == "https://d2l.oakton.edu/d2l/home/156001"
    assert len(prov_rows) == 1
    assert prov_rows[0].stage == "llm_crawl_extract_rules"
    assert prov_rows[0].course_id == "cis-156-spring-2026"
    assert prov_rows[0].detail == {"note": "synthetic test event"}


@pytest.mark.asyncio
async def test_run_refresh_pipeline_uses_crawl_sync(monkeypatch, tmp_path) -> None:
    from acc.main import run_refresh_pipeline

    settings = Settings(
        playwright_browsers_path=tmp_path / "playwright",
        runtime_tmp_dir=tmp_path / "tmp",
        openai_api_key=SecretStr("sk-test"),
    )
    calls: list[str] = []

    async def fake_run_d2l_login(
        settings_arg: Settings,
        force: bool,
        on_progress: object | None = None,
    ) -> int:
        calls.append("login")
        return 0

    async def fake_run_d2l_snapshot(
        settings_arg: Settings,
        limit: int | None,
        on_progress: object | None = None,
    ) -> int:
        calls.append("snapshot")
        return 0

    async def fake_run_crawl_snapshot(
        settings_arg: Settings,
        *,
        course_id: str | None,
        limit_courses: int | None,
        max_external_details: int | None,
        capture_screenshots: bool,
        on_progress: object | None = None,
    ) -> int:
        calls.append("crawl-snapshot")
        return 0

    async def fake_run_crawl_extract(
        settings_arg: Settings,
        *,
        course_id: str | None,
        on_progress: object | None = None,
    ) -> int:
        calls.append("crawl-extract")
        return 0

    async def fake_run_crawl_sync_db(
        settings_arg: Settings,
        on_progress: object | None = None,
    ) -> int:
        calls.append("crawl-sync-db")
        return 0

    monkeypatch.setattr("acc.main.run_d2l_login", fake_run_d2l_login)
    monkeypatch.setattr("acc.main.run_d2l_snapshot", fake_run_d2l_snapshot)
    monkeypatch.setattr("acc.main.run_crawl_snapshot", fake_run_crawl_snapshot)
    monkeypatch.setattr("acc.main.run_crawl_extract", fake_run_crawl_extract)
    monkeypatch.setattr("acc.main.run_crawl_sync_db", fake_run_crawl_sync_db)

    result = await run_refresh_pipeline(settings)

    assert calls == ["login", "snapshot", "crawl-snapshot", "crawl-extract", "crawl-sync-db"]
    assert result["crawl_synced"] is True
