import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from acc.db.models import Base, Course, ProvenanceEvent
from acc.db.repository import Repository


@pytest.mark.asyncio
async def test_record_and_list_provenance_events(tmp_path) -> None:
    db_path = tmp_path / "prov.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", future=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        session.add(
            Course(
                id="c1",
                code="C1",
                name="Course",
                instructor=None,
                d2l_course_id="1",
                d2l_url="https://d2l.example.edu/d2l/home/1",
                semester="Spring 2026",
                external_platform=None,
                external_platform_url=None,
                textbook=None,
                syllabus_raw_text=None,
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

    async with session_factory() as session:
        repo = Repository(session)
        ev = await repo.record_provenance_event(
            stage="llm_extract",
            course_id="c1",
            source_url="https://d2l.example.edu/x",
            artifact_ref=".state/crawl-artifacts/run/a.html",
            text_preview="preview",
            detail={"rationale": "model said so", "follow": [1, 2]},
        )
        assert isinstance(ev, ProvenanceEvent)
        await session.commit()

    async with session_factory() as session:
        repo = Repository(session)
        rows = await repo.list_provenance_events(course_id="c1", limit=10)
        assert len(rows) == 1
        assert rows[0].stage == "llm_extract"
        assert rows[0].detail["rationale"] == "model said so"

    await engine.dispose()
