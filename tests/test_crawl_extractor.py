from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from acc.ai.crawl_extractor import (
    CrawlExtractor,
    RuleExtractionResult,
    build_assignment_chunks,
    read_artifact_text,
)
from acc.config import Settings
from acc.scrapers.snapshots import CrawlArtifact, CrawlCourseSnapshot, CrawlSnapshot


def test_rule_extraction_accepts_null_grading_scale_and_lists() -> None:
    parsed = RuleExtractionResult.model_validate(
        {
            "grading_scale": None,
            "grade_categories": None,
            "notes": None,
        }
    )
    assert parsed.grading_scale == {}
    assert parsed.grade_categories == []
    assert parsed.notes == []


class FakeExtractionClient:
    def __init__(self) -> None:
        self.assignment_calls = 0
        self.rule_calls = 0

    async def complete_json(self, prompt: str) -> str:
        if "grade_categories" in prompt and "late_policy" in prompt and "assignments" not in prompt.splitlines()[0]:
            self.rule_calls += 1
            return json.dumps(
                {
                    "course_code": "CSC-242-0C1",
                    "course_name": "Python Data Structures",
                    "grade_categories": [
                        {"name": "Programming Exercises", "weight": 0.6, "notes": None},
                        {"name": "Quizzes", "weight": 0.4, "notes": None},
                    ],
                    "grading_scale": {"A": [93, 100]},
                    "late_policy": {
                        "raw_text": "2% per day",
                        "accepts_late": True,
                        "default_penalty_per_day": 0.02,
                        "max_late_days": 5,
                    },
                    "notes": [],
                }
            )

        self.assignment_calls += 1
        return json.dumps(
            {
                "assignments": [
                    {
                        "title": "Programming Exercise 7.2",
                        "assignment_type": "homework",
                        "source_platform": "d2l",
                        "grade_category": "Programming Exercises",
                        "due_at": "2026-03-20T23:59:00-05:00",
                        "due_on": None,
                        "due_text": "MAR 20 11:59 PM",
                        "weight_pct": 5.0,
                        "points_possible": 100.0,
                        "points_earned": 100.0,
                        "grade_pct": 100.0,
                        "submitted": True,
                        "graded": True,
                        "optional": False,
                        "extra_credit": False,
                        "counts_toward_grade": True,
                        "status": "graded",
                        "rationale": "Grade row and announcement title align.",
                        "evidence_spans": [
                            {
                                "artifact_id": "grades-page",
                                "quote": "Programming Exercise 7.2 100 / 100",
                            },
                            {
                                "artifact_id": "announcement-page",
                                "quote": "Programming Exercise 7.2",
                            },
                        ],
                        "evidence_artifact_ids": ["grades-page", "announcement-page"],
                        "notes": ["Matched across D2L artifacts"],
                    },
                    {
                        "title": "Unit 7 Reviewing the Basics Quiz",
                        "assignment_type": "exam",
                        "source_platform": "cengage_mindtap",
                        "grade_category": "Quizzes",
                        "due_at": "2099-03-29T23:59:00-05:00",
                        "due_on": None,
                        "due_text": "MAR 29 11:59 PM",
                        "weight_pct": None,
                        "points_possible": 10.0,
                        "points_earned": None,
                        "grade_pct": None,
                        "submitted": False,
                        "graded": False,
                        "optional": False,
                        "extra_credit": False,
                        "counts_toward_grade": True,
                        "status": "upcoming",
                        "evidence_artifact_ids": ["cengage-row"],
                        "notes": [],
                    },
                    {
                        "title": "Programming Exercise 6.7",
                        "assignment_type": "homework",
                        "source_platform": "cengage_mindtap",
                        "grade_category": "Midterm Exam",
                        "due_at": "2026-03-10T23:59:00-05:00",
                        "due_on": None,
                        "due_text": "Mar 10 11:59 PM",
                        "weight_pct": 10.0,
                        "points_possible": 100.0,
                        "points_earned": None,
                        "grade_pct": None,
                        "submitted": True,
                        "graded": False,
                        "optional": False,
                        "extra_credit": False,
                        "counts_toward_grade": True,
                        "status": "submitted",
                        "evidence_artifact_ids": ["cengage-midterm"],
                        "notes": [],
                    },
                ]
            }
        )


class TimeoutSplittingClient:
    async def complete_json(self, prompt: str) -> str:
        artifact_count = prompt.count("artifact_id:")
        if "grade_categories" in prompt and "assignments" not in prompt.splitlines()[0]:
            if artifact_count > 2:
                raise TimeoutError("timed out")
            return json.dumps(
                {
                    "course_code": "CSC-242-0C1",
                    "course_name": "Python Data Structures",
                    "grade_categories": [
                        {"name": "Homework", "weight": 0.5, "notes": None},
                        {"name": "Quizzes", "weight": 0.5, "notes": None},
                    ],
                    "grading_scale": {"A": [93, 100]},
                    "late_policy": None,
                    "notes": [],
                }
            )
        if artifact_count > 2:
            raise TimeoutError("timed out")
        assignments = []
        if "Alpha" in prompt:
            assignments.append(
                {
                    "title": "Alpha Homework",
                    "assignment_type": "homework",
                    "source_platform": "d2l",
                    "grade_category": "Homework",
                    "due_at": "2026-03-20T23:59:00-05:00",
                    "due_on": None,
                    "due_text": "Mar 20 11:59 PM",
                    "weight_pct": None,
                    "points_possible": 10.0,
                    "points_earned": 10.0,
                    "grade_pct": 100.0,
                    "submitted": True,
                    "graded": True,
                    "optional": False,
                    "extra_credit": False,
                    "counts_toward_grade": True,
                    "status": "graded",
                    "evidence_artifact_ids": ["alpha"],
                    "notes": [],
                }
            )
        if "Beta" in prompt:
            assignments.append(
                {
                    "title": "Beta Quiz",
                    "assignment_type": "exam",
                    "source_platform": "d2l",
                    "grade_category": "Quizzes",
                    "due_at": "2026-03-21T23:59:00-05:00",
                    "due_on": None,
                    "due_text": "Mar 21 11:59 PM",
                    "weight_pct": None,
                    "points_possible": 10.0,
                    "points_earned": 8.0,
                    "grade_pct": 80.0,
                    "submitted": True,
                    "graded": True,
                    "optional": False,
                    "extra_credit": False,
                    "counts_toward_grade": True,
                    "status": "graded",
                    "evidence_artifact_ids": ["beta"],
                    "notes": [],
                }
            )
        if "Gamma" in prompt:
            assignments.append(
                {
                    "title": "Gamma Homework",
                    "assignment_type": "homework",
                    "source_platform": "cengage_mindtap",
                    "grade_category": "Homework",
                    "due_at": "2026-03-22T23:59:00-05:00",
                    "due_on": None,
                    "due_text": "Mar 22 11:59 PM",
                    "weight_pct": None,
                    "points_possible": 10.0,
                    "points_earned": 9.0,
                    "grade_pct": 90.0,
                    "submitted": True,
                    "graded": True,
                    "optional": False,
                    "extra_credit": False,
                    "counts_toward_grade": True,
                    "status": "graded",
                    "evidence_artifact_ids": ["gamma"],
                    "notes": [],
                }
            )
        return json.dumps({"assignments": assignments})


def write_artifact(tmp_path: Path, name: str, text: str) -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_build_assignment_chunks_includes_d2l_full_schedule_pages(tmp_path) -> None:
    artifact = CrawlArtifact(
        id="calendar-full-schedule",
        course_id="csc-242-0c1-spring-2026",
        course_code="CSC-242-0C1",
        source_platform="d2l",
        artifact_type="page",
        page_kind="tool_calendar_full_schedule",
        title="Calendar - Full Schedule",
        url="https://d2l.example/calendar",
        fetched_at=datetime.now(UTC),
        text_path=write_artifact(tmp_path, "calendar-full-schedule.txt", "Programming Exercise 7.2 Due"),
    )

    chunks = build_assignment_chunks([artifact])

    assert len(chunks) == 1
    assert chunks[0][0].id == "calendar-full-schedule"


@pytest.mark.asyncio
async def test_crawl_extractor_merges_assignments_and_calculates_current_grade(tmp_path) -> None:
    artifacts_dir = tmp_path / "crawl-artifacts"
    artifacts_dir.mkdir()
    snapshot_path = tmp_path / "crawl-snapshot.json"
    extracted_path = tmp_path / "crawl-extracted.json"

    crawl_snapshot = CrawlSnapshot(
        fetched_at=datetime(2026, 3, 23, 18, 11, 42, tzinfo=UTC),
        artifacts_dir=str(artifacts_dir),
        courses=[
            CrawlCourseSnapshot(
                course_id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                artifact_count=3,
            )
        ],
        artifacts=[
            CrawlArtifact(
                id="grades-page",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="tool_grades",
                title="Grades",
                url="https://d2l.example/grades",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "grades.txt", "Programming Exercise 7.2 100 / 100"),
            ),
            CrawlArtifact(
                id="announcement-page",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="announcement_detail",
                title="What to do the 10th week?",
                url="https://d2l.example/announcement",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "announcement.txt", "Programming Exercise 7.2"),
            ),
            CrawlArtifact(
                id="cengage-row",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="cengage_mindtap",
                artifact_type="fragment",
                page_kind="external_assignment_row",
                title="Unit 7 Reviewing the Basics Quiz",
                parent_url="https://ng.cengage.com",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "row.txt", "Unit 7 Reviewing the Basics Quiz"),
            ),
        ],
    )
    snapshot_path.write_text(crawl_snapshot.model_dump_json(indent=2), encoding="utf-8")

    settings = Settings(
        crawl_snapshot_path=snapshot_path,
        crawl_extracted_path=extracted_path,
        crawl_artifacts_dir=artifacts_dir,
    )
    extractor = CrawlExtractor(settings, client=FakeExtractionClient())

    snapshot = await extractor.extract()

    assert len(snapshot.courses) == 1
    course = snapshot.courses[0]
    assert course.course_code == "CSC-242-0C1"
    assert len(course.assignments) == 3
    assert course.current_grade_pct == 100.0
    assert "Calculated from graded assignments that are already due." in course.calculation_notes
    assert course.grade_categories[0].name == "Programming Exercises"
    assignment_provenance = [
        event for event in course.provenance_events if event.get("stage") == "llm_crawl_extract_assignments"
    ]
    assert assignment_provenance
    detail = assignment_provenance[0]["detail"]
    assert isinstance(detail, dict)
    evidence_rows = detail.get("assignment_evidence")
    assert isinstance(evidence_rows, list)
    assert any(row.get("rationale") == "Grade row and announcement title align." for row in evidence_rows)

    saved = await extractor.save_snapshot()
    assert extracted_path.exists()
    assert saved.courses[0].course_code == "CSC-242-0C1"


@pytest.mark.asyncio
async def test_crawl_extractor_splits_large_chunks_after_timeout(tmp_path) -> None:
    artifacts_dir = tmp_path / "crawl-artifacts"
    artifacts_dir.mkdir()
    snapshot_path = tmp_path / "crawl-snapshot.json"
    extracted_path = tmp_path / "crawl-extracted.json"

    crawl_snapshot = CrawlSnapshot(
        fetched_at=datetime(2026, 3, 23, 18, 11, 42, tzinfo=UTC),
        artifacts_dir=str(artifacts_dir),
        courses=[
            CrawlCourseSnapshot(
                course_id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                artifact_count=3,
            )
        ],
        artifacts=[
            CrawlArtifact(
                id="alpha",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="tool_grades",
                title="Alpha",
                url="https://d2l.example/alpha",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "alpha.txt", "Alpha"),
            ),
            CrawlArtifact(
                id="beta",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="tool_assignments",
                title="Beta",
                url="https://d2l.example/beta",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "beta.txt", "Beta"),
            ),
            CrawlArtifact(
                id="gamma",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="cengage_mindtap",
                artifact_type="fragment",
                page_kind="external_assignment_row",
                title="Gamma",
                url="https://ng.cengage.com/gamma",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "gamma.txt", "Gamma"),
            ),
        ],
    )
    snapshot_path.write_text(crawl_snapshot.model_dump_json(indent=2), encoding="utf-8")

    settings = Settings(
        crawl_snapshot_path=snapshot_path,
        crawl_extracted_path=extracted_path,
        crawl_artifacts_dir=artifacts_dir,
    )

    course = await CrawlExtractor(settings, client=TimeoutSplittingClient()).extract()

    assert len(course.courses) == 1
    assert len(course.courses[0].assignments) == 3
    assert [category.name for category in course.courses[0].grade_categories] == ["Homework", "Quizzes"]


@pytest.mark.asyncio
async def test_crawl_extractor_save_snapshot_merges_incremental_course_runs(tmp_path) -> None:
    artifacts_dir = tmp_path / "crawl-artifacts"
    artifacts_dir.mkdir()
    snapshot_path = tmp_path / "crawl-snapshot.json"
    extracted_path = tmp_path / "crawl-extracted.json"

    existing = {
        "extracted_at": "2026-03-23T18:20:00Z",
        "source_snapshot_path": str(snapshot_path),
        "source_artifacts_dir": str(artifacts_dir),
        "courses": [
            {
                "course_id": "phy-221-001-spring-2026",
                "course_code": "PHY-221-001",
                "course_name": "General Physics I",
                "assignments": [],
                "grade_categories": [],
                "grading_scale": {},
                "late_policy": None,
                "current_grade_pct": None,
                "calculation_notes": [],
                "artifact_count": 0,
            }
        ],
    }
    extracted_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")

    crawl_snapshot = CrawlSnapshot(
        fetched_at=datetime(2026, 3, 23, 18, 11, 42, tzinfo=UTC),
        artifacts_dir=str(artifacts_dir),
        courses=[
            CrawlCourseSnapshot(
                course_id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                artifact_count=1,
            )
        ],
        artifacts=[
            CrawlArtifact(
                id="grades-page",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="tool_grades",
                title="Grades",
                url="https://d2l.example/grades",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "grades-2.txt", "Programming Exercise 7.2 100 / 100"),
            )
        ],
    )
    snapshot_path.write_text(crawl_snapshot.model_dump_json(indent=2), encoding="utf-8")

    settings = Settings(
        crawl_snapshot_path=snapshot_path,
        crawl_extracted_path=extracted_path,
        crawl_artifacts_dir=artifacts_dir,
    )
    saved = await CrawlExtractor(settings, client=FakeExtractionClient()).save_snapshot(
        course_id="csc-242-0c1-spring-2026"
    )

    assert sorted(course.course_id for course in saved.courses) == [
        "csc-242-0c1-spring-2026",
        "phy-221-001-spring-2026",
    ]


def test_read_artifact_text_decodes_embedded_d2l_html(tmp_path) -> None:
    html_path = tmp_path / "announcement.html"
    html_path.write_text(
        '<html><body><d2l-html-block html="&lt;ul&gt;&lt;li&gt;Programming Exercise 7.1&lt;/li&gt;'
        '&lt;li&gt;Programming Exercise 7.2&lt;/li&gt;&lt;/ul&gt;"></d2l-html-block></body></html>',
        encoding="utf-8",
    )
    artifact = CrawlArtifact(
        id="announcement",
        course_id="csc-242-0c1-spring-2026",
        course_code="CSC-242-0C1",
        source_platform="d2l",
        artifact_type="page",
        page_kind="announcement_detail",
        title="Announcement",
        url="https://d2l.example/announcement",
        fetched_at=datetime.now(UTC),
        html_path=str(html_path),
    )

    text = read_artifact_text(artifact)

    assert "Programming Exercise 7.1" in text
    assert "Programming Exercise 7.2" in text
    assert "Mar 29, 2026 11:59 PM" not in text


def test_read_artifact_text_preserves_fuzzydate_titles(tmp_path) -> None:
    html_path = tmp_path / "content.html"
    html_path.write_text(
        '<html><body>'
        '<h3>Programming Exercise 7.2 - Due</h3>'
        '<abbr class="d2l-fuzzydate" title="Mar 29, 2026 11:59 PM">March 29 at 11:59 PM</abbr>'
        '</body></html>',
        encoding="utf-8",
    )
    artifact = CrawlArtifact(
        id="content",
        course_id="csc-242-0c1-spring-2026",
        course_code="CSC-242-0C1",
        source_platform="d2l",
        artifact_type="page",
        page_kind="tool_content",
        title="Content",
        url="https://d2l.example/content",
        fetched_at=datetime.now(UTC),
        html_path=str(html_path),
    )

    text = read_artifact_text(artifact)

    assert "March 29 at 11:59 PM (Mar 29, 2026 11:59 PM)" in text


@pytest.mark.asyncio
async def test_crawl_extractor_keeps_zero_scores_excluded_from_extracted_course_grade(tmp_path) -> None:
    artifacts_dir = tmp_path / "crawl-artifacts"
    artifacts_dir.mkdir()
    snapshot_path = tmp_path / "crawl-snapshot.json"
    extracted_path = tmp_path / "crawl-extracted.json"

    class ZeroPlaceholderClient:
        async def complete_json(self, prompt: str) -> str:
            if "grade_categories" in prompt and "assignments" not in prompt.splitlines()[0]:
                return json.dumps(
                    {
                        "course_code": "CSC-242-0C1",
                        "course_name": "Python Data Structures",
                        "grade_categories": [{"name": "Homework", "weight": 1.0, "notes": None}],
                        "grading_scale": {},
                        "late_policy": None,
                        "notes": [],
                    }
                )
            return json.dumps(
                {
                    "assignments": [
                        {
                            "title": "Programming Exercise 7.1",
                            "assignment_type": "lab",
                            "source_platform": "cengage_mindtap",
                            "grade_category": "Homework",
                            "due_at": None,
                            "due_on": None,
                            "due_text": None,
                            "weight_pct": None,
                            "points_possible": 100.0,
                            "points_earned": 0.0,
                            "grade_pct": 0.0,
                            "submitted": False,
                            "graded": False,
                            "optional": False,
                            "extra_credit": False,
                            "counts_toward_grade": True,
                            "status": "available",
                            "evidence_artifact_ids": ["row"],
                            "notes": [],
                        },
                        {
                            "title": "Programming Exercise 6.1",
                            "assignment_type": "lab",
                            "source_platform": "cengage_mindtap",
                            "grade_category": "Homework",
                            "due_at": None,
                            "due_on": None,
                            "due_text": None,
                            "weight_pct": None,
                            "points_possible": 100.0,
                            "points_earned": 100.0,
                            "grade_pct": 100.0,
                            "submitted": True,
                            "graded": True,
                            "optional": False,
                            "extra_credit": False,
                            "counts_toward_grade": True,
                            "status": "graded",
                            "evidence_artifact_ids": ["graded-row"],
                            "notes": [],
                        },
                    ]
                }
            )

    crawl_snapshot = CrawlSnapshot(
        fetched_at=datetime(2026, 3, 23, 18, 11, 42, tzinfo=UTC),
        artifacts_dir=str(artifacts_dir),
        courses=[
            CrawlCourseSnapshot(
                course_id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                artifact_count=1,
            )
        ],
        artifacts=[
            CrawlArtifact(
                id="row",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="cengage_mindtap",
                artifact_type="fragment",
                page_kind="external_assignment_row",
                title="Programming Exercise 7.1",
                fetched_at=datetime.now(UTC),
                text_path=write_artifact(tmp_path, "row-zero.txt", "Programming Exercise 7.1"),
            )
        ],
    )
    snapshot_path.write_text(crawl_snapshot.model_dump_json(indent=2), encoding="utf-8")

    settings = Settings(
        crawl_snapshot_path=snapshot_path,
        crawl_extracted_path=extracted_path,
        crawl_artifacts_dir=artifacts_dir,
    )

    snapshot = await CrawlExtractor(settings, client=ZeroPlaceholderClient()).extract()

    course = snapshot.courses[0]
    available = next(assignment for assignment in course.assignments if assignment.title == "Programming Exercise 7.1")
    assert available.grade_pct == 0.0
    assert available.points_earned == 0.0
    assert course.current_grade_pct == 100.0


@pytest.mark.asyncio
async def test_crawl_extractor_parses_d2l_content_due_dates_without_model_help(tmp_path) -> None:
    artifacts_dir = tmp_path / "crawl-artifacts"
    artifacts_dir.mkdir()
    snapshot_path = tmp_path / "crawl-snapshot.json"
    extracted_path = tmp_path / "crawl-extracted.json"

    html_path = tmp_path / "content.html"
    html_path.write_text(
        '<html><body>'
        '<h3>Programming Exercise 7.2: Unit 7 Code: Unit 7: Stacks - Due</h3>'
        '<abbr class="d2l-fuzzydate" title="Mar 29, 2026 11:59 PM">March 29 at 11:59 PM</abbr>'
        '</body></html>',
        encoding="utf-8",
    )

    class EmptyClient:
        async def complete_json(self, prompt: str) -> str:
            if "grade_categories" in prompt and "assignments" not in prompt.splitlines()[0]:
                return json.dumps(
                    {
                        "course_code": "CSC-242-0C1",
                        "course_name": "Python Data Structures",
                        "grade_categories": [],
                        "grading_scale": {},
                        "late_policy": None,
                        "notes": [],
                    }
                )
            return json.dumps({"assignments": []})

    crawl_snapshot = CrawlSnapshot(
        fetched_at=datetime(2026, 3, 23, 18, 11, 42, tzinfo=UTC),
        artifacts_dir=str(artifacts_dir),
        courses=[
            CrawlCourseSnapshot(
                course_id="csc-242-0c1-spring-2026",
                code="CSC-242-0C1",
                name="Python Data Structures",
                artifact_count=1,
            )
        ],
        artifacts=[
            CrawlArtifact(
                id="content",
                course_id="csc-242-0c1-spring-2026",
                course_code="CSC-242-0C1",
                source_platform="d2l",
                artifact_type="page",
                page_kind="tool_content",
                title="Content",
                fetched_at=datetime.now(UTC),
                html_path=str(html_path),
            )
        ],
    )
    snapshot_path.write_text(crawl_snapshot.model_dump_json(indent=2), encoding="utf-8")

    settings = Settings(
        crawl_snapshot_path=snapshot_path,
        crawl_extracted_path=extracted_path,
        crawl_artifacts_dir=artifacts_dir,
    )

    snapshot = await CrawlExtractor(settings, client=EmptyClient()).extract()

    assignment = snapshot.courses[0].assignments[0]
    assert assignment.title.startswith("Programming Exercise 7.2")
    assert assignment.due_at == "2026-03-29T23:59:00-05:00"
    assert assignment.source_platforms == ["d2l"]
