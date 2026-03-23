from datetime import UTC, datetime

from acc.engine.normalizer import (
    assignment_match_key,
    normalize_d2l_snapshot,
    parse_due_text,
    parse_fraction,
    parse_percent,
)
from acc.scrapers.snapshots import (
    D2LContentTopic,
    D2LCourseSnapshot,
    D2LDashboardSnapshot,
    D2LGradeRow,
    D2LGradeSummary,
    D2LToolLink,
    D2LUpcomingEvent,
)


def test_parse_due_text_with_time() -> None:
    parsed = parse_due_text("MAR 22 11:59 PM", datetime(2026, 3, 20, tzinfo=UTC))
    assert parsed == datetime(2026, 3, 23, 4, 59, tzinfo=UTC)


def test_normalize_d2l_snapshot() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
        source_url="https://d2l.oakton.edu/d2l/home",
        courses=[
            D2LCourseSnapshot(
                course_id="189252",
                code="CSC-242-0C1",
                name="Python Data Structures",
                offering_code="10570.202610",
                semester="Spring 2026",
                end_date_text="Ends May 15, 2026 at 11:59 PM",
                home_url="https://d2l.oakton.edu/d2l/home/189252",
                final_calculated_grade=D2LGradeSummary(
                    weight_achieved_text="46.92 / 100",
                    grade_text="46.92 %",
                ),
                tool_links=[
                    D2LToolLink(
                        name="Assignments",
                        url="https://d2l.oakton.edu/d2l/lms/dropbox/dropbox.d2l?ou=189252",
                    )
                ],
                upcoming_events=[
                    D2LUpcomingEvent(
                        title="Programming Exercise 6.7 - Due",
                        due_text="MAR 22 11:59 PM",
                        details_url="https://d2l.oakton.edu/d2l/le/calendar/189252/event/7590085/detailsview#7590085",
                    )
                ],
                grade_rows=[
                    D2LGradeRow(
                        title="Programming Exercises",
                        is_category=True,
                        weight_achieved_text="38.75 / 60",
                        grade_text="64.58 %",
                    ),
                    D2LGradeRow(
                        title="Programming Exercise 6.7",
                        is_category=False,
                        category_title="Programming Exercises",
                        points_text="100 / 100",
                        weight_achieved_text="5 / 5",
                        grade_text="100 %",
                    ),
                ],
            )
        ],
    )

    normalized = normalize_d2l_snapshot(snapshot)

    assert normalized.courses[0].id == "csc-242-0c1-spring-2026"
    assert normalized.courses[0].current_grade_pct == 46.92
    assert normalized.courses[0].external_platform_urls["assignments"].endswith("ou=189252")
    assert [assignment.id for assignment in normalized.assignments] == [
        "csc-242-0c1-spring-2026-7590085"
    ]
    assert normalized.assignments[0].course_id == "csc-242-0c1-spring-2026"
    assert normalized.assignments[0].type == "homework"
    assert normalized.assignments[0].status == "upcoming"
    assert normalized.assignments[0].due_at == datetime(2026, 3, 23, 4, 59, tzinfo=UTC)
    assert normalized.assignments[0].points_earned == 100.0
    assert normalized.assignments[0].grade_pct == 100.0
    assert normalized.assignments[0].grade_category == "Programming Exercises"


def test_grade_helpers() -> None:
    assert parse_fraction("38.75 / 60") == (38.75, 60.0)
    assert parse_percent("46.92 %") == 46.92
    assert assignment_match_key("Programming Exercise 6.7 - Due") == "programming-exercise-6-7"


def test_normalize_course_captures_syllabus_and_external_platform() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 23, 2, 0, tzinfo=UTC),
        source_url="https://d2l.oakton.edu/d2l/home",
        courses=[
            D2LCourseSnapshot(
                course_id="188307",
                code="PHY-221-001",
                name="General Physics I",
                semester="Spring 2026",
                home_url="https://d2l.oakton.edu/d2l/home/188307",
                syllabus_topics=[
                    D2LContentTopic(
                        title="PHY 221-001 Syllabus",
                        url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/5183160/View",
                        module_title="SYLLABUS",
                        content_type="Word Document",
                        extracted_text="OAKTON COLLEGE GENERAL PHYSICS 221-001 COURSE SYLLABUS Spring Semester 2026",
                    )
                ],
                external_tools=[
                    D2LContentTopic(
                        title="Pearson eText",
                        url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/5072360/View",
                        module_title="Mastering",
                        content_type="External Learning Tool",
                        launch_url="https://plus.pearson.com/",
                    ),
                    D2LContentTopic(
                        title="Mastering Assignments",
                        url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/5072361/View",
                        module_title="Mastering",
                        content_type="External Learning Tool",
                        launch_url="https://mlm.pearson.com/",
                    ),
                ],
            )
        ],
    )

    normalized = normalize_d2l_snapshot(snapshot)

    assert normalized.courses[0].syllabus_raw_text == (
        "OAKTON COLLEGE GENERAL PHYSICS 221-001 COURSE SYLLABUS Spring Semester 2026"
    )
    assert normalized.courses[0].external_platform == "pearson_mylab"
    assert normalized.courses[0].external_platform_url == "https://mlm.pearson.com/"
    assert normalized.courses[0].external_platform_urls["mastering-assignments"] == (
        "https://mlm.pearson.com/"
    )
