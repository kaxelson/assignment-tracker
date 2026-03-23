from datetime import UTC, datetime

from acc.scrapers.external import (
    build_external_assignment_id,
    clean_cengage_title,
    parse_cengage_assignment,
    parse_estimated_minutes,
    parse_pearson_assignment,
    parse_pearson_due_text,
    parse_points,
)


def test_parse_points_handles_ungraded_and_scored_rows() -> None:
    assert parse_points("100/100points") == (100.0, 100.0)
    assert parse_points("--/10 points") == (None, 10.0)


def test_clean_cengage_title_removes_status_suffix() -> None:
    assert clean_cengage_title("Programming Exercise 1.2 Not started") == "Programming Exercise 1.2"


def test_parse_cengage_assignment_extracts_status_and_grade() -> None:
    assignment = parse_cengage_assignment(
        course_id="csc-242-0c1-spring-2026",
        title="Programming Exercise 1.1 Submitted",
        row_text="Programming Exercise 1.1 Submitted COUNTS TOWARDS GRADE 100/100 points",
        points_text="100/100 points",
        activity_class="activity done activity-activityType-2",
        external_url="https://ng.cengage.com/activity/2733512518",
        description="Basic Python syntax practice",
    )

    assert assignment is not None
    assert assignment.title == "Programming Exercise 1.1"
    assert assignment.status == "completed"
    assert assignment.grade_pct == 100.0
    assert assignment.points_possible == 100.0


def test_parse_pearson_due_text_converts_from_local_timezone() -> None:
    due_at = parse_pearson_due_text("03/25/2026 10:59 PM", timezone="America/Chicago")
    assert due_at == datetime(2026, 3, 26, 3, 59, tzinfo=UTC)


def test_parse_pearson_assignment_extracts_due_and_minutes() -> None:
    assignment = parse_pearson_assignment(
        course_id="phy-221-001-spring-2026",
        title="HW 6",
        row_text="HW 6 03/25/2026 10:59 PM 6 items, about 25-30 minutes",
        external_url="https://session.physics-mastering.pearson.com/myct/item/123",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.status == "upcoming"
    assert assignment.due_text == "03/25/2026 10:59 PM"
    assert assignment.due_at == datetime(2026, 3, 26, 3, 59, tzinfo=UTC)
    assert assignment.estimated_minutes == 28


def test_parse_estimated_minutes_averages_ranges() -> None:
    assert parse_estimated_minutes("6 items, about 25-30 minutes") == 28


def test_build_external_assignment_id_keeps_similar_urls_distinct() -> None:
    first = build_external_assignment_id(
        "pearson",
        "phy-221-001-spring-2026",
        "https://session.physics-mastering.pearson.com/myct/item/123",
    )
    second = build_external_assignment_id(
        "pearson",
        "phy-221-001-spring-2026",
        "https://session.physics-mastering.pearson.com/myct/item/456",
    )

    assert first != second
