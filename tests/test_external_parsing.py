from datetime import UTC, datetime

from acc.scrapers.external import (
    build_external_assignment_id,
    clean_cengage_title,
    extract_pearson_assignment_identifier,
    is_pearson_url,
    parse_pearson_score_text,
    parse_cengage_assignment,
    parse_estimated_minutes,
    parse_pearson_assignment,
    parse_pearson_assignment_table_row,
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


def test_parse_cengage_assignment_treats_scored_in_progress_rows_as_completed() -> None:
    assignment = parse_cengage_assignment(
        course_id="csc-242-0c1-spring-2026",
        title="Programming Exercise 6.1 In progress",
        row_text="Programming Exercise 6.1 In progress COUNTS TOWARDS GRADE 100/100 points",
        points_text="100/100 points",
        activity_class="activity inprogress activity-activityType-2",
        external_url=None,
        description="Complete this lab assignment in the code editor.",
    )

    assert assignment is not None
    assert assignment.title == "Programming Exercise 6.1"
    assert assignment.status == "completed"
    assert assignment.grade_pct == 100.0


def test_parse_pearson_due_text_converts_from_local_timezone() -> None:
    due_at = parse_pearson_due_text("03/25/2026 10:59 PM", timezone="America/Chicago")
    assert due_at == datetime(2026, 3, 26, 3, 59, tzinfo=UTC)


def test_parse_pearson_due_text_accepts_short_year_and_lowercase_pm() -> None:
    due_at = parse_pearson_due_text("01/26/26 6:00pm", timezone="America/Chicago")
    assert due_at == datetime(2026, 1, 27, 0, 0, tzinfo=UTC)


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


def test_parse_pearson_assignment_extracts_points_when_visible() -> None:
    assignment = parse_pearson_assignment(
        course_id="phy-221-001-spring-2026",
        title="HW 7",
        row_text="HW 7 04/01/2026 11:59 PM 9/10 points",
        external_url="https://session.physics-mastering.pearson.com/myct/item/456",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.points_earned == 9.0
    assert assignment.points_possible == 10.0
    assert assignment.grade_pct == 90.0


def test_parse_pearson_score_text_reads_percent_callback_result() -> None:
    assert parse_pearson_score_text("100%") == (None, None, 100.0)


def test_parse_pearson_assignment_marks_incomplete_rows_in_progress() -> None:
    assignment = parse_pearson_assignment(
        course_id="phy-221-001-spring-2026",
        title="Practice Test",
        row_text="Practice Test 03/25/2026 10:59 PM 2 of 2 incomplete --",
        external_url="https://example.com",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.status == "in_progress"


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


def test_extract_pearson_assignment_identifier_reads_javascript_link() -> None:
    href = "javascript:doHomework(717756881, false, true);"
    assert extract_pearson_assignment_identifier(href) == "717756881"


def test_parse_pearson_assignment_table_row_extracts_due_and_identifier() -> None:
    assignment = parse_pearson_assignment_table_row(
        course_id="mat-251-050-spring-2026",
        title="HW 6.2",
        assignment_kind="Homework",
        row_text="01/26/26 6:00pm Homework HW 6.2 see score for HW 6.2",
        due_text="01/26/26 6:00pm",
        due_class="past due nowrap",
        score_text=None,
        href="javascript:doHomework(717756881, false, true);",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert "717756881" in assignment.id
    assert assignment.title == "HW 6.2"
    assert assignment.type == "homework"
    assert assignment.status == "overdue"
    assert assignment.due_text == "01/26/26 6:00pm"
    assert assignment.due_at == datetime(2026, 1, 27, 0, 0, tzinfo=UTC)


def test_parse_pearson_assignment_table_row_marks_scored_items_completed() -> None:
    assignment = parse_pearson_assignment_table_row(
        course_id="mat-251-050-spring-2026",
        title="HW 6.2",
        assignment_kind="Homework",
        row_text="01/26/26 6:00pm Homework HW 6.2 100%",
        due_text="01/26/26 6:00pm",
        due_class="rightnone",
        score_text="100%",
        href="javascript:doHomework(717756881, false, true);",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.status == "completed"
    assert assignment.grade_pct == 100.0


def test_parse_pearson_assignment_table_row_marks_tests_as_exam() -> None:
    assignment = parse_pearson_assignment_table_row(
        course_id="mat-251-050-spring-2026",
        title="Test 1/4 (6.2-7.1) Sp 26",
        assignment_kind="Test",
        row_text="02/11/26 6:00pm Test Test 1/4 (6.2-7.1) Sp 26 59.66%",
        due_text="02/11/26 6:00pm",
        due_class="rightnone",
        score_text="59.66%",
        href="javascript:doHomework(717756900, false, true);",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.type == "exam"
    assert assignment.grade_pct == 59.66


def test_parse_pearson_assignment_table_row_keeps_future_zero_percent_actionable() -> None:
    assignment = parse_pearson_assignment_table_row(
        course_id="mat-251-050-spring-2026",
        title="HW 8.8",
        assignment_kind="Homework",
        row_text="12/31/2099 6:00pm Homework HW 8.8 0%",
        due_text="12/31/2099 6:00pm",
        due_class="rightnone",
        score_text="0%",
        href="javascript:doHomework(717756895, false, true);",
        timezone="America/Chicago",
    )

    assert assignment is not None
    assert assignment.status == "upcoming"
    assert assignment.grade_pct == 0.0


def test_is_pearson_url_accepts_pearsoned_launch_hosts() -> None:
    assert is_pearson_url("https://socket.pearsoned.com/uiservice/optstatus/#/optstatus") is True
    assert is_pearson_url("https://session.physics-mastering.pearson.com/myct/item/123") is True
    assert is_pearson_url("https://tpi.bb.pearsoncmg.com/highlander/api/o/lti/tools/dda") is True
    assert is_pearson_url("https://d2l.oakton.edu/d2l/le/content/188500/viewContent/5114776/View") is False
