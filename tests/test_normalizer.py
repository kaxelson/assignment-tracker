from datetime import UTC, datetime

from acc.engine.normalizer import (
    assignment_match_key,
    compose_syllabus_raw_text,
    extract_assignments_from_content_outline,
    normalize_d2l_snapshot,
    parse_due_text,
    parse_flexible_due_at,
    parse_fraction,
    parse_percent,
)
from acc.scrapers.snapshots import (
    D2LAnnouncement,
    D2LAnnouncementItem,
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


def test_parse_flexible_due_at_accepts_written_month_dates() -> None:
    ref = datetime(2026, 3, 20, tzinfo=UTC)
    parsed = parse_flexible_due_at("Mar 1, 2026 11:59 PM", ref, timezone="America/Chicago")
    assert parsed == datetime(2026, 3, 2, 5, 59, tzinfo=UTC)


def test_extract_assignments_from_content_outline() -> None:
    text = (
        "GRADES TASKS - CH05\n"
        "Programming Exercise 5.7 Due Mar 1, 2026 11:59 PM\n"
        "Extra Credit: Bonus Quiz Mar 8, 2026 11:59 PM\n"
    )
    rows = extract_assignments_from_content_outline(text)
    assert rows == [
        ("Programming Exercise 5.7", "Mar 1, 2026 11:59 PM"),
        ("Extra Credit: Bonus Quiz", "Mar 8, 2026 11:59 PM"),
    ]


def test_normalize_d2l_merges_content_outline_due_with_grade_row() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 20, 12, 0, tzinfo=UTC),
        source_url="https://d2l.oakton.edu",
        courses=[
            D2LCourseSnapshot(
                course_id="189252",
                code="CSC-242-0C1",
                name="Python Data Structures",
                home_url="https://d2l.oakton.edu/d2l/home/189252",
                grade_rows=[
                    D2LGradeRow(
                        title="Programming Exercise 5.7",
                        is_category=False,
                        category_title="Programming Exercises",
                        points_text="0 / 100",
                        weight_achieved_text="0 / 5",
                        grade_text="0 %",
                    ),
                ],
                content_outline_topics=[
                    D2LContentTopic(
                        title="Week 6 (module view)",
                        url="https://d2l.example/le/content/189252#outline-module=Week%206",
                        module_title="Week 6",
                        extracted_text="Programming Exercise 5.7 Due Mar 1, 2026 11:59 PM\n",
                    ),
                ],
            )
        ],
    )
    normalized = normalize_d2l_snapshot(snapshot, timezone="America/Chicago")
    assert len(normalized.assignments) == 1
    assignment = normalized.assignments[0]
    assert assignment.title == "Programming Exercise 5.7"
    assert assignment.due_at == datetime(2026, 3, 2, 5, 59, tzinfo=UTC)
    assert "content_outline_module" in assignment.raw_source


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
    assert assignment_match_key(
        "Programming Exercise 6.7Not startedCOUNTS TOWARDS GRADE"
    ) == "programming-exercise-6-7"


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
        "[PHY 221-001 Syllabus]\n"
        "OAKTON COLLEGE GENERAL PHYSICS 221-001 COURSE SYLLABUS Spring Semester 2026"
    )
    assert normalized.courses[0].external_platform == "pearson_mylab"
    assert normalized.courses[0].external_platform_url == "https://mlm.pearson.com/"
    assert normalized.courses[0].external_platform_urls["mastering-assignments"] == (
        "https://mlm.pearson.com/"
    )


def test_compose_syllabus_raw_text_merges_all_syllabus_topics() -> None:
    course = D2LCourseSnapshot(
        course_id="188307",
        code="PHY-221-001",
        name="General Physics I",
        semester="Spring 2026",
        home_url="https://d2l.oakton.edu/d2l/home/188307",
        syllabus_topics=[
            D2LContentTopic(
                title="Welcome",
                url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/1/View",
                module_title="SYLLABUS",
                content_type="HTML",
                extracted_text="Welcome to the course.",
            ),
            D2LContentTopic(
                title="Full syllabus",
                url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/2/View",
                module_title="SYLLABUS",
                content_type="PDF",
                extracted_text="Exams 60%, Homework 40%.",
            ),
        ],
    )
    text = compose_syllabus_raw_text(course)
    assert text is not None
    assert "Welcome to the course." in text
    assert "Exams 60%" in text
    assert "\n\n---\n\n" in text


def test_compose_syllabus_raw_text_adds_grading_like_content_outline_topics() -> None:
    course = D2LCourseSnapshot(
        course_id="188307",
        code="MAT-251-050",
        name="Calculus II",
        semester="Spring 2026",
        home_url="https://d2l.oakton.edu/d2l/home/188307",
        syllabus_topics=[],
        content_outline_topics=[
            D2LContentTopic(
                title="Grade breakdown",
                url="https://d2l.oakton.edu/d2l/le/content/188307/viewContent/99/View",
                module_title="Week 1",
                content_type="HTML",
                extracted_text="Tests 75%, WebAssign 25%.",
            ),
        ],
    )
    text = compose_syllabus_raw_text(course)
    assert text is not None
    assert "Tests 75%" in text


def test_normalize_d2l_snapshot_preserves_orphan_grade_rows_as_assignments() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 23, 2, 0, tzinfo=UTC),
        source_url="https://d2l.oakton.edu/d2l/home",
        courses=[
            D2LCourseSnapshot(
                course_id="188900",
                code="PHL-130-001",
                name="Religious Diversity in America",
                semester="Spring 2026",
                home_url="https://d2l.oakton.edu/d2l/home/188900",
                grade_rows=[
                    D2LGradeRow(
                        title="Participation",
                        is_category=True,
                        weight_achieved_text="8 / 10",
                        grade_text="80 %",
                    ),
                    D2LGradeRow(
                        title="Exam 1",
                        is_category=True,
                        weight_achieved_text="16.3 / 20",
                        grade_text="81.5 %",
                    ),
                    D2LGradeRow(
                        title="Written Work",
                        is_category=True,
                        weight_achieved_text="8.5 / 10",
                        grade_text="85 %",
                    ),
                ],
            )
        ],
    )

    normalized = normalize_d2l_snapshot(snapshot)

    assert [assignment.title for assignment in normalized.assignments] == [
        "Participation",
        "Exam 1",
        "Written Work",
    ]
    assert normalized.assignments[0].weight_possible == 10.0
    assert normalized.assignments[1].grade_pct == 81.5


def test_normalize_d2l_snapshot_merges_announcement_metadata_for_matching_assignments() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 23, 16, 34, tzinfo=UTC),
        source_url="https://d2l.oakton.edu/d2l/home",
        courses=[
            D2LCourseSnapshot(
                course_id="189252",
                code="CSC-242-0C1",
                name="Python Data Structures",
                semester="Spring 2026",
                home_url="https://d2l.oakton.edu/d2l/home/189252",
                upcoming_events=[
                    D2LUpcomingEvent(
                        title="Programming Exercise 7.2: Unit 7 Code: Unit 7: Stacks - Due",
                        due_text="MAR 29 11:59 PM",
                        details_url="https://d2l.oakton.edu/d2l/le/calendar/189252/event/7594102/detailsview#7594102",
                    )
                ],
                grade_rows=[
                    D2LGradeRow(
                        title="Programming Exercise 7.2: Unit 7 Code: Unit 7: Stacks",
                        is_category=False,
                        category_title="Programming Exercises",
                        points_text="0 / 100",
                        weight_achieved_text="0 / 5",
                        grade_text="0 %",
                    )
                ],
                announcements=[
                    D2LAnnouncement(
                        title="What to do the 10th week?",
                        url="https://d2l.oakton.edu/d2l/le/news/189252/627940/view",
                        posted_at_text="Mar 23, 2026 12:01 AM",
                        items=[
                            D2LAnnouncementItem(
                                title="Programming Exercise 7.2: Unit 7 Code: Unit 7: Stacks",
                                url=(
                                    "https://d2l.oakton.edu/d2l/common/dialogs/quickLink/quickLink.d2l"
                                    "?ou=189252&type=content&rcode=oakton-3088088"
                                ),
                            )
                        ],
                    )
                ],
            )
        ],
    )

    normalized = normalize_d2l_snapshot(snapshot)

    assert len(normalized.assignments) == 1
    assert normalized.assignments[0].raw_source["announcement_title"] == "What to do the 10th week?"
    assert normalized.assignments[0].raw_source["announcement_posted_at_text"] == (
        "Mar 23, 2026 12:01 AM"
    )


def test_normalize_d2l_snapshot_creates_assignment_from_announcement_only_item() -> None:
    snapshot = D2LDashboardSnapshot(
        fetched_at=datetime(2026, 3, 23, 16, 34, tzinfo=UTC),
        source_url="https://d2l.oakton.edu/d2l/home",
        courses=[
            D2LCourseSnapshot(
                course_id="189252",
                code="CSC-242-0C1",
                name="Python Data Structures",
                semester="Spring 2026",
                home_url="https://d2l.oakton.edu/d2l/home/189252",
                announcements=[
                    D2LAnnouncement(
                        title="What to do the 10th week?",
                        url="https://d2l.oakton.edu/d2l/le/news/189252/627940/view",
                        posted_at_text="Mar 23, 2026 12:01 AM",
                        items=[
                            D2LAnnouncementItem(
                                title="Programming Exercise 7.1: Unit 7 Code: Unit 7: Stacks",
                                url=(
                                    "https://d2l.oakton.edu/d2l/common/dialogs/quickLink/quickLink.d2l"
                                    "?ou=189252&type=content&rcode=oakton-3088086"
                                ),
                            )
                        ],
                    )
                ],
            )
        ],
    )

    normalized = normalize_d2l_snapshot(snapshot)

    assert len(normalized.assignments) == 1
    assert normalized.assignments[0].title == "Programming Exercise 7.1: Unit 7 Code: Unit 7: Stacks"
    assert normalized.assignments[0].raw_source["announcement_title"] == "What to do the 10th week?"
    assert normalized.assignments[0].status == "available"
