from datetime import UTC, datetime

from acc.db.repository import (
    assignment_match_key,
    assignment_to_model,
    course_to_model,
    display_title,
    external_assignment_to_model,
)
from acc.engine.normalizer import NormalizedAssignment, NormalizedCourse
from acc.scrapers.snapshots import ExternalAssignmentSnapshot


def test_course_to_model_maps_core_fields() -> None:
    course = NormalizedCourse(
        id="csc-242-0c1-spring-2026",
        d2l_course_id="189252",
        code="CSC-242-0C1",
        name="Python Data Structures",
        semester="Spring 2026",
        d2l_url="https://d2l.oakton.edu/d2l/home/189252",
        current_grade_pct=46.92,
        current_grade_text="46.92 %",
        syllabus_raw_text="CSC 242 Python Data Structures Course Syllabus",
        external_platform="cengage_mindtap",
        external_platform_url="https://gateway.cengage.com/rest/launchBasicLTI/2152/example",
        external_platform_urls={"assignments": "https://d2l.oakton.edu/d2l/lms/dropbox/dropbox.d2l?ou=189252"},
    )

    model = course_to_model(course, datetime(2026, 3, 22, 22, 46, tzinfo=UTC))

    assert model.id == "csc-242-0c1-spring-2026"
    assert model.d2l_course_id == "189252"
    assert model.name == "Python Data Structures"
    assert model.current_grade_pct == 46.92
    assert model.syllabus_raw_text == "CSC 242 Python Data Structures Course Syllabus"
    assert model.external_platform == "cengage_mindtap"
    assert model.external_platform_url == "https://gateway.cengage.com/rest/launchBasicLTI/2152/example"
    assert model.last_scraped_d2l == datetime(2026, 3, 22, 22, 46, tzinfo=UTC)


def test_assignment_to_model_maps_due_date_and_raw_source() -> None:
    assignment = NormalizedAssignment(
        id="csc-242-0c1-spring-2026-7590085",
        course_id="csc-242-0c1-spring-2026",
        title="Programming Exercise 6.7 - Due",
        type="homework",
        status="upcoming",
        external_url="https://d2l.oakton.edu/d2l/le/calendar/189252/event/7590085/detailsview#7590085",
        grade_category="Programming Exercises",
        points_earned=100.0,
        points_possible=100.0,
        weight_achieved=5.0,
        weight_possible=5.0,
        grade_pct=100.0,
        due_at=datetime(2026, 3, 22, 23, 59, tzinfo=UTC),
        due_text="MAR 22 11:59 PM",
        raw_source={"details_url": "https://d2l.oakton.edu/d2l/le/calendar/189252/event/7590085/detailsview#7590085"},
    )

    model = assignment_to_model(assignment, datetime(2026, 3, 22, 22, 46, tzinfo=UTC))

    assert model.id == "csc-242-0c1-spring-2026-7590085"
    assert model.course_id == "csc-242-0c1-spring-2026"
    assert model.due_date == datetime(2026, 3, 22, 23, 59, tzinfo=UTC)
    assert model.grade_category == "Programming Exercises"
    assert model.points_earned == 100.0
    assert model.points_possible == 100.0
    assert model.grade_pct == 100.0
    assert model.raw_scraped_data == assignment.raw_source


def test_external_assignment_to_model_maps_status_and_estimate() -> None:
    assignment = ExternalAssignmentSnapshot(
        id="pearson-phy-221-001-spring-2026-hw-6",
        course_id="phy-221-001-spring-2026",
        source_platform="pearson_mylab",
        title="HW 6",
        type="homework",
        status="completed",
        external_url="https://session.physics-mastering.pearson.com/myct/item/123",
        description="6 items, about 25-30 minutes",
        due_at=datetime(2026, 3, 26, 3, 59, tzinfo=UTC),
        due_text="03/25/2026 10:59 PM",
        estimated_minutes=28,
        raw_source={"row_text": "HW 6 03/25/2026 10:59 PM 6 of 6 complete"},
    )

    model = external_assignment_to_model(assignment, datetime(2026, 3, 22, 22, 46, tzinfo=UTC))

    assert model.id == "pearson-phy-221-001-spring-2026-hw-6"
    assert model.course_id == "phy-221-001-spring-2026"
    assert model.source_platform == "pearson_mylab"
    assert model.is_submitted is True
    assert model.submitted_at == datetime(2026, 3, 22, 22, 46, tzinfo=UTC)
    assert model.estimated_minutes == 28


def test_display_title_and_match_key_normalize_d2l_and_external_variants() -> None:
    d2l_title = "Programming Exercise 6.7: Unit 6 Code: Unit 6: Inheritance and..."
    external_title = "Programming Exercise 6.7Not startedCOUNTS TOWARDS GRADE"

    assert display_title(d2l_title) == "Programming Exercise 6.7"
    assert display_title(external_title) == "Programming Exercise 6.7"
    assert assignment_match_key(d2l_title) == assignment_match_key(external_title)
