from datetime import UTC, datetime

from acc.db.repository import (
    CanonicalAssignment,
    assignment_match_key,
    assignment_to_model,
    compute_effective_course_grade,
    course_to_model,
    display_title,
    explain_effective_course_grade,
    external_assignment_to_model,
    infer_assignment_category_key,
    reconcile_assignments,
)
from acc.db.models import Assignment, Course
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


def test_reconcile_assignments_prefers_external_grade_without_double_counting() -> None:
    course = Course(
        id="csc-242-0c1-spring-2026",
        code="CSC-242-0C1",
        name="Python Data Structures",
        instructor=None,
        d2l_course_id="189252",
        d2l_url="https://d2l.oakton.edu/d2l/home/189252",
        semester="Spring 2026",
        external_platform="cengage_mindtap",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[{"name": "Programming Exercises", "weight": 0.4}],
        late_policy_global=None,
        current_grade_pct=75.0,
        current_letter_grade=None,
        last_scraped_d2l=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
        last_scraped_external=datetime(2026, 3, 22, 23, 0, tzinfo=UTC),
        last_syllabus_parse=None,
    )
    duplicate_d2l = Assignment(
        id="d2l-6-7",
        course_id=course.id,
        title="Programming Exercise 6.7: Unit 6 Code: Unit 6: Inheritance and...",
        description=None,
        type="homework",
        source_platform="d2l",
        external_url="https://d2l.oakton.edu/event/7590085",
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category="Programming Exercises",
        grade_weight_pct=20.0,
        points_possible=100.0,
        points_earned=50.0,
        grade_pct=50.0,
        status="graded",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
        course=course,
    )
    duplicate_external = Assignment(
        id="cengage-6-7",
        course_id=course.id,
        title="Programming Exercise 6.7SubmittedCOUNTS TOWARDS GRADE",
        description=None,
        type="homework",
        source_platform="cengage_mindtap",
        external_url="https://ng.cengage.com/activity/2733512518",
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=100.0,
        points_earned=100.0,
        grade_pct=100.0,
        status="completed",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=25,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 22, 23, 0, tzinfo=UTC),
        course=course,
    )
    other_d2l = Assignment(
        id="d2l-6-5",
        course_id=course.id,
        title="Programming Exercise 6.5",
        description=None,
        type="homework",
        source_platform="d2l",
        external_url=None,
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category="Programming Exercises",
        grade_weight_pct=20.0,
        points_possible=100.0,
        points_earned=100.0,
        grade_pct=100.0,
        status="graded",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
        course=course,
    )

    canonical_assignments = reconcile_assignments([duplicate_d2l, duplicate_external, other_d2l])

    assert len(canonical_assignments) == 2
    merged_duplicate = next(
        assignment for assignment in canonical_assignments if assignment.title == "Programming Exercise 6.7"
    )
    assert merged_duplicate.grade_pct == 100.0
    assert merged_duplicate.grade_weight_pct == 20.0
    assert compute_effective_course_grade(canonical_assignments, course=course) == 100.0


def test_reconcile_assignments_prefers_d2l_due_date_when_both_systems_have_one() -> None:
    course = Course(
        id="csc-242-0c1-spring-2026",
        code="CSC-242-0C1",
        name="Python Data Structures",
        instructor=None,
        d2l_course_id="189252",
        d2l_url="https://d2l.oakton.edu/d2l/home/189252",
        semester="Spring 2026",
        external_platform="cengage_mindtap",
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
    d2l_assignment = Assignment(
        id="d2l-7-2",
        course_id=course.id,
        title="Programming Exercise 7.2: Unit 7 Code: Unit 7: Stacks",
        description=None,
        type="homework",
        source_platform="d2l",
        external_url="https://d2l.oakton.edu/event/7594102",
        available_date=None,
        due_date=datetime(2026, 3, 30, 4, 59, tzinfo=UTC),
        close_date=None,
        grade_category="Programming Exercises",
        grade_weight_pct=5.0,
        points_possible=100.0,
        points_earned=0.0,
        grade_pct=0.0,
        status="upcoming",
        is_submitted=False,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data={"event_due_text": "MAR 29 11:59 PM"},
        last_scraped=datetime(2026, 3, 23, 16, 34, tzinfo=UTC),
        course=course,
    )
    external_assignment = Assignment(
        id="cengage-7-2",
        course_id=course.id,
        title="Programming Exercise 7.2Not startedCOUNTS TOWARDS GRADE",
        description=None,
        type="homework",
        source_platform="cengage_mindtap",
        external_url="https://ng.cengage.com/activity/2733512624",
        available_date=None,
        due_date=datetime(2026, 3, 31, 4, 59, tzinfo=UTC),
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
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data={"due_text": "MAR 30 11:59 PM"},
        last_scraped=datetime(2026, 3, 23, 16, 35, tzinfo=UTC),
        course=course,
    )

    canonical_assignments = reconcile_assignments([d2l_assignment, external_assignment])

    assert len(canonical_assignments) == 1
    assert canonical_assignments[0].due_date == datetime(2026, 3, 30, 4, 59, tzinfo=UTC)


def test_reconcile_assignments_uses_highest_grade_for_repeated_test_attempts() -> None:
    course = Course(
        id="mat-251-050-spring-2026",
        code="MAT-251-050",
        name="Calculus II",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform="pearson_mylab",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[{"name": "Tests", "weight": 0.65}],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    first_attempt = Assignment(
        id="test-1-attempt-1",
        course_id=course.id,
        title="Test 1/4 (6.2-7.1) Sp 26",
        description=None,
        type="exam",
        source_platform="pearson_mylab",
        external_url=None,
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=None,
        points_earned=None,
        grade_pct=59.66,
        status="completed",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 22, 22, 46, tzinfo=UTC),
        course=course,
    )
    second_attempt = Assignment(
        id="test-1-attempt-2",
        course_id=course.id,
        title="Test 1/4 (6.2-7.1) Sp 26",
        description=None,
        type="exam",
        source_platform="pearson_mylab",
        external_url=None,
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=None,
        points_earned=None,
        grade_pct=82.0,
        status="completed",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 22, 23, 46, tzinfo=UTC),
        course=course,
    )

    canonical_assignments = reconcile_assignments([first_attempt, second_attempt])

    assert len(canonical_assignments) == 1
    assert canonical_assignments[0].grade_pct == 82.0


def test_compute_effective_course_grade_merges_weighted_d2l_and_external_category_scores() -> None:
    course = Course(
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
        grade_categories=[{"name": "Homework", "weight": 0.2}, {"name": "Exams", "weight": 0.8}],
        late_policy_global=None,
        current_grade_pct=70.0,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    assignments = [
        CanonicalAssignment(
            id="exam-1",
            course_id=course.id,
            title="Exam 1",
            description=None,
            type="exam",
            source_platform="d2l",
            external_url=None,
            due_date=None,
            grade_category="Exams",
            grade_weight_pct=80.0,
            grade_pct=70.0,
            points_possible=100.0,
            points_earned=70.0,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="hw-1",
            course_id=course.id,
            title="HW 1",
            description=None,
            type="homework",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=100.0,
            points_possible=10.0,
            points_earned=10.0,
            status="completed",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="hw-2",
            course_id=course.id,
            title="HW 2",
            description=None,
            type="homework",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=80.0,
            points_possible=10.0,
            points_earned=8.0,
            status="completed",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course) == 74.0


def test_infer_assignment_category_key_matches_exam_rows_to_tests_category() -> None:
    course = Course(
        id="mat-251-050-spring-2026",
        code="MAT-251-050",
        name="Calculus II",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform="pearson_mylab",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[
            {"name": "Attendance", "weight": 0.1},
            {"name": "Homework", "weight": 0.25},
            {"name": "Tests", "weight": 0.65},
        ],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    assignment = CanonicalAssignment(
        id="test-1",
        course_id=course.id,
        title="Test 1/4 (6.2-7.1) Sp 26",
        description=None,
        type="exam",
        source_platform="pearson_mylab",
        external_url=None,
        due_date=None,
        grade_category=None,
        grade_weight_pct=None,
        grade_pct=59.66,
        points_possible=None,
        points_earned=None,
        status="completed",
        late_policy=None,
        estimated_minutes=None,
        raw_scraped_data=None,
        course=course,
    )

    assert infer_assignment_category_key(assignment, course) == "tests"


def test_compute_effective_course_grade_excludes_practice_tests() -> None:
    course = Course(
        id="mat-251-050-spring-2026",
        code="MAT-251-050",
        name="Calculus II",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform="pearson_mylab",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[{"name": "Tests", "weight": 0.65}],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    assignments = [
        CanonicalAssignment(
            id="practice-test-1",
            course_id=course.id,
            title="Practice Test 1/4 (6.2-7.1) Sp26",
            description=None,
            type="exam",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=100.0,
            points_possible=None,
            points_earned=None,
            status="completed",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="test-1",
            course_id=course.id,
            title="Test 1/4 (6.2-7.1) Sp 26",
            description=None,
            type="exam",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=72.0,
            points_possible=None,
            points_earned=None,
            status="completed",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course) == 72.0


def test_compute_effective_course_grade_ignores_future_zero_percent_pearson_rows() -> None:
    course = Course(
        id="mat-251-050-spring-2026",
        code="MAT-251-050",
        name="Calculus II",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform="pearson_mylab",
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[{"name": "Homework", "weight": 0.25}],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    assignments = [
        CanonicalAssignment(
            id="hw-8-8",
            course_id=course.id,
            title="HW 8.8",
            description=None,
            type="homework",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=0.0,
            points_possible=None,
            points_earned=None,
            status="upcoming",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="hw-8-9",
            course_id=course.id,
            title="HW 8.9",
            description=None,
            type="homework",
            source_platform="pearson_mylab",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=None,
            grade_pct=100.0,
            points_possible=None,
            points_earned=None,
            status="completed",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course) == 100.0


def test_compute_effective_course_grade_uses_orphan_d2l_grade_rows_when_no_summary_grade_exists() -> None:
    course = Course(
        id="phl-130-001-spring-2026",
        code="PHL-130-001",
        name="Religious Diversity in America",
        instructor=None,
        d2l_course_id="188900",
        d2l_url="https://d2l.oakton.edu/d2l/home/188900",
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
    assignments = [
        CanonicalAssignment(
            id="participation",
            course_id=course.id,
            title="Participation",
            description=None,
            type="discussion",
            status="graded",
            source_platform="d2l",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=10.0,
            grade_pct=80.0,
            points_possible=None,
            points_earned=None,
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="exam-1",
            course_id=course.id,
            title="Exam 1",
            description=None,
            type="exam",
            status="graded",
            source_platform="d2l",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=20.0,
            grade_pct=81.5,
            points_possible=None,
            points_earned=None,
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="essay",
            course_id=course.id,
            title="Thinking About Religion Essay",
            description=None,
            type="writing",
            status="graded",
            source_platform="d2l",
            external_url=None,
            due_date=None,
            grade_category=None,
            grade_weight_pct=10.0,
            grade_pct=85.0,
            points_possible=None,
            points_earned=None,
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course) == 82.0


def test_compute_effective_course_grade_includes_past_due_zero_when_weighted() -> None:
    course = Course(
        id="demo-101-spring-2026",
        code="DEMO-101",
        name="Demo",
        instructor=None,
        d2l_course_id=None,
        d2l_url=None,
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
    due_past = datetime(2026, 3, 1, 23, 59, tzinfo=UTC)
    ref = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    assignments = [
        CanonicalAssignment(
            id="late-hw",
            course_id=course.id,
            title="Late HW",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due_past,
            grade_category=None,
            grade_weight_pct=40.0,
            grade_pct=0.0,
            points_possible=None,
            points_earned=None,
            status="upcoming",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="good-hw",
            course_id=course.id,
            title="Good HW",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due_past,
            grade_category=None,
            grade_weight_pct=60.0,
            grade_pct=100.0,
            points_possible=None,
            points_earned=None,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course, now=ref) == 60.0


def test_compute_effective_course_grade_excludes_future_due_zero_when_weighted() -> None:
    course = Course(
        id="demo-102-spring-2026",
        code="DEMO-102",
        name="Demo",
        instructor=None,
        d2l_course_id=None,
        d2l_url=None,
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
    due_future = datetime(2026, 4, 1, 23, 59, tzinfo=UTC)
    ref = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
    assignments = [
        CanonicalAssignment(
            id="future-hw",
            course_id=course.id,
            title="Future HW",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due_future,
            grade_category=None,
            grade_weight_pct=40.0,
            grade_pct=0.0,
            points_possible=None,
            points_earned=None,
            status="upcoming",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="done-hw",
            course_id=course.id,
            title="Done HW",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due_future,
            grade_category=None,
            grade_weight_pct=60.0,
            grade_pct=100.0,
            points_possible=None,
            points_earned=None,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    assert compute_effective_course_grade(assignments, course=course, now=ref) == 100.0


def test_syllabus_category_blend_ignores_equal_lms_row_weights() -> None:
    """D2L row weights are often not course-level %; syllabus categories should dominate."""
    course = Course(
        id="mat-123-spring-2026",
        code="MAT-123",
        name="College Algebra",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform=None,
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[
            {"name": "Homework", "weight": 0.2},
            {"name": "Exams", "weight": 0.8},
        ],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    due = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    ref = datetime(2026, 3, 20, 12, 0, tzinfo=UTC)
    assignments = [
        CanonicalAssignment(
            id="exam-1",
            course_id=course.id,
            title="Midterm",
            description=None,
            type="exam",
            source_platform="d2l",
            external_url=None,
            due_date=due,
            grade_category="Exams",
            grade_weight_pct=50.0,
            grade_pct=0.0,
            points_possible=100.0,
            points_earned=0.0,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="hw-1",
            course_id=course.id,
            title="HW 1",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due,
            grade_category="Homework",
            grade_weight_pct=50.0,
            grade_pct=100.0,
            points_possible=100.0,
            points_earned=100.0,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]

    # Legacy row blend would be (50*0 + 50*100) / 100 = 50.
    assert compute_effective_course_grade(assignments, course=course, now=ref) == 20.0


def test_syllabus_grade_detail_includes_assignment_members_per_category() -> None:
    course = Course(
        id="mat-123-spring-2026",
        code="MAT-123",
        name="College Algebra",
        instructor=None,
        d2l_course_id="188500",
        d2l_url="https://d2l.oakton.edu/d2l/home/188500",
        semester="Spring 2026",
        external_platform=None,
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed=None,
        grading_scale=None,
        grade_categories=[
            {"name": "Homework", "weight": 0.2},
            {"name": "Exams", "weight": 0.8},
        ],
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    due = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    ref = datetime(2026, 3, 20, 12, 0, tzinfo=UTC)
    assignments = [
        CanonicalAssignment(
            id="exam-1",
            course_id=course.id,
            title="Midterm",
            description=None,
            type="exam",
            source_platform="d2l",
            external_url=None,
            due_date=due,
            grade_category="Exams",
            grade_weight_pct=50.0,
            grade_pct=0.0,
            points_possible=100.0,
            points_earned=0.0,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
        CanonicalAssignment(
            id="hw-1",
            course_id=course.id,
            title="HW 1",
            description=None,
            type="homework",
            source_platform="d2l",
            external_url=None,
            due_date=due,
            grade_category="Homework",
            grade_weight_pct=50.0,
            grade_pct=100.0,
            points_possible=100.0,
            points_earned=100.0,
            status="graded",
            late_policy=None,
            estimated_minutes=None,
            raw_scraped_data=None,
            course=course,
        ),
    ]
    detail = explain_effective_course_grade(assignments, course=course, now=ref)
    components = detail.get("components")
    assert isinstance(components, list)
    cats = [c for c in components if c.get("type") == "category"]
    assert len(cats) == 2
    by_name = {str(c["name"]): c for c in cats}
    assert "Midterm" in [m["title"] for m in by_name["Exams"]["members"]]
    assert "HW 1" in [m["title"] for m in by_name["Homework"]["members"]]


def test_reconcile_assignments_uses_d2l_row_as_canonical_source_and_records_provenance() -> None:
    course = Course(
        id="c1",
        code="C1",
        name="Course",
        instructor=None,
        d2l_course_id="1",
        d2l_url="https://d2l.example.edu/d2l/home/1",
        semester="Spring 2026",
        external_platform="cengage_mindtap",
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
    d2l_row = Assignment(
        id="d2l-a1",
        course_id=course.id,
        title="Homework 1",
        description=None,
        type="homework",
        source_platform="d2l",
        external_url="https://d2l.example.edu/a1",
        available_date=None,
        due_date=datetime(2026, 3, 25, 4, 59, tzinfo=UTC),
        close_date=None,
        grade_category="Homework",
        grade_weight_pct=10.0,
        points_possible=10.0,
        points_earned=8.0,
        grade_pct=80.0,
        status="graded",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data={"d2l": True},
        last_scraped=datetime(2026, 3, 25, 5, 0, tzinfo=UTC),
        course=course,
    )
    external_row = Assignment(
        id="ext-a1",
        course_id=course.id,
        title="Homework 1SubmittedCOUNTS TOWARDS GRADE",
        description=None,
        type="homework",
        source_platform="cengage_mindtap",
        external_url="https://ext.example/a1",
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=10.0,
        points_earned=10.0,
        grade_pct=100.0,
        status="completed",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=20,
        is_multi_day=False,
        raw_scraped_data={"ext": True},
        last_scraped=datetime(2026, 3, 25, 5, 1, tzinfo=UTC),
        course=course,
    )
    merged = reconcile_assignments([d2l_row, external_row])[0]
    assert merged.id == "d2l-a1"
    assert merged.grade_pct == 100.0
    assert isinstance(merged.raw_scraped_data, dict)
    assert "_reconcile" in merged.raw_scraped_data


def test_reconcile_assignments_filters_rollup_rows_but_keeps_participation() -> None:
    course = Course(
        id="c2",
        code="C2",
        name="Course",
        instructor=None,
        d2l_course_id="2",
        d2l_url="https://d2l.example.edu/d2l/home/2",
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
    rollup = Assignment(
        id="rollup",
        course_id=course.id,
        title="Final Calculated Grade",
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
        grade_pct=88.0,
        status="graded",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 25, 5, 0, tzinfo=UTC),
        course=course,
    )
    participation = Assignment(
        id="part",
        course_id=course.id,
        title="Participation",
        description=None,
        type="discussion",
        source_platform="d2l",
        external_url=None,
        available_date=None,
        due_date=None,
        close_date=None,
        grade_category=None,
        grade_weight_pct=None,
        points_possible=None,
        points_earned=None,
        grade_pct=92.0,
        status="graded",
        is_submitted=True,
        submitted_at=None,
        is_late=False,
        days_late=0,
        late_policy=None,
        estimated_minutes=None,
        is_multi_day=False,
        raw_scraped_data=None,
        last_scraped=datetime(2026, 3, 25, 5, 0, tzinfo=UTC),
        course=course,
    )
    canonical = reconcile_assignments([rollup, participation])
    assert len(canonical) == 1
    assert canonical[0].title == "Participation"


def test_effective_grade_applies_syllabus_late_penalty_per_day() -> None:
    course = Course(
        id="c3",
        code="C3",
        name="Course",
        instructor=None,
        d2l_course_id="3",
        d2l_url="https://d2l.example.edu/d2l/home/3",
        semester="Spring 2026",
        external_platform=None,
        external_platform_url=None,
        textbook=None,
        syllabus_raw_text=None,
        syllabus_parsed={"late_policy": {"default_penalty_per_day": 0.1, "max_late_days": 5}},
        grading_scale=None,
        grade_categories=None,
        late_policy_global=None,
        current_grade_pct=None,
        current_letter_grade=None,
        last_scraped_d2l=None,
        last_scraped_external=None,
        last_syllabus_parse=None,
    )
    assignment = CanonicalAssignment(
        id="a1",
        course_id=course.id,
        title="Essay 1",
        description=None,
        type="writing",
        source_platform="d2l",
        external_url=None,
        due_date=datetime(2026, 3, 1, 23, 59, tzinfo=UTC),
        grade_category=None,
        grade_weight_pct=100.0,
        grade_pct=90.0,
        points_possible=100.0,
        points_earned=90.0,
        status="graded",
        late_policy=None,
        estimated_minutes=None,
        raw_scraped_data={"days_late": 2},
        course=course,
    )
    assert compute_effective_course_grade([assignment], course=course) == 70.0
