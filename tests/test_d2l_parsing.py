from acc.scrapers.d2l import (
    build_due_text,
    content_module_has_schedule_hints,
    detect_external_platform,
    extract_announcement_posted_at_text,
    extract_course_id,
    parse_content_type,
    parse_course_link_text,
    trim_document_preamble,
)


def test_extract_course_id_from_home_url() -> None:
    assert extract_course_id("/d2l/home/189252") == "189252"
    assert extract_course_id("/d2l/lms/dropbox/dropbox.d2l?ou=189252") is None


def test_parse_course_link_text() -> None:
    course = parse_course_link_text(
        "CSC-242-0C1 - Python Data Structures, 10570.202610, Spring 2026, Ends May 15, 2026 at 11:59 PM",
        course_id="189252",
        home_url="https://d2l.oakton.edu/d2l/home/189252",
    )

    assert course.course_id == "189252"
    assert course.code == "CSC-242-0C1"
    assert course.name == "Python Data Structures"
    assert course.offering_code == "10570.202610"
    assert course.semester == "Spring 2026"
    assert course.end_date_text == "Ends May 15, 2026 at 11:59 PM"


def test_build_due_text() -> None:
    due_text = build_due_text(["MAR", "22", "11:59 PM", "Programming Exercise 6.7 - Due"])
    assert due_text == "MAR 22 11:59 PM"


def test_extract_announcement_posted_at_text() -> None:
    text = "What to do the 10th week? Hector Hernandez posted on Mar 23, 2026 12:01 AM Edited"
    assert extract_announcement_posted_at_text(text) == "Mar 23, 2026 12:01 AM"


def test_parse_content_type_from_d2l_topic_title() -> None:
    assert parse_content_type("'PHY 221-001 Syllabus' - Word Document") == "Word Document"
    assert parse_content_type("'Mastering Assignments' - External Learning Tool") == (
        "External Learning Tool"
    )


def test_detect_external_platform() -> None:
    assert detect_external_platform("Cengage MindTap") == "cengage_mindtap"
    assert detect_external_platform("Mastering Assignments") == "pearson_mylab"


def test_content_module_has_schedule_hints() -> None:
    assert content_module_has_schedule_hints("Week 6")
    assert content_module_has_schedule_hints("GRADES TASKS - CH05")
    assert content_module_has_schedule_hints("Graded Tasks - Chapter 5")
    assert content_module_has_schedule_hints("Extra Credit Opportunities")
    assert not content_module_has_schedule_hints("Start Here")
    assert not content_module_has_schedule_hints("Instructor Information")


def test_trim_document_preamble() -> None:
    noisy = (
        "4 Presentation Mode Tools Zoom Out Zoom In Automatic Zoom Actual Size "
        "OAKTON COLLEGE CSC 242 PYTHON DATA STRUCTURES COURSE SYLLABUS"
    )
    assert trim_document_preamble(noisy) == (
        "OAKTON COLLEGE CSC 242 PYTHON DATA STRUCTURES COURSE SYLLABUS"
    )
