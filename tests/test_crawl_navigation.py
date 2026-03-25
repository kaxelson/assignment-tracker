from acc.scrapers.crawl_navigation import (
    cengage_url_same_course,
    d2l_calendar_url,
    d2l_external_tool_wrapper_should_be_crawled,
    d2l_href_allowed_for_course,
    d2l_syllabus_topic_should_be_crawled,
    d2l_tool_nav_should_be_crawled,
    is_pearson_mylab_course_tool_frame_url,
    nav_target_should_be_crawled,
    normalize_crawl_url,
    pearson_href_in_course_scope,
)


def test_nav_target_should_be_crawled_accepts_grades_rejects_etext() -> None:
    assert nav_target_should_be_crawled("Grades", "https://ng.cengage.com/x?tab=grades") is True
    assert nav_target_should_be_crawled("Assignments", "/path") is True
    assert nav_target_should_be_crawled("eText", "https://ng.cengage.com/read") is False
    assert nav_target_should_be_crawled("Videos", "https://ng.cengage.com/watch") is False
    assert nav_target_should_be_crawled("Course Home", "https://ng.cengage.com/home") is False


def test_nav_target_should_not_match_network_for_coursework_token() -> None:
    assert nav_target_should_be_crawled("Network settings", "https://x.com") is False


def test_normalize_crawl_url_strips_fragment() -> None:
    assert normalize_crawl_url("https://HOST/P?q=1#frag") == "https://host/P?q=1"


def test_d2l_href_allowed_for_course_requires_matching_scope() -> None:
    host = "d2l.example.edu"
    cid = "189252"
    ok = f"https://{host}/d2l/lms/grades/index.d2l?ou={cid}"
    assert d2l_href_allowed_for_course(ok, cid, host) is True
    assert d2l_href_allowed_for_course(ok.replace(cid, "999"), cid, host) is False
    assert d2l_href_allowed_for_course(f"https://other.edu/d2l/lms/grades/index.d2l?ou={cid}", cid, host) is False


def test_cengage_url_same_course_requires_matching_snapshot() -> None:
    base = (
        "https://ng.cengage.com/static/nb/ui/evo/index.html?"
        "snapshotId=1&deploymentId=d&eISBN=978"
    )
    ok = (
        "https://ng.cengage.com/static/nb/ui/evo/index.html?"
        "snapshotId=1&deploymentId=d&eISBN=978&tab=grades"
    )
    wrong = (
        "https://ng.cengage.com/static/nb/ui/evo/index.html?"
        "snapshotId=2&deploymentId=d&eISBN=978"
    )
    assert cengage_url_same_course(ok, base) is True
    assert cengage_url_same_course(wrong, base) is False
    assert cengage_url_same_course("https://other.com/x", base) is False


def test_pearson_href_in_course_scope() -> None:
    assert pearson_href_in_course_scope("https://mylabmastering.pearson.com/course") is True
    assert pearson_href_in_course_scope("https://help.pearson.com/x") is False
    assert pearson_href_in_course_scope("https://www.pearson.com/") is False


def test_is_pearson_mylab_course_tool_frame_url() -> None:
    assert (
        is_pearson_mylab_course_tool_frame_url(
            "https://mylab.pearson.com/courses/12345/assignments?foo=1"
        )
        is True
    )
    assert is_pearson_mylab_course_tool_frame_url("https://mylab.pearson.com/other.js") is False


def test_d2l_calendar_url() -> None:
    assert d2l_calendar_url("https://d2l.example.edu/d2l/home/99", "99") == (
        "https://d2l.example.edu/d2l/le/calendar/99"
    )


def test_d2l_tool_nav_filters_classlist_and_keeps_content() -> None:
    assert d2l_tool_nav_should_be_crawled("Classlist", "https://d2l.example.edu/d2l/lms/classlist/classlist.d2l?ou=1") is False
    assert d2l_tool_nav_should_be_crawled("Content", "https://d2l.example.edu/d2l/le/content/1/Home") is True
    assert d2l_tool_nav_should_be_crawled("Grades", "https://d2l.example.edu/d2l/lms/grades/index.d2l?ou=1") is True


def test_d2l_syllabus_topic_skips_panopto() -> None:
    assert d2l_syllabus_topic_should_be_crawled("Syllabus", "https://d2l.example.edu/x", None) is True
    assert d2l_syllabus_topic_should_be_crawled("Panopto recording", "https://d2l.example.edu/x", None) is False


def test_d2l_external_wrapper_not_included_for_plain_content_view() -> None:
    u = "https://d2l.example.edu/d2l/le/content/1/viewContent/2/View"
    assert d2l_external_tool_wrapper_should_be_crawled("Random link", u, None) is False
    assert d2l_external_tool_wrapper_should_be_crawled("MindTap homework", u, "Week 4") is True
