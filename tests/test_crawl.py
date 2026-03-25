from acc.scrapers.crawl import build_artifact_id, build_cengage_detail_url, build_d2l_crawl_targets
from acc.scrapers.snapshots import D2LAnnouncement, D2LCourseSnapshot, D2LToolLink


def test_build_d2l_crawl_targets_includes_course_surfaces_without_duplicates() -> None:
    course = D2LCourseSnapshot(
        course_id="189252",
        code="CSC-242-0C1",
        name="Python Data Structures",
        home_url="https://d2l.oakton.edu/d2l/home/189252",
        tool_links=[
            D2LToolLink(name="Grades", url="https://d2l.oakton.edu/d2l/lms/grades/index.d2l?ou=189252"),
            D2LToolLink(name="Grades", url="https://d2l.oakton.edu/d2l/lms/grades/index.d2l?ou=189252"),
            D2LToolLink(name="Classlist", url="https://d2l.oakton.edu/d2l/lms/classlist/classlist.d2l?ou=189252"),
        ],
        announcements=[
            D2LAnnouncement(
                title="What to do the 10th week?",
                url="https://d2l.oakton.edu/d2l/le/news/189252/627940/view",
            )
        ],
    )

    targets = build_d2l_crawl_targets(course)

    urls = [target.url for target in targets]
    assert "https://d2l.oakton.edu/d2l/home/189252" in urls
    assert "https://d2l.oakton.edu/d2l/lms/news/main.d2l?ou=189252" in urls
    assert "https://d2l.oakton.edu/d2l/le/calendar/189252" in urls
    assert "https://d2l.oakton.edu/d2l/lms/grades/index.d2l?ou=189252" in urls
    assert "https://d2l.oakton.edu/d2l/le/content/189252/Home" in urls
    assert "https://d2l.oakton.edu/d2l/lms/dropbox/dropbox.d2l?ou=189252" in urls
    assert "https://d2l.oakton.edu/d2l/lms/quizzing/user/quizzes_list.d2l?ou=189252" in urls
    assert "https://d2l.oakton.edu/d2l/le/news/189252/627940/view" in urls
    assert "https://d2l.oakton.edu/d2l/lms/classlist/classlist.d2l?ou=189252" not in urls
    assert len(urls) == len(set(urls))


def test_build_d2l_crawl_targets_resolves_standard_tool_urls_from_fake_links() -> None:
    course = D2LCourseSnapshot(
        course_id="189252",
        code="CSC-242-0C1",
        name="Python Data Structures",
        home_url="https://d2l.oakton.edu/d2l/home/189252",
        tool_links=[
            D2LToolLink(name="Course Content", url="https://d2l.oakton.edu/d2l/le/content/189252/Home"),
            D2LToolLink(name="Dropbox", url="https://d2l.oakton.edu/d2l/lms/dropbox/dropbox.d2l?ou=189252"),
            D2LToolLink(name="Quizzes", url="https://d2l.oakton.edu/d2l/lms/quizzing/user/quizzes_list.d2l?ou=189252"),
            D2LToolLink(name="My Grades", url="https://d2l.oakton.edu/d2l/lms/grades/my_grades/main.d2l?ou=189252"),
            D2LToolLink(name="News", url="https://d2l.oakton.edu/d2l/lms/news/main.d2l?ou=189252"),
            D2LToolLink(name="Calendar", url="https://d2l.oakton.edu/d2l/le/calendar/189252"),
        ],
    )

    targets = build_d2l_crawl_targets(course)
    by_kind = {target.page_kind: target.url for target in targets}
    assert by_kind["tool_content"].endswith("/d2l/le/content/189252/Home")
    assert by_kind["tool_assignments"].endswith("/d2l/lms/dropbox/dropbox.d2l?ou=189252")
    assert by_kind["tool_quizzes-exams"].endswith("/d2l/lms/quizzing/user/quizzes_list.d2l?ou=189252")
    assert by_kind["tool_grades"].endswith("/d2l/lms/grades/my_grades/main.d2l?ou=189252")
    assert by_kind["announcements_index"].endswith("/d2l/lms/news/main.d2l?ou=189252")
    assert by_kind["tool_calendar"].endswith("/d2l/le/calendar/189252")


def test_build_cengage_detail_url_uses_course_context() -> None:
    course_url = (
        "https://ng.cengage.com/static/nb/ui/evo/index.html?"
        "snapshotId=5097675&id=2733512483&deploymentId=60338321522369707401831720&eISBN=9780357505458"
    )

    detail_url = build_cengage_detail_url(course_url, "2733512624")

    assert detail_url == (
        "https://ng.cengage.com/static/nb/ui/evo/index.html?"
        "deploymentId=60338321522369707401831720&eISBN=9780357505458&id=2733512624&snapshotId=5097675&"
    )


def test_build_artifact_id_is_stable() -> None:
    assert build_artifact_id("csc-242-0c1-spring-2026", "course_home", "Python Data Structures") == (
        build_artifact_id("csc-242-0c1-spring-2026", "course_home", "Python Data Structures")
    )
